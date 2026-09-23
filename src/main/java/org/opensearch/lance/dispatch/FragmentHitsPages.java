/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.apache.lucene.util.BytesRef;
import org.lance.Dataset;
import org.lance.ipc.ColumnOrdering;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.engine.LanceFragmentLeafReader;
import org.opensearch.lance.query.ScanAdmission;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceKnnQuery;
import org.opensearch.search.SearchHit;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;

/**
 * The hits phase of the fragment executor in two steps. The page is
 * collected first, as Lucene {@link ScoreDoc}s over the fragment leaf
 * readers: {@link #viaIndexSearcher} through Lucene's top docs
 * collectors (with the request's {@code min_score} and
 * {@code terminate_after} composed around them, see
 * {@link CollectorKnobs}), {@link #viaCollapsingCollector} through the
 * collapsing collector of a {@code collapse} request, or
 * {@link #viaLanceSortedScan} from one
 * ordered, limited Lance scan when the coordinator's plan pushed the
 * page into the scan. {@link #rescore} re scores a collected first pass
 * with the request's rescorers. {@link #materialise} then renders the hits of
 * either page through {@link FragmentFetchPhase}, so the hit envelope
 * ({@code _id}, {@code _source} under the request's source filter,
 * {@code stored_fields}, {@code docvalue_fields}, {@code fields},
 * {@code _explanation}, score and sort values) does not depend on which
 * step collected the page, and reports every hit's Lance row address for
 * the coordinator's tie break.
 */
final class FragmentHitsPages {

    private FragmentHitsPages() {}

    /**
     * A collected page before the fetch phase: the top level doc ids
     * with their scores and, for a sorted page, their sort values as
     * {@link FieldDoc}s, in response order. {@code collected} is the
     * match count the collection itself established, or {@code null}
     * when the caller counts through {@code PlanExecutor.computeMatched}
     * (no {@code min_score} or {@code terminate_after} on the request).
     * {@code terminatedEarly} is {@code null} without
     * {@code terminate_after}, else whether the bound stopped the
     * collection.
     */
    record CollectedPage(ScoreDoc[] scoreDocs, TotalHits collected, Boolean terminatedEarly) {
        static final CollectedPage EMPTY = new CollectedPage(new ScoreDoc[0], null, null);
    }

    /**
     * The hits of one page together with the Lance row address
     * ({@code fragmentId << 32 | offset}) of each, parallel arrays. The
     * addresses travel to the coordinator, which breaks ties between
     * hits with equal sort values on them.
     */
    record HitsPage(List<SearchHit> hits, long[] rowAddrs) {
        static final HitsPage EMPTY = new HitsPage(Collections.emptyList(), new long[0]);
    }

    /**
     * Row address of a doc of {@code reader}: the fragment id of the
     * Lance leaf the doc belongs to in the high 32 bits, the offset of
     * the doc inside that leaf in the low 32 bits. Every leaf of a
     * fragment dispatch reader is Lance-backed; any other leaf is a
     * bug in the reader construction, not something to paper over.
     */
    private static long rowAddressOf(IndexReader reader, int doc) {
        List<LeafReaderContext> leaves = reader.leaves();
        LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(doc, leaves));
        LanceFragmentLeafReader lance = LanceFragmentLeafReader.unwrap(leaf.reader());
        if (lance == null) {
            throw new IllegalStateException("leaf " + leaf.ord + " of the fragment reader is not backed by a Lance fragment");
        }
        return ((long) lance.fragmentId() << 32) | (lance.rowOf(doc - leaf.docBase) & 0xFFFFFFFFL);
    }

    /**
     * Total-hits threshold {@link IndexSearcher} hands its own top-docs
     * collector managers ({@code IndexSearcher.TOTAL_HITS_THRESHOLD},
     * which is private there). Without collector knobs only the
     * {@code TopDocs.totalHits} accounting depends on it and the
     * executor reads {@code hits.total} from
     * {@code PlanExecutor.computeMatched}, so the value keeps the page
     * identical to the one {@code IndexSearcher.search} would return.
     */
    private static final int TOTAL_HITS_THRESHOLD = 1000;

    /**
     * Collect the top-{@code size} page of {@code query} on the shared
     * {@link ContextIndexSearcher} through the collector managers
     * {@link IndexSearcher#search(Query, int)},
     * {@link IndexSearcher#search(Query, int, Sort, boolean)} and their
     * {@code searchAfter} overloads build internally: a
     * {@link TopScoreDocCollectorManager} for score order, a
     * {@link TopFieldCollectorManager} for a sort, each with
     * {@code numHits} capped and the stock total hits threshold. The
     * reader is built by the caller so hits and aggregations share one
     * Lucene scan of the fragment subset.
     *
     * <p>The score is the real Lucene score (BM25 for Lance FTS, cosine
     * for Lance knn, 1.0 for {@link MatchAllDocsQuery}). A sorted page
     * captures per-hit sort values for the coordinator's merge and,
     * when {@code trackScores} ({@code track_scores}) is set, scores
     * populated after the collection the way the 4 argument
     * {@code search} overload does; without it a sorted page carries
     * {@link Float#NaN} scores.
     *
     * <p>{@code after} is the {@code search_after} cursor as
     * {@code SearchAfterBuilder.buildFieldDoc} typed it against the
     * request's sort, or null. Its {@code doc} is pinned to the reader's
     * last doc: Lucene reads the field as the tie break for docs sharing
     * the cursor's sort values, and any value at or past the end of the
     * reader excludes the tied docs, which is the shard path's
     * {@code Integer.MAX_VALUE} semantics without tripping the
     * {@code doc >= maxDoc} pre-flight of {@code IndexSearcher.searchAfter}.
     *
     * <p>When {@code sharedWeight} is non-null the collectors run
     * through {@link LanceFragmentIndexSearcher#search(Weight, CollectorManager)}
     * with that Weight instead of letting {@link IndexSearcher} create
     * one from {@code query}, so a bare {@link LanceFtsQuery} or
     * {@link LanceKnnQuery}'s Lance scan is shared with the aggregators
     * and, for FTS, the match count.
     *
     * <p>{@code knobs} composes {@code min_score} and
     * {@code terminate_after} around the top docs collector; with either
     * present the collection also establishes the match count
     * ({@link CollectedPage#collected}): the top docs collector's total
     * under the request's {@code track_total_hits} threshold for a page,
     * a {@link TotalHitCountCollector} for {@code size: 0}, both of which
     * see only the documents the knobs let through. Without knobs a
     * {@code size: 0} request collects nothing here.
     */
    static CollectedPage viaIndexSearcher(
        LanceFragmentIndexSearcher searcher,
        Query query,
        Weight sharedWeight,
        SortAndFormats sortAndFormats,
        FieldDoc after,
        int size,
        boolean trackScores,
        CollectorKnobs knobs,
        int trackTotalHitsUpTo
    ) throws IOException {
        if (size <= 0 && !knobs.any()) {
            return CollectedPage.EMPTY;
        }
        if (size <= 0) {
            CollectorKnobs.Wrapped<TotalHitCountCollector, Long> manager = knobs.wrap(new HitCountCollectorManager());
            Boolean terminatedEarly = knobs.terminatesEarly() ? Boolean.FALSE : null;
            long count;
            try {
                count = sharedWeight == null ? searcher.search(query, manager) : searcher.search(sharedWeight, manager);
            } catch (CollectorKnobs.Terminated terminated) {
                terminatedEarly = Boolean.TRUE;
                count = manager.reduceCreated();
            }
            return new CollectedPage(new ScoreDoc[0], new TotalHits(count, TotalHits.Relation.EQUAL_TO), terminatedEarly);
        }
        int threshold = knobs.any() ? totalHitsThreshold(trackTotalHitsUpTo) : TOTAL_HITS_THRESHOLD;
        int numHits = cappedNumHits(searcher, size);
        FieldDoc cursor = after == null ? null : pinnedCursor(searcher, after);
        Sort sort = sortAndFormats == null ? null : sortAndFormats.sort.rewrite(searcher);
        CollectorManager<?, ? extends TopDocs> topDocsManager;
        if (sort == null) {
            topDocsManager = new TopScoreDocCollectorManager(numHits, cursor, threshold);
        } else {
            topDocsManager = new TopFieldCollectorManager(sort, numHits, cursor, threshold);
        }
        CollectorKnobs.Wrapped<?, ? extends TopDocs> manager = knobs.wrap(topDocsManager);
        Boolean terminatedEarly = knobs.terminatesEarly() ? Boolean.FALSE : null;
        TopDocs topDocs;
        try {
            topDocs = sharedWeight == null ? searcher.search(query, manager) : searcher.search(sharedWeight, manager);
        } catch (CollectorKnobs.Terminated terminated) {
            terminatedEarly = Boolean.TRUE;
            topDocs = manager.reduceCreated();
        }
        if (sort != null && trackScores) {
            if (sharedWeight == null) {
                TopFieldCollector.populateScores(topDocs.scoreDocs, searcher, query);
            } else {
                populateScores(topDocs.scoreDocs, searcher, sharedWeight);
            }
        }
        return new CollectedPage(topDocs.scoreDocs, knobs.any() ? topDocs.totalHits : null, terminatedEarly);
    }

    /**
     * Collect the top-{@code size} collapsed page of {@code query}: one
     * hit per distinct value of the collapse field, the best hit of each
     * group under the request's sort (score order without one), through
     * {@link CollapseContext#createTopDocs} the way the shard path's
     * {@code CollapsingTopDocsCollectorContext} does. One collapsing
     * collector runs per slice and the slices' {@link CollapseTopFieldDocs}
     * are merged with {@link CollapseTopFieldDocs#merge}, which is the
     * reduce the shard path applies under concurrent segment search.
     * {@code after} is the {@code search_after} cursor (the sort is then
     * the collapse field alone, checked by the caller), pinned as in
     * {@link #viaIndexSearcher}. The knobs compose around the collector
     * as for a plain page; with either present the match count is the
     * collapsing collector's document count, which is what the shard
     * path reports for the same body. The page's {@link FieldDoc}s carry
     * the group's sort values (the score, under score order); the
     * collapse value of every hit reaches the coordinator as the doc
     * value field the fetch phase adds for the collapse field.
     */
    static CollectedPage viaCollapsingCollector(
        LanceFragmentIndexSearcher searcher,
        Query query,
        Weight sharedWeight,
        CollapseContext collapse,
        SortAndFormats sortAndFormats,
        FieldDoc after,
        int size,
        CollectorKnobs knobs,
        int trackTotalHitsUpTo
    ) throws IOException {
        if (size <= 0) {
            // A count only request collapses nothing; the plain path
            // counts it (or collects nothing without knobs).
            return viaIndexSearcher(searcher, query, sharedWeight, sortAndFormats, after, size, false, knobs, trackTotalHitsUpTo);
        }
        int numHits = cappedNumHits(searcher, size);
        FieldDoc cursor = after == null ? null : pinnedCursor(searcher, after);
        Sort sort = sortAndFormats == null ? Sort.RELEVANCE : sortAndFormats.sort.rewrite(searcher);
        CollectorKnobs.Wrapped<CollapsingTopDocsCollector<?>, CollapseTopFieldDocs> manager = knobs.wrap(
            new CollapsingCollectorManager(collapse, sort, numHits, cursor)
        );
        Boolean terminatedEarly = knobs.terminatesEarly() ? Boolean.FALSE : null;
        CollapseTopFieldDocs topDocs;
        try {
            topDocs = sharedWeight == null ? searcher.search(query, manager) : searcher.search(sharedWeight, manager);
        } catch (CollectorKnobs.Terminated terminated) {
            terminatedEarly = Boolean.TRUE;
            topDocs = manager.reduceCreated();
        }
        return new CollectedPage(topDocs.scoreDocs, knobs.any() ? topDocs.totalHits : null, terminatedEarly);
    }

    /**
     * One {@link CollapsingTopDocsCollector} per slice over the same
     * collapse field, sort, group count and cursor; the reduce merges the
     * slices' top groups so one hit per value survives, as the shard
     * path's collapsing collector context reduces its slices.
     */
    private static final class CollapsingCollectorManager implements CollectorManager<CollapsingTopDocsCollector<?>, CollapseTopFieldDocs> {
        private final CollapseContext collapse;
        private final Sort sort;
        private final int numHits;
        private final FieldDoc after;

        CollapsingCollectorManager(CollapseContext collapse, Sort sort, int numHits, FieldDoc after) {
            this.collapse = collapse;
            this.sort = sort;
            this.numHits = numHits;
            this.after = after;
        }

        @Override
        public CollapsingTopDocsCollector<?> newCollector() {
            return after == null ? collapse.createTopDocs(sort, numHits) : collapse.createTopDocs(sort, numHits, after);
        }

        @Override
        public CollapseTopFieldDocs reduce(Collection<CollapsingTopDocsCollector<?>> collectors) throws IOException {
            List<CollapseTopFieldDocs> perSlice = new ArrayList<>(collectors.size());
            for (CollapsingTopDocsCollector<?> collector : collectors) {
                perSlice.add(collector.getTopDocs());
            }
            if (perSlice.size() == 1) {
                return perSlice.get(0);
            }
            return CollapseTopFieldDocs.merge(sort, 0, numHits, perSlice.toArray(new CollapseTopFieldDocs[0]));
        }
    }

    /**
     * Run the request's rescorers over a collected first pass, in body
     * order, the way {@code RescoreProcessor} does on the shard path:
     * each {@link RescoreContext}'s rescorer re scores the top
     * {@code window_size} docs of the page with its query over the
     * shared searcher, combines the two scores under the rescorer's
     * weights and score mode, and re sorts the page by the new scores.
     * The page is then cut to {@code size} (the executor's
     * {@code from + size}), since the first pass collected the larger
     * window. A rescore query that is a Lance full text or knn query
     * runs its one shard level Lance scan when the rescorer asks the
     * Weight for its first leaf, as on the shard path; the match count
     * of the page stays the first pass count.
     */
    static CollectedPage rescore(CollectedPage page, List<RescoreContext> rescorers, LanceFragmentIndexSearcher searcher, int size)
        throws IOException {
        if (page.scoreDocs().length == 0 || rescorers.isEmpty()) {
            return page;
        }
        TotalHits total = page.collected() == null ? new TotalHits(page.scoreDocs().length, TotalHits.Relation.EQUAL_TO) : page.collected();
        TopDocs topDocs = new TopDocs(total, page.scoreDocs());
        for (RescoreContext rescoreContext : rescorers) {
            topDocs = rescoreContext.rescorer().rescore(topDocs, searcher, rescoreContext);
        }
        ScoreDoc[] scoreDocs = topDocs.scoreDocs.length > size ? Arrays.copyOf(topDocs.scoreDocs, size) : topDocs.scoreDocs;
        return new CollectedPage(scoreDocs, page.collected(), page.terminatedEarly());
    }

    /**
     * The total hits threshold of a page whose collection also counts
     * the matches: the request's {@code track_total_hits} bound, every
     * match for {@code track_total_hits: true}, and a single hit when
     * tracking is off (the coordinator leaves {@code hits.total} out
     * then).
     */
    private static int totalHitsThreshold(int trackTotalHitsUpTo) {
        if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            return Integer.MAX_VALUE;
        }
        if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
            return 1;
        }
        return trackTotalHitsUpTo;
    }

    /** One {@link TotalHitCountCollector} per slice, the counts summed. */
    private static final class HitCountCollectorManager implements CollectorManager<TotalHitCountCollector, Long> {
        @Override
        public TotalHitCountCollector newCollector() {
            return new TotalHitCountCollector();
        }

        @Override
        public Long reduce(Collection<TotalHitCountCollector> collectors) {
            long total = 0L;
            for (TotalHitCountCollector collector : collectors) {
                total += collector.getTotalHits();
            }
            return total;
        }
    }

    /**
     * {@code numHits} cap {@link IndexSearcher#searchAfter}
     * applies before building a collector: a top-docs collector
     * rejects {@code numHits > maxDoc} and {@code numHits < 1}, so the
     * result is clamped to {@code [1, max(1, maxDoc)]} whatever
     * {@code size} is.
     */
    private static int cappedNumHits(LanceFragmentIndexSearcher searcher, int size) {
        return Math.max(1, Math.min(size, Math.max(1, searcher.getIndexReader().maxDoc())));
    }

    /**
     * The cursor with its tie break doc pinned to the reader's last doc
     * (or 0 on an empty reader); see {@link #viaIndexSearcher}.
     */
    private static FieldDoc pinnedCursor(LanceFragmentIndexSearcher searcher, FieldDoc after) {
        int maxDoc = searcher.getIndexReader().maxDoc();
        int afterDoc = maxDoc > 0 ? maxDoc - 1 : 0;
        return new FieldDoc(afterDoc, after.score, after.fields);
    }

    /**
     * Fill {@link ScoreDoc#score} of a sorted page from {@code weight},
     * the way {@link TopFieldCollector#populateScores(ScoreDoc[], IndexSearcher, Query)}
     * does, except that the caller's Weight is used instead of a new
     * one created from the Query (which for a Lance-backed query
     * would run the native scan again). Docs are visited in doc id
     * order so each leaf's scorer is obtained once.
     */
    private static void populateScores(ScoreDoc[] scoreDocs, LanceFragmentIndexSearcher searcher, Weight weight) throws IOException {
        ScoreDoc[] byDoc = scoreDocs.clone();
        Arrays.sort(byDoc, Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        LeafReaderContext current = null;
        Scorer scorer = null;
        for (ScoreDoc scoreDoc : byDoc) {
            if (current == null || scoreDoc.doc >= current.docBase + current.reader().maxDoc()) {
                current = leaves.get(ReaderUtil.subIndex(scoreDoc.doc, leaves));
                ScorerSupplier supplier = weight.scorerSupplier(current);
                if (supplier == null) {
                    throw new IllegalStateException("Doc id " + scoreDoc.doc + " does not match the query");
                }
                scorer = supplier.get(1L);
            }
            int leafDoc = scoreDoc.doc - current.docBase;
            if (scorer.iterator().advance(leafDoc) != leafDoc) {
                throw new IllegalStateException("Doc id " + scoreDoc.doc + " does not match the query");
            }
            scoreDoc.score = scorer.score();
        }
    }

    /**
     * Render the hits of a collected page: one Lance take per leaf for
     * the rows behind the page ({@link #prefetchHitRows}), the fetch
     * phase over the page's doc ids, then the score and, for a sorted
     * page, the sort values of every hit from its {@link ScoreDoc} (a
     * {@code _score} clause's value becoming the hit's score), as
     * {@code SearchPhaseController} stamps them on the shard path. The
     * row address of every hit rides along for the coordinator.
     */
    static HitsPage materialise(
        LanceFragmentSearchContext searchContext,
        FragmentFetchPhase fetchPhase,
        IndexReader reader,
        ScoreDoc[] scoreDocs,
        SortAndFormats sortAndFormats
    ) throws IOException {
        if (scoreDocs.length == 0) {
            return HitsPage.EMPTY;
        }
        prefetchHitRows(reader, scoreDocs);
        int[] docIds = new int[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            docIds[i] = scoreDocs[i].doc;
        }
        SearchHit[] hits = fetchPhase.fetch(searchContext, docIds);
        // A sort with a _score clause carries the score as that clause's
        // sort value; SearchPhaseController copies it into _score on the
        // shard path, whether or not track_scores is set.
        int sortScoreIndex = -1;
        if (sortAndFormats != null) {
            SortField[] sortFields = sortAndFormats.sort.getSort();
            for (int i = 0; i < sortFields.length; i++) {
                if (sortFields[i].getType() == SortField.Type.SCORE) {
                    sortScoreIndex = i;
                }
            }
        }
        List<SearchHit> out = new ArrayList<>(hits.length);
        long[] rowAddrs = new long[hits.length];
        for (int i = 0; i < hits.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];
            SearchHit hit = hits[i];
            hit.score(scoreDoc.score);
            if (sortAndFormats != null && scoreDoc instanceof FieldDoc fieldDoc) {
                hit.sortValues(fieldDoc.fields, sortAndFormats.formats);
                if (sortScoreIndex != -1 && fieldDoc.fields[sortScoreIndex] instanceof Number score) {
                    hit.score(score.floatValue());
                }
            }
            out.add(hit);
            rowAddrs[i] = rowAddressOf(reader, scoreDoc.doc);
        }
        return new HitsPage(out, rowAddrs);
    }

    /**
     * Group the page's doc ids by leaf and hand each Lance-backed leaf
     * its slice so the rows behind the hits are fetched in one Lance
     * take per leaf before the per-doc {@code document(...)} loop
     * runs. Without this, {@link LanceFragmentLeafReader#materialiseStoredFields}
     * would fall back to a single-row take per hit ({@code size} JNI
     * round trips instead of one per leaf touched).
     *
     * <p>Leaves that do not unwrap to a {@link LanceFragmentLeafReader}
     * (which should not happen on this path; every leaf the fragment
     * dispatch reader exposes is Lance-backed) are skipped and fall
     * back to the per-doc path.
     */
    private static void prefetchHitRows(IndexReader reader, ScoreDoc[] scoreDocs) throws IOException {
        if (scoreDocs.length == 0) {
            return;
        }
        List<LeafReaderContext> leaves = reader.leaves();
        Map<Integer, List<Integer>> docsByLeaf = new TreeMap<>();
        for (ScoreDoc scoreDoc : scoreDocs) {
            int leafIndex = ReaderUtil.subIndex(scoreDoc.doc, leaves);
            int localDoc = scoreDoc.doc - leaves.get(leafIndex).docBase;
            docsByLeaf.computeIfAbsent(leafIndex, k -> new ArrayList<>()).add(localDoc);
        }
        for (Map.Entry<Integer, List<Integer>> entry : docsByLeaf.entrySet()) {
            LanceFragmentLeafReader lance = LanceFragmentLeafReader.unwrap(leaves.get(entry.getKey()).reader());
            if (lance == null) {
                continue;
            }
            List<Integer> docs = entry.getValue();
            int[] docIds = new int[docs.size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = docs.get(i);
            }
            lance.prefetchRows(docIds);
        }
    }

    /**
     * Hits phase for sorted scalar-filter pages: one Lance scan with
     * the pushed query's SQL (plus the {@code search_after} cursor
     * bound, see {@link FragmentPlan.TopK#scanFilterSql}),
     * {@code setColumnOrderings}, {@code limit(fetch)} and the sort
     * columns projected. Lance evaluates the filter and the top-k in
     * one pass (pylance measures {@code filter + order_by + limit 10}
     * on a 10M-row table at tens of milliseconds where the Lucene
     * collector path needed the whole sort column loaded), and the
     * batches come back already in the requested order. Each row's
     * {@code _rowaddr} is decoded to (fragment id, doc id), routed to
     * the matching leaf and reported as a top level {@link FieldDoc}
     * so {@link #materialise} renders it through the same fetch phase
     * as a collector page.
     *
     * <p>Sort values are read from the projected columns and typed the
     * way the request's Lucene {@link SortField}s
     * would type them (see {@link #sortValueFrom}) so
     * {@link SearchHit#sortValues} formats them identically and
     * clients can feed them back as {@code search_after}. Arrow nulls
     * become the missing-value object OpenSearch installed on the
     * SortField for the request's {@code missing} / direction
     * combination. The score is 1.0 under {@code track_scores} and
     * {@link Float#NaN} otherwise, as for a sorted collector page over
     * a scalar filter.
     */
    static CollectedPage viaLanceSortedScan(
        Dataset dataset,
        LanceFragmentQueryRequest request,
        List<ColumnOrdering> orderings,
        int fetch,
        String filterSql,
        SortAndFormats sortAndFormats,
        IndexReader reader,
        List<Integer> fragmentIds,
        LanceCancellation cancellation
    ) throws IOException {
        SortField[] sortFields = sortAndFormats.sort.getSort();
        Map<Integer, LeafReaderContext> leafByFragment = new HashMap<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            LanceFragmentLeafReader lance = LanceFragmentLeafReader.unwrap(ctx.reader());
            if (lance != null) {
                leafByFragment.put(lance.fragmentId(), ctx);
            }
        }
        List<String> sortColumns = new ArrayList<>();
        for (ColumnOrdering ordering : orderings) {
            if (!sortColumns.contains(ordering.getColumnName())) {
                sortColumns.add(ordering.getColumnName());
            }
        }
        ScanOptions.Builder builder = new ScanOptions.Builder().fragmentIds(fragmentIds)
            .columns(sortColumns)
            .setColumnOrderings(orderings)
            .limit(fetch)
            .withRowAddress(true);
        if (filterSql != null) {
            builder = builder.filter(filterSql);
        }
        // The sorted page reads the sort columns of every row the
        // filter keeps (Lance's top-k sorts after the scan), so it is
        // gated as a filter scan over the node's fragments whose row
        // width is the row address plus the sort columns; a filter
        // without one is a scan of every row.
        long nodeRows = 0L;
        for (LeafReaderContext ctx : leafByFragment.values()) {
            nodeRows += ctx.reader().maxDoc();
        }
        long rowWidth = ScanAdmission.ROW_ADDRESS_BYTES;
        for (String sortColumn : sortColumns) {
            rowWidth += ScanAdmission.columnWidthBytes(dataset.getSchema().getFields(), sortColumn);
        }
        ScanAdmission.admitExecutorFilterScan(
            request.indexName(),
            dataset,
            filterSql == null ? "" : filterSql,
            nodeRows,
            fetch,
            rowWidth,
            "sorted page scan"
        );
        // Ordered (fragment id, row offset, raw sort values) triples in
        // the order Lance returned them, which is the response order.
        List<long[]> addresses = new ArrayList<>(fetch);
        List<Object[]> sortValues = new ArrayList<>(fetch);
        ScanAdmission.scanStarted();
        try (LanceScanner scanner = dataset.newScan(builder.build()); ArrowReader arrowReader = scanner.scanBatches()) {
            while (arrowReader.loadNextBatch()) {
                cancellation.checkCancelled();
                VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                FieldVector[] vectors = new FieldVector[orderings.size()];
                for (int o = 0; o < vectors.length; o++) {
                    vectors[o] = root.getVector(orderings.get(o).getColumnName());
                }
                for (int i = 0; i < root.getRowCount(); i++) {
                    long addr = rowAddr.get(i);
                    addresses.add(new long[] { addr >>> 32, addr & 0xFFFFFFFFL });
                    Object[] raw = new Object[orderings.size()];
                    for (int o = 0; o < raw.length; o++) {
                        raw[o] = sortValueFrom(vectors[o], i, sortFields[o]);
                    }
                    sortValues.add(raw);
                }
            }
        } catch (IOException | TaskCancelledException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            ScanAdmission.scanFinished();
        }
        // The decoded offsets are physical rows; the leaf's doc ids map
        // through docOfRow (identity unless the table has nested
        // columns), and the top level doc id adds the leaf's docBase.
        float score = request.trackScores() ? 1.0f : Float.NaN;
        List<ScoreDoc> page = new ArrayList<>(addresses.size());
        for (int i = 0; i < addresses.size(); i++) {
            long[] address = addresses.get(i);
            LeafReaderContext ctx = leafByFragment.get((int) address[0]);
            if (ctx == null) {
                // The scan was pinned to fragmentIds, which is the same
                // list the reader was opened with, so every address
                // should map to a leaf. Skipping rather than failing
                // keeps a fragment that vanished between the two opens
                // from taking the whole page down.
                continue;
            }
            LanceFragmentLeafReader lance = LanceFragmentLeafReader.unwrap(ctx.reader());
            page.add(new FieldDoc(ctx.docBase + lance.docOfRow((int) address[1]), score, sortValues.get(i)));
        }
        return new CollectedPage(page.toArray(new ScoreDoc[0]), null, null);
    }

    /**
     * Read one sort value from a projected column the way Lucene's
     * comparator for the matching {@link SortField}
     * would report it, so {@link SearchHit#sortValues} formats it
     * identically to the Lucene hits path and clients can feed it
     * back as {@code search_after}. {@link SortedSetSortField}
     * (keyword) yields a {@link BytesRef};
     * {@link SortedNumericSortField} yields
     * {@code Integer} / {@code Long} / {@code Float} / {@code Double}
     * according to its numeric type (OpenSearch maps byte, short,
     * integer and boolean to INT; long, date and unsigned_long to
     * LONG). Integers, booleans, dates and timestamps go through
     * {@link LanceFragmentLeafReader#readAsLong}, which already
     * normalises date / timestamp units to epoch millis. A null cell
     * yields {@code null} for keyword (what {@code TermOrdValComparator}
     * reports for a missing term) and, for numerics, the exact
     * missing-value object OpenSearch installed on the SortField for
     * the request's {@code missing} / order combination.
     */
    private static Object sortValueFrom(FieldVector vector, int i, SortField sortField) {
        if (sortField instanceof SortedSetSortField) {
            if (vector.isNull(i)) {
                return null;
            }
            return new BytesRef(((VarCharVector) vector).get(i));
        }
        if (vector.isNull(i)) {
            return sortField.getMissingValue();
        }
        SortedNumericSortField numeric = (SortedNumericSortField) sortField;
        return switch (numeric.getNumericType()) {
            case FLOAT -> ((Float4Vector) vector).get(i);
            case DOUBLE -> ((Float8Vector) vector).get(i);
            case INT -> (int) integralValue(vector, i);
            case LONG -> integralValue(vector, i);
            default -> throw new IllegalStateException("unsupported sort field type " + numeric.getNumericType());
        };
    }

    private static long integralValue(FieldVector vector, int i) {
        if (vector instanceof BitVector bits) {
            return bits.get(i);
        }
        return LanceFragmentLeafReader.readAsLong(vector, i);
    }

    /**
     * The knobs of {@code request} as a {@link CollectorKnobs}, for the
     * hits and the aggregation collections alike.
     */
    static CollectorKnobs knobsOf(LanceFragmentQueryRequest request) {
        return new CollectorKnobs(request.minScore(), request.terminateAfter());
    }
}

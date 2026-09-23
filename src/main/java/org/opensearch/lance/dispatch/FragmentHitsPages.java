/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.Weight;
import org.lance.Dataset;
import org.lance.ipc.ColumnOrdering;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.engine.LanceFragmentLeafReader;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceKnnQuery;
import org.opensearch.search.SearchHit;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.sort.SortAndFormats;

/**
 * The two hits phases of the fragment executor and the page they
 * produce: {@link #viaIndexSearcher} collects the page through Lucene's
 * top docs collectors over the fragment leaf readers, and
 * {@link #viaLanceSortedScan} reads it from one ordered, limited Lance
 * scan when the coordinator's plan pushed the page into the scan. Both
 * materialise the hit envelope ({@code _id}, {@code _source}, sort
 * values) through the fragment readers' stored fields path and report
 * every hit's Lance row address for the coordinator's tie break, so the
 * response shape does not depend on which phase answered.
 */
final class FragmentHitsPages {

    private FragmentHitsPages() {}

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
     * Run the top-{@code size} query on the shared
     * {@link ContextIndexSearcher} and materialise every hit through
     * OpenSearch's stock stored-fields path
     * ({@link org.opensearch.lance.engine.LanceFragmentLeafReader#materialiseStoredFields}).
     * The reader is built by the caller so hits and aggregations
     * share one Lucene scan of the fragment subset.
     *
     * <p>The score is the real Lucene score (BM25 for Lance FTS,
     * cosine for Lance knn, 1.0 for {@link MatchAllDocsQuery}).
     * Sort clauses go through the
     * standard {@link org.apache.lucene.search.IndexSearcher#search(Query, int, org.apache.lucene.search.Sort)}
     * call and per-hit sort values are captured for the coordinator's
     * merge phase.
     *
     * <p>{@code trackScores} follows the OpenSearch
     * {@code track_scores} request flag. Sort-based Lucene search
     * defaults to computing sort values only, leaving
     * {@link ScoreDoc#score} at {@link Float#NaN}. When the caller
     * asks for {@code track_scores:true} the 4 / 5 argument
     * {@link org.apache.lucene.search.IndexSearcher#search(Query, int, org.apache.lucene.search.Sort, boolean)}
     * / {@code searchAfter} overloads compute scores alongside the
     * sort, so hits come back with numeric {@code _score} values
     * and the coordinator's {@code max_score} sees real numbers.
     * The score-only path ({@code sortAndFormats == null}) already
     * collects scores through
     * {@link org.apache.lucene.search.IndexSearcher#search(Query, int)}
     * and ignores this flag.
     *
     * <p>When {@code sharedWeight} is non-null the collectors run
     * through {@link LanceFragmentIndexSearcher#search(Weight, org.apache.lucene.search.CollectorManager)}
     * with that Weight instead of letting {@link org.apache.lucene.search.IndexSearcher}
     * create one from {@code query}; the collector managers, the
     * {@code numHits} cap and the total-hits threshold are the ones
     * the stock {@code search} / {@code searchAfter} overloads build
     * internally, so the returned page is the same either way. The
     * caller passes a Weight only for a bare {@link LanceFtsQuery} or
     * {@link LanceKnnQuery} so that its Lance scan is shared with the
     * aggregators and, for FTS, the match count.
     */
    static HitsPage viaIndexSearcher(
        LanceFragmentIndexSearcher searcher,
        Query query,
        Weight sharedWeight,
        SortAndFormats sortAndFormats,
        Object[] searchAfter,
        int size,
        boolean trackScores
    ) throws java.io.IOException {
        if (size <= 0) {
            return HitsPage.EMPTY;
        }
        TopDocs topDocs;
        if (searchAfter != null && sortAndFormats != null) {
            // FieldDoc.doc is Lucene's tie-breaker for docs sharing
            // the sort value with the cursor. Setting it just past
            // the reader's last doc means "exclude the tied doc",
            // which matches OpenSearch's usual search_after
            // semantics. Integer.MAX_VALUE is rejected by Lucene's
            // pre-flight (`>= maxDoc`), so pin the value to
            // `maxDoc - 1` (or 0 when the reader is empty).
            int maxDoc = searcher.getIndexReader().maxDoc();
            int afterDoc = maxDoc > 0 ? maxDoc - 1 : 0;
            FieldDoc after = new FieldDoc(afterDoc, 0f, searchAfter);
            topDocs = sharedWeight == null
                ? searcher.searchAfter(after, query, size, sortAndFormats.sort, trackScores)
                : searchSortedWithWeight(searcher, sharedWeight, after, size, sortAndFormats.sort, trackScores);
        } else if (sortAndFormats == null) {
            topDocs = sharedWeight == null
                ? searcher.search(query, size)
                : searcher.search(sharedWeight, new TopScoreDocCollectorManager(cappedNumHits(searcher, size), null, TOTAL_HITS_THRESHOLD));
        } else {
            topDocs = sharedWeight == null
                ? searcher.search(query, size, sortAndFormats.sort, trackScores)
                : searchSortedWithWeight(searcher, sharedWeight, null, size, sortAndFormats.sort, trackScores);
        }
        prefetchHitRows(searcher.getIndexReader(), topDocs.scoreDocs);
        List<SearchHit> out = new ArrayList<>(topDocs.scoreDocs.length);
        long[] rowAddrs = new long[topDocs.scoreDocs.length];
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            HitVisitor visitor = new HitVisitor();
            searcher.storedFields().document(scoreDoc.doc, visitor);
            SearchHit hit = new SearchHit(i, visitor.idString(), Collections.emptyMap(), Collections.emptyMap());
            hit.score(scoreDoc.score);
            if (visitor.source != null) {
                hit.sourceRef(new org.opensearch.core.common.bytes.BytesArray(visitor.source));
            }
            if (sortAndFormats != null && scoreDoc instanceof FieldDoc fieldDoc) {
                hit.sortValues(fieldDoc.fields, sortAndFormats.formats);
            }
            out.add(hit);
            rowAddrs[i] = rowAddressOf(searcher.getIndexReader(), scoreDoc.doc);
        }
        return new HitsPage(out, rowAddrs);
    }

    /**
     * Total-hits threshold {@link org.apache.lucene.search.IndexSearcher}
     * hands its own top-docs collector managers ({@code
     * IndexSearcher.TOTAL_HITS_THRESHOLD}, which is private there).
     * Only the {@code TopDocs.totalHits} accounting depends on it;
     * this class reads {@code hits.total} from {@link PlanExecutor#computeMatched}
     * and never from the collector, so the value just keeps the
     * Weight-driven page identical to the Query-driven one.
     */
    private static final int TOTAL_HITS_THRESHOLD = 1000;

    /**
     * {@code numHits} cap {@link org.apache.lucene.search.IndexSearcher#searchAfter}
     * applies before building a collector: a top-docs collector
     * rejects {@code numHits > maxDoc} and {@code numHits < 1}, so the
     * result is clamped to {@code [1, max(1, maxDoc)]} whatever
     * {@code size} is.
     */
    private static int cappedNumHits(LanceFragmentIndexSearcher searcher, int size) {
        return Math.max(1, Math.min(size, Math.max(1, searcher.getIndexReader().maxDoc())));
    }

    /**
     * Sorted top-{@code size} page driven by a caller-built
     * {@link Weight}: the same steps as
     * {@link org.apache.lucene.search.IndexSearcher#searchAfter(ScoreDoc, Query, int, org.apache.lucene.search.Sort, boolean)}
     * ({@code Sort.rewrite}, {@link TopFieldCollectorManager} with the
     * stock threshold, score population when {@code trackScores})
     * with the Weight substituted for the Query.
     */
    private static TopFieldDocs searchSortedWithWeight(
        LanceFragmentIndexSearcher searcher,
        Weight weight,
        FieldDoc after,
        int size,
        Sort sort,
        boolean trackScores
    ) throws IOException {
        Sort rewrittenSort = sort.rewrite(searcher);
        TopFieldCollectorManager manager = new TopFieldCollectorManager(
            rewrittenSort,
            cappedNumHits(searcher, size),
            after,
            TOTAL_HITS_THRESHOLD
        );
        TopFieldDocs topDocs = searcher.search(weight, manager);
        if (trackScores) {
            populateScores(topDocs.scoreDocs, searcher, weight);
        }
        return topDocs;
    }

    /**
     * Fill {@link ScoreDoc#score} of a sorted page from {@code weight},
     * the way {@link TopFieldCollector#populateScores(ScoreDoc[], org.apache.lucene.search.IndexSearcher, Query)}
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
    private static void prefetchHitRows(org.apache.lucene.index.IndexReader reader, ScoreDoc[] scoreDocs) throws IOException {
        if (scoreDocs.length == 0) {
            return;
        }
        List<org.apache.lucene.index.LeafReaderContext> leaves = reader.leaves();
        java.util.Map<Integer, List<Integer>> docsByLeaf = new java.util.TreeMap<>();
        for (ScoreDoc scoreDoc : scoreDocs) {
            int leafIndex = org.apache.lucene.index.ReaderUtil.subIndex(scoreDoc.doc, leaves);
            int localDoc = scoreDoc.doc - leaves.get(leafIndex).docBase;
            docsByLeaf.computeIfAbsent(leafIndex, k -> new ArrayList<>()).add(localDoc);
        }
        for (java.util.Map.Entry<Integer, List<Integer>> entry : docsByLeaf.entrySet()) {
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
     * {@code _rowaddr} is decoded to (fragment id, doc id) and routed
     * to the matching leaf, which fetches {@code _id} / {@code _source}
     * through the same {@link LanceFragmentLeafReader#prefetchRows} /
     * {@link LanceFragmentLeafReader#materialiseStoredFields} path the
     * Lucene hits phase uses, so the response shape is identical.
     *
     * <p>Sort values are read from the projected columns and typed the
     * way the request's Lucene {@link org.apache.lucene.search.SortField}s
     * would type them (see {@link #sortValueFrom}) so
     * {@link SearchHit#sortValues} formats them identically and
     * clients can feed them back as {@code search_after}. Arrow nulls
     * become the missing-value object OpenSearch installed on the
     * SortField for the request's {@code missing} / direction
     * combination.
     */
    static HitsPage viaLanceSortedScan(
        Dataset dataset,
        LanceFragmentQueryRequest request,
        List<ColumnOrdering> orderings,
        int fetch,
        String filterSql,
        SortAndFormats sortAndFormats,
        org.apache.lucene.index.IndexReader reader,
        List<Integer> fragmentIds,
        LanceCancellation cancellation
    ) throws IOException {
        org.apache.lucene.search.SortField[] sortFields = sortAndFormats.sort.getSort();
        java.util.Map<Integer, LanceFragmentLeafReader> leafByFragment = new java.util.HashMap<>();
        for (org.apache.lucene.index.LeafReaderContext ctx : reader.leaves()) {
            LanceFragmentLeafReader lance = LanceFragmentLeafReader.unwrap(ctx.reader());
            if (lance != null) {
                leafByFragment.put(lance.fragmentId(), lance);
            }
        }
        List<String> sortColumns = new ArrayList<>();
        for (ColumnOrdering ordering : orderings) {
            if (!sortColumns.contains(ordering.getColumnName())) {
                sortColumns.add(ordering.getColumnName());
            }
        }
        org.lance.ipc.ScanOptions.Builder builder = new org.lance.ipc.ScanOptions.Builder().fragmentIds(fragmentIds)
            .columns(sortColumns)
            .setColumnOrderings(orderings)
            .limit(fetch)
            .withRowAddress(true);
        if (filterSql != null) {
            builder = builder.filter(filterSql);
        }
        // Ordered (fragment id, doc id, raw sort values) triples in the
        // order Lance returned them, which is the response order.
        List<long[]> addresses = new ArrayList<>(fetch);
        List<Object[]> sortValues = new ArrayList<>(fetch);
        try (
            org.lance.ipc.LanceScanner scanner = dataset.newScan(builder.build());
            org.apache.arrow.vector.ipc.ArrowReader arrowReader = scanner.scanBatches()
        ) {
            while (arrowReader.loadNextBatch()) {
                cancellation.checkCancelled();
                org.apache.arrow.vector.VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
                org.apache.arrow.vector.UInt8Vector rowAddr = (org.apache.arrow.vector.UInt8Vector) root.getVector("_rowaddr");
                org.apache.arrow.vector.FieldVector[] vectors = new org.apache.arrow.vector.FieldVector[orderings.size()];
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
        }
        // One take per leaf for the rows behind the page, then render
        // each hit in Lance's order. The decoded offsets are physical
        // rows; the leaf's stored-fields and prefetch paths are keyed by
        // doc id, so each offset maps through docOfRow (identity unless
        // the table has nested columns).
        java.util.Map<Integer, List<Integer>> docsByFragment = new java.util.HashMap<>();
        for (long[] address : addresses) {
            LanceFragmentLeafReader lance = leafByFragment.get((int) address[0]);
            if (lance == null) {
                continue;
            }
            docsByFragment.computeIfAbsent((int) address[0], k -> new ArrayList<>()).add(lance.docOfRow((int) address[1]));
        }
        for (java.util.Map.Entry<Integer, List<Integer>> entry : docsByFragment.entrySet()) {
            LanceFragmentLeafReader lance = leafByFragment.get(entry.getKey());
            if (lance == null) {
                continue;
            }
            int[] docIds = new int[entry.getValue().size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = entry.getValue().get(i);
            }
            lance.prefetchRows(docIds);
        }
        List<SearchHit> out = new ArrayList<>(addresses.size());
        long[] rowAddrs = new long[addresses.size()];
        float score = request.trackScores() ? 1.0f : Float.NaN;
        for (int i = 0; i < addresses.size(); i++) {
            long[] address = addresses.get(i);
            LanceFragmentLeafReader lance = leafByFragment.get((int) address[0]);
            if (lance == null) {
                // The scan was pinned to fragmentIds, which is the same
                // list the reader was opened with, so every address
                // should map to a leaf. Skipping rather than failing
                // keeps a fragment that vanished between the two opens
                // from taking the whole page down.
                continue;
            }
            HitVisitor visitor = new HitVisitor();
            lance.materialiseStoredFields(lance.docOfRow((int) address[1]), visitor);
            SearchHit hit = new SearchHit(out.size(), visitor.idString(), Collections.emptyMap(), Collections.emptyMap());
            hit.score(score);
            if (visitor.source != null) {
                hit.sourceRef(new org.opensearch.core.common.bytes.BytesArray(visitor.source));
            }
            hit.sortValues(sortValues.get(i), sortAndFormats.formats);
            rowAddrs[out.size()] = (address[0] << 32) | address[1];
            out.add(hit);
        }
        return new HitsPage(out, out.size() == rowAddrs.length ? rowAddrs : Arrays.copyOf(rowAddrs, out.size()));
    }

    /**
     * Read one sort value from a projected column the way Lucene's
     * comparator for the matching {@link org.apache.lucene.search.SortField}
     * would report it, so {@link SearchHit#sortValues} formats it
     * identically to the Lucene hits path and clients can feed it
     * back as {@code search_after}. {@link org.apache.lucene.search.SortedSetSortField}
     * (keyword) yields a {@link org.apache.lucene.util.BytesRef};
     * {@link org.apache.lucene.search.SortedNumericSortField} yields
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
    private static Object sortValueFrom(org.apache.arrow.vector.FieldVector vector, int i, org.apache.lucene.search.SortField sortField) {
        if (sortField instanceof org.apache.lucene.search.SortedSetSortField) {
            if (vector.isNull(i)) {
                return null;
            }
            return new org.apache.lucene.util.BytesRef(((org.apache.arrow.vector.VarCharVector) vector).get(i));
        }
        if (vector.isNull(i)) {
            return sortField.getMissingValue();
        }
        org.apache.lucene.search.SortedNumericSortField numeric = (org.apache.lucene.search.SortedNumericSortField) sortField;
        return switch (numeric.getNumericType()) {
            case FLOAT -> ((org.apache.arrow.vector.Float4Vector) vector).get(i);
            case DOUBLE -> ((org.apache.arrow.vector.Float8Vector) vector).get(i);
            case INT -> (int) integralValue(vector, i);
            case LONG -> integralValue(vector, i);
            default -> throw new IllegalStateException("unsupported sort field type " + numeric.getNumericType());
        };
    }

    private static long integralValue(org.apache.arrow.vector.FieldVector vector, int i) {
        if (vector instanceof org.apache.arrow.vector.BitVector bits) {
            return bits.get(i);
        }
        return LanceFragmentLeafReader.readAsLong(vector, i);
    }

    /**
     * StoredFieldVisitor that captures the {@code _id} and
     * {@code _source} bytes {@link LanceFragmentLeafReader#materialiseStoredFields}
     * emits per hit. Reused for every doc in {@link
     * #viaIndexSearcher} to avoid allocating a new visitor
     * per doc; the two capture fields are reset by the visitor
     * itself on each {@code document} call.
     */
    private static final class HitVisitor extends org.apache.lucene.index.StoredFieldVisitor {

        private byte[] source;
        private byte[] idBytes;

        @Override
        public Status needsField(org.apache.lucene.index.FieldInfo fieldInfo) {
            String name = fieldInfo.name;
            if ("_id".equals(name) || "_source".equals(name)) {
                return Status.YES;
            }
            return Status.NO;
        }

        @Override
        public void binaryField(org.apache.lucene.index.FieldInfo fieldInfo, byte[] value) {
            if ("_id".equals(fieldInfo.name)) {
                idBytes = value;
            } else if ("_source".equals(fieldInfo.name)) {
                source = value;
            }
        }

        String idString() {
            if (idBytes == null) {
                return "";
            }
            return org.opensearch.index.mapper.Uid.decodeId(idBytes);
        }
    }
}

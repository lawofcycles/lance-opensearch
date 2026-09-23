/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.util.BytesRef;

import org.apache.lucene.util.PriorityQueue;
import org.opensearch.common.util.Comparators;
import org.opensearch.lance.execute.AggregateSpecResolver.Child;
import org.opensearch.lance.execute.AggregateSpecResolver.KeyKind;
import org.opensearch.lance.execute.AggregateSpecResolver.Level;
import org.opensearch.lance.execute.AggregateSpecResolver.LevelKind;
import org.opensearch.lance.execute.AggregateSpecResolver.Metric;
import org.opensearch.lance.execute.AggregateSpecResolver.Source;
import org.opensearch.lance.execute.GroupAggregationState.Candidate;
import org.opensearch.lance.execute.GroupAggregationState.Group;
import org.opensearch.lance.execute.GroupAggregationState.GroupState;
import org.opensearch.lance.execute.GroupAggregationState.GroupTable;
import org.opensearch.lance.execute.GroupAggregationState.Partial;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeKey;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.InternalDateRange;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.lance.execute.LanceAggregateResults.Result;
import static org.opensearch.lance.execute.AggregateSpecResolver.metadata;
import static org.opensearch.lance.execute.AggregateSpecResolver.thresholds;

/**
 * Turns the merged {@link GroupAggregationState} of a resolved
 * aggregate into the {@link InternalAggregation} objects the Lucene
 * aggregators would have produced on this node, so the coordinator's
 * reduce merges them unchanged: the {@code terms} selection and its
 * {@code sum_other_doc_count} (over the merged top-k, over the key
 * sorted primitive columns, or over boxed candidates for a nested
 * level), {@code histogram} and {@code date_histogram} buckets with
 * their empty bucket info, the mask levels ({@code range},
 * {@code date_range}, {@code filters}, {@code filter},
 * {@code missing}) with every requested bucket reported, the
 * {@code composite} page after the request's {@code after} key, and
 * the metric aggregations of every bucket through
 * {@link AggregateSpecResolver.Metric#toAggregation}. The results
 * whose constructors OpenSearch keeps package private come from
 * {@link CoreAggregationResults}; a top level {@code date_histogram}
 * is created through the aggregator's own empty prototype the caller
 * supplies. Nothing here reads a Lance scan or the request beyond the
 * resolved specs.
 */
final class AggregationResultAssembler {

    private final ResolvedAggregate resolved;

    AggregationResultAssembler(ResolvedAggregate resolved) {
        this.resolved = resolved;
    }

    /** Builds the node's aggregations from the merged partials. */
    Result assemble(Partial merged, int scans, Function<String, InternalAggregation> dateHistogramPrototype) {
        if (resolved.keyCount() == 0) {
            // Lance returns exactly one row for a plan without
            // groupings, even over zero fragments; the fallback only
            // covers a reader that yielded no batch at all, and a
            // cardinality plan, whose distinct grouping returns no
            // row over no rows.
            GroupState state = merged.metricsOnly != null ? merged.metricsOnly : GroupState.empty(resolved.allMetrics().size());
            return new Result(toAggregations(resolved.topMetrics(), state), merged.total, scans);
        }
        GroupTable table = merged.groups != null ? merged.groups : new GroupTable(resolved.keyKinds(), resolved.allMetrics());
        InternalAggregation aggregation;
        if (resolved.topK() != null) {
            aggregation = assembleTopTerms(table, merged.keyedCount, dateHistogramPrototype);
        } else if (resolved.composite() == null && resolved.levels().size() == 1 && isKeyOrderedTerms(resolved.levels().get(0))) {
            aggregation = assembleKeyedTerms(table, dateHistogramPrototype);
        } else {
            List<Group> groups = table.box();
            aggregation = resolved.composite() != null ? buildComposite(groups) : buildLevel(0, groups, dateHistogramPrototype);
        }
        return new Result(InternalAggregations.from(Collections.singletonList(aggregation)), merged.total, scans);
    }

    private static boolean isKeyOrderedTerms(Level level) {
        return level.kind() == LevelKind.TERMS && InternalOrder.isKeyOrder(((TermsAggregationBuilder) level.builder()).order());
    }

    /**
     * The terms result of the merged top-k candidates: the best
     * {@code shard_size} under the request order become the
     * buckets, every other retained or dropped group's count goes
     * to {@code sum_other_doc_count} (the scans counted every keyed
     * row, retained or not), and the doc count error is left at 0
     * for the reduce to derive from the smallest returned bucket,
     * exactly as it does for an aggregator shard result.
     */
    private InternalAggregation assembleTopTerms(GroupTable table, long keyedCount, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(0);
        TermsAggregationBuilder terms = (TermsAggregationBuilder) level.builder();
        TermsAggregator.BucketCountThresholds thresholds = thresholds(terms);
        int[] selected = table.selectTop(thresholds.getShardSize(), topOrder(table));
        List<Candidate> candidates = boxSelection(table, selected);
        long otherDocCount = keyedCount;
        for (Candidate candidate : candidates) {
            otherDocCount -= candidate.count();
        }
        // Shards hand the reduce key sorted buckets when the request
        // order is not itself a key order, and a top-k order never is.
        BucketOrder reduceOrder = BucketOrder.key(true);
        candidates.sort(candidateComparator(reduceOrder, level.keyKind()));
        List<InternalAggregations> subAggregations = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            subAggregations.add(subAggregations(0, candidate, prototype));
        }
        return termsAggregation(level, terms, reduceOrder, thresholds, otherDocCount, candidates, subAggregations);
    }

    /** The selection order of the top-k shape over the merged group table. */
    private GroupTable.GroupOrder topOrder(GroupTable table) {
        if (resolved.topK().sortSlot() < 0) {
            return (a, b) -> {
                int byCount = Long.compare(table.countAt(b), table.countAt(a));
                return byCount != 0 ? byCount : table.compareKeys(a, b);
            };
        }
        int slot = resolved.topK().sortSlot();
        boolean asc = resolved.topK().sortAsc();
        return (a, b) -> {
            int byValue = Comparators.compareDiscardNaN(table.metrics.sortValue(slot, a), table.metrics.sortValue(slot, b), asc);
            return byValue != 0 ? byValue : table.compareKeys(a, b);
        };
    }

    /**
     * A single {@code terms} level ordered by {@code _key}: the
     * groups are sorted and cut in their primitive columns and only
     * the first {@code shard_size} are boxed into buckets, the same
     * selection {@link #buildTerms} makes over boxed candidates.
     */
    private InternalAggregation assembleKeyedTerms(GroupTable table, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(0);
        TermsAggregationBuilder terms = (TermsAggregationBuilder) level.builder();
        TermsAggregator.BucketCountThresholds thresholds = thresholds(terms);
        boolean ascending = InternalOrder.isKeyAsc(terms.order());
        GroupTable.GroupOrder order = ascending ? table::compareKeys : (a, b) -> table.compareKeys(b, a);
        int[] selected = table.selectTop(thresholds.getShardSize(), order);
        List<Candidate> candidates = boxSelection(table, selected);
        long otherDocCount = table.totalCount();
        for (Candidate candidate : candidates) {
            otherDocCount -= candidate.count();
        }
        List<InternalAggregations> subAggregations = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            subAggregations.add(subAggregations(0, candidate, prototype));
        }
        return termsAggregation(level, terms, terms.order(), thresholds, otherDocCount, candidates, subAggregations);
    }

    /** The selected single key groups as boxed candidates, in selection order. */
    private List<Candidate> boxSelection(GroupTable table, int[] selected) {
        List<Candidate> candidates = new ArrayList<>(selected.length);
        for (int idx : selected) {
            Object key = table.boxKey(0, idx);
            GroupState state = new GroupState(table.countAt(idx), table.metrics.boxAll(idx));
            candidates.add(new Candidate(key, state.count, List.of(new Group(Collections.singletonList(key), state))));
        }
        return candidates;
    }

    /**
     * Folds {@code rows} into the bucket aggregation of level
     * {@code depth}: the rows are grouped by that level's key, each
     * key becomes a bucket candidate with the summed count, and the
     * level's kind decides which candidates become buckets. A mask
     * level has a fixed bucket list instead, and a row joins every
     * bucket whose bit its key carries.
     */
    private InternalAggregation buildLevel(int depth, List<Group> rows, Function<String, InternalAggregation> dateHistogramPrototype) {
        Level level = resolved.levels().get(depth);
        if (level.kind().isMask()) {
            return buildMaskLevel(depth, rows, dateHistogramPrototype);
        }
        LinkedHashMap<Object, List<Group>> byKey = new LinkedHashMap<>();
        for (Group row : rows) {
            Object key = row.keys().get(depth);
            if (key == null) {
                continue;
            }
            byKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(row);
        }
        List<Candidate> candidates = new ArrayList<>(byKey.size());
        for (Map.Entry<Object, List<Group>> entry : byKey.entrySet()) {
            long count = 0L;
            for (Group row : entry.getValue()) {
                count += row.count();
            }
            candidates.add(new Candidate(entry.getKey(), count, entry.getValue()));
        }
        return switch (level.kind()) {
            case TERMS -> buildTerms(depth, candidates, dateHistogramPrototype);
            case HISTOGRAM -> buildHistogram(depth, candidates, dateHistogramPrototype);
            case DATE_HISTOGRAM -> buildDateHistogram(depth, candidates, dateHistogramPrototype);
            default -> throw new IllegalStateException("unexpected level kind " + level.kind());
        };
    }

    /**
     * The buckets of a {@code range} / {@code date_range} /
     * {@code filters} / {@code filter} / {@code missing} level: every
     * bucket the request names, in request order, with the rows
     * whose mask has its bit; then the other bucket of a
     * {@code filters} with the rows no filter matched. Every bucket
     * is reported, with a count of 0 and the sub aggregations built
     * over no rows when nothing fell in, as the aggregators do.
     */
    private InternalAggregation buildMaskLevel(int depth, List<Group> rows, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(depth);
        int branches = level.branchKeys().size();
        List<List<Group>> perBucket = new ArrayList<>(level.bucketCount());
        for (int i = 0; i < level.bucketCount(); i++) {
            perBucket.add(new ArrayList<>());
        }
        for (Group row : rows) {
            long mask = (Long) row.keys().get(depth);
            if (mask == 0L) {
                if (level.otherBucketKey() != null) {
                    perBucket.get(branches).add(row);
                }
                continue;
            }
            for (int i = 0; i < branches; i++) {
                if ((mask & (1L << i)) != 0L) {
                    perBucket.get(i).add(row);
                }
            }
        }
        List<Candidate> buckets = new ArrayList<>(level.bucketCount());
        for (int i = 0; i < level.bucketCount(); i++) {
            long count = 0L;
            for (Group row : perBucket.get(i)) {
                count += row.count();
            }
            buckets.add(new Candidate(i < branches ? level.branchKeys().get(i) : level.otherBucketKey(), count, perBucket.get(i)));
        }
        List<InternalAggregations> subAggregations = new ArrayList<>(buckets.size());
        for (Candidate bucket : buckets) {
            subAggregations.add(subAggregations(depth, bucket, prototype));
        }
        return maskAggregation(depth, buckets, subAggregations);
    }

    /**
     * The result of a mask level from its buckets, in order, and
     * their sub aggregations. {@code range} buckets carry the
     * resolved bounds; a {@code filter} / {@code missing} is the one
     * bucket itself.
     */
    private InternalAggregation maskAggregation(int depth, List<Candidate> buckets, List<InternalAggregations> subAggregations) {
        Level level = resolved.levels().get(depth);
        Map<String, Object> metadata = metadata(level.builder());
        switch (level.kind()) {
            case RANGE -> {
                RangeAggregationBuilder range = (RangeAggregationBuilder) level.builder();
                List<InternalRange.Bucket> rangeBuckets = new ArrayList<>(buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    RangeAggregator.Range bounds = level.ranges()[i];
                    rangeBuckets.add(
                        new InternalRange.Bucket(
                            bounds.getKey(),
                            bounds.getFrom(),
                            bounds.getTo(),
                            buckets.get(i).count(),
                            subAggregations.get(i),
                            range.keyed(),
                            level.format()
                        )
                    );
                }
                return new InternalRange<>(range.getName(), rangeBuckets, level.format(), range.keyed(), metadata);
            }
            case DATE_RANGE -> {
                DateRangeAggregationBuilder range = (DateRangeAggregationBuilder) level.builder();
                List<InternalDateRange.Bucket> rangeBuckets = new ArrayList<>(buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    RangeAggregator.Range bounds = level.ranges()[i];
                    rangeBuckets.add(
                        new InternalDateRange.Bucket(
                            bounds.getKey(),
                            bounds.getFrom(),
                            bounds.getTo(),
                            buckets.get(i).count(),
                            subAggregations.get(i),
                            range.keyed(),
                            level.format()
                        )
                    );
                }
                return InternalDateRange.FACTORY.create(range.getName(), rangeBuckets, level.format(), range.keyed(), metadata);
            }
            case FILTERS -> {
                FiltersAggregationBuilder filters = (FiltersAggregationBuilder) level.builder();
                List<InternalFilters.InternalBucket> filterBuckets = new ArrayList<>(buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    Candidate bucket = buckets.get(i);
                    filterBuckets.add(
                        new InternalFilters.InternalBucket((String) bucket.key(), bucket.count(), subAggregations.get(i), filters.isKeyed())
                    );
                }
                return new InternalFilters(filters.getName(), filterBuckets, filters.isKeyed(), metadata);
            }
            case FILTER -> {
                return CoreAggregationResults.filter(level.builder().getName(), buckets.get(0).count(), subAggregations.get(0), metadata);
            }
            case MISSING -> {
                return CoreAggregationResults.missing(level.builder().getName(), buckets.get(0).count(), subAggregations.get(0), metadata);
            }
            default -> throw new IllegalStateException("not a mask level: " + level.kind());
        }
    }

    /**
     * The sub aggregations of one bucket at level {@code depth}: the
     * level's metrics folded over the bucket's rows, and the nested
     * level built from the same rows, in request order.
     */
    private InternalAggregations subAggregations(int depth, Candidate candidate, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(depth);
        if (level.children().isEmpty()) {
            return InternalAggregations.EMPTY;
        }
        // The rows' own states stay untouched: the bucket's values
        // are folded into a fresh state, so the same rows can feed
        // the nested level next.
        GroupState folded = GroupState.empty(resolved.allMetrics().size());
        for (Group row : candidate.rows()) {
            folded.merge(row.state());
        }
        List<InternalAggregation> aggregations = new ArrayList<>(level.children().size());
        for (Child child : level.children()) {
            if (child.metric() != null) {
                aggregations.add(child.metric().toAggregation(folded));
            } else {
                aggregations.add(buildLevel(depth + 1, candidate.rows(), prototype));
            }
        }
        return InternalAggregations.from(aggregations);
    }

    /** What the aggregators of level {@code depth} report for a bucket that saw no document, in request order. */
    private InternalAggregations emptySubAggregations(int depth) {
        Level level = resolved.levels().get(depth);
        if (level.children().isEmpty()) {
            return InternalAggregations.EMPTY;
        }
        List<InternalAggregation> aggregations = new ArrayList<>(level.children().size());
        for (Child child : level.children()) {
            aggregations.add(child.metric() != null ? child.metric().empty() : emptyLevel(depth + 1));
        }
        return InternalAggregations.from(aggregations);
    }

    /**
     * The empty result of level {@code depth} as the aggregator's
     * {@code buildEmptyAggregation} returns it. Only the empty bucket
     * filling of a histogram parent with {@code min_doc_count} 0
     * reads it. {@code terms} keeps the request order as its reduce
     * order here, unlike a built result, which the aggregator sorts
     * by key. A mask level reports every bucket with a count of 0 and
     * the empty sub aggregations.
     */
    private InternalAggregation emptyLevel(int depth) {
        Level level = resolved.levels().get(depth);
        if (level.builder() instanceof TermsAggregationBuilder terms) {
            TermsAggregator.BucketCountThresholds thresholds = thresholds(terms);
            return termsAggregation(level, terms, terms.order(), thresholds, 0L, List.of(), List.of());
        }
        if (level.builder() instanceof HistogramAggregationBuilder histogram) {
            return histogramAggregation(depth, histogram, List.of());
        }
        if (level.builder() instanceof DateHistogramAggregationBuilder dateHistogram) {
            return dateHistogramAggregation(depth, dateHistogram, List.of());
        }
        List<Candidate> buckets = new ArrayList<>(level.bucketCount());
        List<InternalAggregations> subAggregations = new ArrayList<>(level.bucketCount());
        InternalAggregations empty = emptySubAggregations(depth);
        for (int i = 0; i < level.bucketCount(); i++) {
            String key = i < level.branchKeys().size() ? level.branchKeys().get(i) : level.otherBucketKey();
            buckets.add(new Candidate(key, 0L, List.of()));
            subAggregations.add(empty);
        }
        return maskAggregation(depth, buckets, subAggregations);
    }

    private InternalAggregation buildTerms(int depth, List<Candidate> candidates, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(depth);
        TermsAggregationBuilder terms = (TermsAggregationBuilder) level.builder();
        BucketOrder order = terms.order();
        TermsAggregator.BucketCountThresholds thresholds = thresholds(terms);
        int shardSize = thresholds.getShardSize();

        // Keep the first shard_size candidates in request order. The
        // queue's top is the candidate that leaves first, so lessThan
        // is "sorts later".
        Comparator<Candidate> candidateOrder = candidateComparator(order, level.keyKind());
        PriorityQueue<Candidate> queue = new PriorityQueue<>(Math.max(1, Math.min(shardSize, candidates.size()))) {
            @Override
            protected boolean lessThan(Candidate a, Candidate b) {
                return candidateOrder.compare(a, b) > 0;
            }
        };
        long otherDocCount = 0L;
        for (Candidate candidate : candidates) {
            otherDocCount += candidate.count();
            queue.insertWithOverflow(candidate);
        }
        List<Candidate> selected = new ArrayList<>(queue.size());
        while (queue.size() > 0) {
            selected.add(queue.pop());
        }
        Collections.reverse(selected);
        for (Candidate candidate : selected) {
            otherDocCount -= candidate.count();
        }
        // Shards hand the reduce key sorted buckets unless the
        // request order is itself a key order.
        BucketOrder reduceOrder;
        if (InternalOrder.isKeyOrder(order)) {
            reduceOrder = order;
        } else {
            reduceOrder = BucketOrder.key(true);
            selected.sort(candidateComparator(reduceOrder, level.keyKind()));
        }
        // Sub aggregations only for the kept buckets: the rows of a
        // dropped bucket, nested levels included, go with it.
        List<InternalAggregations> subAggregations = new ArrayList<>(selected.size());
        for (Candidate candidate : selected) {
            subAggregations.add(subAggregations(depth, candidate, prototype));
        }
        return termsAggregation(level, terms, reduceOrder, thresholds, otherDocCount, selected, subAggregations);
    }

    private InternalAggregation buildHistogram(int depth, List<Candidate> candidates, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(depth);
        HistogramAggregationBuilder histogram = (HistogramAggregationBuilder) level.builder();
        double interval = histogram.interval();
        double offset = histogram.offset();
        candidates.sort(Comparator.comparingLong(candidate -> (Long) candidate.key()));
        List<InternalHistogram.Bucket> buckets = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            // The scan returns floor((value - offset) / interval); the
            // aggregator multiplies back the same way.
            double key = (double) (Long) candidate.key() * interval + offset;
            buckets.add(
                new InternalHistogram.Bucket(
                    key,
                    candidate.count(),
                    histogram.keyed(),
                    level.format(),
                    subAggregations(depth, candidate, prototype)
                )
            );
        }
        return histogramAggregation(depth, histogram, buckets);
    }

    private InternalAggregation histogramAggregation(
        int depth,
        HistogramAggregationBuilder histogram,
        List<InternalHistogram.Bucket> buckets
    ) {
        Level level = resolved.levels().get(depth);
        InternalHistogram.EmptyBucketInfo emptyBucketInfo = null;
        if (histogram.minDocCount() == 0L) {
            emptyBucketInfo = new InternalHistogram.EmptyBucketInfo(
                histogram.interval(),
                histogram.offset(),
                histogram.minBound(),
                histogram.maxBound(),
                emptySubAggregations(depth)
            );
        }
        return new InternalHistogram(
            histogram.getName(),
            buckets,
            histogram.order(),
            histogram.minDocCount(),
            emptyBucketInfo,
            level.format(),
            histogram.keyed(),
            metadata(histogram)
        );
    }

    private InternalAggregation buildDateHistogram(
        int depth,
        List<Candidate> candidates,
        Function<String, InternalAggregation> dateHistogramPrototype
    ) {
        Level level = resolved.levels().get(depth);
        DateHistogramAggregationBuilder dateHistogram = (DateHistogramAggregationBuilder) level.builder();
        candidates.sort(Comparator.comparingLong(candidate -> (Long) candidate.key()));
        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            buckets.add(
                new InternalDateHistogram.Bucket(
                    (Long) candidate.key(),
                    candidate.count(),
                    dateHistogram.keyed(),
                    level.format(),
                    subAggregations(depth, candidate, dateHistogramPrototype)
                )
            );
        }
        if (depth == 0) {
            // The prototype carries the rounding, offset, order,
            // min_doc_count, empty bucket info and format the
            // aggregator computed for this request; create() copies
            // them.
            InternalDateHistogram prototype = (InternalDateHistogram) dateHistogramPrototype.apply(dateHistogram.getName());
            return prototype.create(buckets);
        }
        return dateHistogramAggregation(depth, dateHistogram, buckets);
    }

    /**
     * A nested {@code date_histogram} result with the fields the
     * aggregator would set; the level's rounding is the fixed
     * interval or the calendar unit in UTC with no offset, the only
     * shapes the pushdown accepts.
     */
    private InternalAggregation dateHistogramAggregation(
        int depth,
        DateHistogramAggregationBuilder dateHistogram,
        List<InternalDateHistogram.Bucket> buckets
    ) {
        Level level = resolved.levels().get(depth);
        return CoreAggregationResults.dateHistogram(
            dateHistogram.getName(),
            buckets,
            dateHistogram.order(),
            dateHistogram.minDocCount(),
            level.rounding(),
            dateHistogram.minDocCount() == 0L ? emptySubAggregations(depth) : null,
            level.format(),
            dateHistogram.keyed(),
            metadata(dateHistogram)
        );
    }

    private InternalAggregation termsAggregation(
        Level level,
        TermsAggregationBuilder terms,
        BucketOrder reduceOrder,
        TermsAggregator.BucketCountThresholds thresholds,
        long otherDocCount,
        List<Candidate> selected,
        List<InternalAggregations> subAggregations
    ) {
        BucketOrder order = terms.order();
        boolean showError = terms.showTermDocCountError();
        String name = terms.getName();
        Map<String, Object> metadata = metadata(terms);
        DocValueFormat format = level.format();
        int shardSize = thresholds.getShardSize();
        switch (level.keyKind()) {
            case STRING -> {
                List<StringTerms.Bucket> buckets = new ArrayList<>(selected.size());
                for (int i = 0; i < selected.size(); i++) {
                    Candidate candidate = selected.get(i);
                    buckets.add(
                        new StringTerms.Bucket((BytesRef) candidate.key(), candidate.count(), subAggregations.get(i), showError, 0L, format)
                    );
                }
                return new StringTerms(
                    name,
                    reduceOrder,
                    order,
                    metadata,
                    format,
                    shardSize,
                    showError,
                    otherDocCount,
                    buckets,
                    0L,
                    thresholds
                );
            }
            case LONG -> {
                List<LongTerms.Bucket> buckets = new ArrayList<>(selected.size());
                for (int i = 0; i < selected.size(); i++) {
                    Candidate candidate = selected.get(i);
                    buckets.add(
                        new LongTerms.Bucket((Long) candidate.key(), candidate.count(), subAggregations.get(i), showError, 0L, format)
                    );
                }
                return new LongTerms(
                    name,
                    reduceOrder,
                    order,
                    metadata,
                    format,
                    shardSize,
                    showError,
                    otherDocCount,
                    buckets,
                    0L,
                    thresholds
                );
            }
            case DOUBLE -> {
                List<DoubleTerms.Bucket> buckets = new ArrayList<>(selected.size());
                for (int i = 0; i < selected.size(); i++) {
                    Candidate candidate = selected.get(i);
                    buckets.add(
                        new DoubleTerms.Bucket((Double) candidate.key(), candidate.count(), subAggregations.get(i), showError, 0L, format)
                    );
                }
                return new DoubleTerms(
                    name,
                    reduceOrder,
                    order,
                    metadata,
                    format,
                    shardSize,
                    showError,
                    otherDocCount,
                    buckets,
                    0L,
                    thresholds
                );
            }
            default -> throw new IllegalStateException("unexpected key kind " + level.keyKind());
        }
    }

    /**
     * The composite page of this node: every key combination after
     * {@code after} in source order, the first {@code size} of them
     * as buckets, the last one as {@code after_key}.
     */
    private InternalAggregation buildComposite(List<Group> groups) {
        CompositeAggregationBuilder builder = resolved.composite().builder();
        List<Source> sources = resolved.composite().sources();
        List<Group> page = new ArrayList<>();
        for (Group group : groups) {
            if (compareToAfter(group) > 0) {
                page.add(group);
            }
        }
        page.sort(compositeOrder());
        int size = Math.min(builder.size(), page.size());

        List<String> sourceNames = new ArrayList<>(sources.size());
        List<DocValueFormat> formats = new ArrayList<>(sources.size());
        int[] reverseMuls = new int[sources.size()];
        MissingOrder[] missingOrders = new MissingOrder[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            Source source = sources.get(i);
            sourceNames.add(source.builder().name());
            formats.add(source.format());
            reverseMuls[i] = source.reverseMul();
            missingOrders[i] = source.builder().missingOrder();
        }
        List<InternalComposite.InternalBucket> buckets = new ArrayList<>(size);
        CompositeKey afterKey = null;
        for (int i = 0; i < size; i++) {
            Group group = page.get(i);
            Comparable<?>[] values = new Comparable<?>[sources.size()];
            for (int s = 0; s < sources.size(); s++) {
                values[s] = (Comparable<?>) group.keys().get(s);
            }
            CompositeKey key = CoreAggregationResults.compositeKey(values);
            List<InternalAggregation> metrics = new ArrayList<>(resolved.composite().metrics().size());
            for (Metric metric : resolved.composite().metrics()) {
                metrics.add(metric.toAggregation(group.state()));
            }
            buckets.add(
                CoreAggregationResults.compositeBucket(
                    sourceNames,
                    formats,
                    key,
                    reverseMuls,
                    missingOrders,
                    group.count(),
                    InternalAggregations.from(metrics)
                )
            );
            afterKey = key;
        }
        return CoreAggregationResults.composite(
            builder.getName(),
            builder.size(),
            sourceNames,
            formats,
            buckets,
            afterKey,
            reverseMuls,
            missingOrders,
            metadata(builder)
        );
    }

    /** Source by source comparison in each source's direction, the composite's bucket order. */
    private Comparator<Group> compositeOrder() {
        List<Source> sources = resolved.composite().sources();
        Comparator<Group> order = null;
        for (int i = 0; i < sources.size(); i++) {
            int index = i;
            Source source = sources.get(i);
            Comparator<Group> bySource = (a, b) -> compareKeys(source.keyKind(), a.keys().get(index), b.keys().get(index)) * source
                .reverseMul();
            order = order == null ? bySource : order.thenComparing(bySource);
        }
        return order;
    }

    /**
     * Where {@code group} sorts relative to the request's {@code after}
     * key in the composite order; positive when it belongs to this
     * page. Always positive without an {@code after}.
     */
    private int compareToAfter(Group group) {
        List<Source> sources = resolved.composite().sources();
        if (sources.get(0).after() == null) {
            return 1;
        }
        for (int i = 0; i < sources.size(); i++) {
            Source source = sources.get(i);
            int cmp = compareKeys(source.keyKind(), group.keys().get(i), source.after()) * source.reverseMul();
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private static InternalAggregations toAggregations(List<Metric> metrics, GroupState state) {
        if (metrics.isEmpty()) {
            return InternalAggregations.EMPTY;
        }
        List<InternalAggregation> values = new ArrayList<>(metrics.size());
        for (Metric metric : metrics) {
            values.add(metric.toAggregation(state));
        }
        return InternalAggregations.from(values);
    }

    /**
     * Ordering of candidates for the terms selection: {@code _count}
     * descending with the key ascending as tie breaker, or the key in
     * the requested direction. Keys compare the way the terms buckets
     * compare them: {@link BytesRef} order for strings, numeric order
     * otherwise.
     */
    private static Comparator<Candidate> candidateComparator(BucketOrder order, KeyKind keyKind) {
        Comparator<Candidate> byKey = (a, b) -> compareKeys(keyKind, a.key(), b.key());
        if (InternalOrder.isKeyOrder(order)) {
            return InternalOrder.isKeyAsc(order) ? byKey : byKey.reversed();
        }
        return Comparator.comparingLong(Candidate::count).reversed().thenComparing(byKey);
    }

    private static int compareKeys(KeyKind keyKind, Object a, Object b) {
        return switch (keyKind) {
            case STRING -> ((BytesRef) a).compareTo((BytesRef) b);
            case LONG -> Long.compare((Long) a, (Long) b);
            case DOUBLE -> Double.compare((Double) a, (Double) b);
        };
    }
}

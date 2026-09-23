/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.opensearch.lance.dispatch.LanceFragmentQueryRequest;
import org.opensearch.lance.execute.LanceAggregateResults;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.sort.SortAndFormats;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * The fragment executor's side of planning: the shipped
 * {@link FragmentPlan} is checked against what only the data node
 * knows and downgraded where a pushed operation cannot run here.
 * Downgrades go one way, from the Lance scan to Lucene; nothing is
 * pushed on the data node that the coordinator did not push.
 *
 * <p>Three guards exist. {@link Reason#SECURITY_WRAPPER}: a reader
 * wrapper on the index service (the security plugin's DLS / FLS)
 * filters documents on the Lucene side, which a Lance scan never sees,
 * so a pushed aggregate, a pushed page and a pushed full text clause
 * move to the aggregators, the collector and the Lucene composition of
 * the request's query; a pushed knn stays, because its filter is the
 * caller's own predicate applied before the wrapper narrows the hits
 * on the Lucene side, and the scalar filter SQL stays, because the
 * leaf readers' column materialisation under it is compatible with
 * the wrapper. {@link Reason#SORT_FIELD_TYPE}: the pushed page types
 * its sort values from the Lucene {@link SortField}s the mapping
 * built, so a sort field outside the two plain field data shapes
 * (numeric INT / LONG / FLOAT / DOUBLE, or a sorted set), an {@code ip}
 * format (address order is not the stored strings' order), a sort
 * count that differs from the request's, or a {@code search_after}
 * cursor equal to a sort field's missing value sentinel (the strict
 * SQL bound cannot tell the sentinel from a stored value) sends the
 * page to the collector. {@link Reason#AGGREGATE_RESOLUTION}: the
 * executor's resolution of the pushed aggregate against the mapping
 * ({@link LanceAggregateResults#resolve}: field types, the group
 * estimate bound, the terms top-k selection) refused, and the
 * aggregators run.
 *
 * <p>Every downgrade is counted per reason in {@link #refinementCounts}
 * for {@code GET /_lance/stats}, and the caller logs the planned and
 * the executed plan.
 */
public final class FragmentPlanRefiner {

    /** Why a pushed operation moved to the Lucene side. */
    public enum Reason {
        SECURITY_WRAPPER,
        SORT_FIELD_TYPE,
        AGGREGATE_RESOLUTION;

        /** The key under {@code plan.refinements} in the stats. */
        public String statsKey() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * What the data node knows that the coordinator did not: whether a
     * reader wrapper is installed, the Lucene sort the mapping built for
     * the request's sort clauses (null when the request has none), the
     * request itself, and the resolution of a pushed aggregate against
     * the mapping (null when the request carries none; returns null when
     * the resolution refuses).
     */
    public record Inputs(boolean hasSecurityWrapper, SortAndFormats sortAndFormats, LanceFragmentQueryRequest request, Function<
        FragmentPlan.Aggregate,
        LanceAggregateResults> aggregateResolver) {
    }

    /**
     * The plan to execute, the reasons that changed it (empty when the
     * shipped plan runs as is), and the resolved aggregate executor when
     * the plan still carries a pushed aggregate.
     */
    public record Refined(FragmentPlan plan, List<Reason> reasons, LanceAggregateResults aggregate) {

        public Refined {
            reasons = List.copyOf(reasons);
        }

        public boolean refined() {
            return !reasons.isEmpty();
        }
    }

    private static final Map<Reason, LongAdder> COUNTS = new ConcurrentHashMap<>();

    /** Applies the guards to {@code planned}, counting every reason that fired. */
    public Refined refine(FragmentPlan planned, Inputs in) {
        List<Reason> reasons = new ArrayList<>();
        FragmentPlan plan = planned;
        if (in.hasSecurityWrapper()) {
            FragmentPlan before = plan;
            plan = plan.withoutAggregate().withoutTopK();
            if (!plan.isKnn()) {
                plan = plan.withoutLanceClause();
            }
            if (plan != before) {
                reasons.add(Reason.SECURITY_WRAPPER);
            }
        }
        if (plan.topK() != null && !plan.topK().orderings().isEmpty() && !sortFieldsSupportThePage(plan.topK(), in)) {
            plan = plan.withoutTopK();
            reasons.add(Reason.SORT_FIELD_TYPE);
        }
        LanceAggregateResults aggregate = null;
        if (plan.aggregate() != null) {
            aggregate = in.aggregateResolver() == null ? null : in.aggregateResolver().apply(plan.aggregate());
            if (aggregate == null) {
                plan = plan.withoutAggregate();
                reasons.add(Reason.AGGREGATE_RESOLUTION);
            }
        }
        for (Reason reason : reasons) {
            COUNTS.computeIfAbsent(reason, r -> new LongAdder()).increment();
        }
        return new Refined(plan, reasons, aggregate);
    }

    /**
     * Whether the Lucene sort the mapping built lets the executor type
     * the pushed page's sort values: one sort field per request sort
     * clause, each a numeric INT / LONG / FLOAT / DOUBLE or a sorted set
     * field, no {@code ip} format, and no cursor at a missing value
     * sentinel.
     */
    private static boolean sortFieldsSupportThePage(FragmentPlan.TopK topK, Inputs in) {
        SortAndFormats sortAndFormats = in.sortAndFormats();
        if (sortAndFormats == null) {
            return false;
        }
        SortField[] sortFields = sortAndFormats.sort.getSort();
        if (sortFields.length != in.request().sorts().size()) {
            return false;
        }
        for (SortField sortField : sortFields) {
            if (sortField instanceof SortedNumericSortField numeric) {
                switch (numeric.getNumericType()) {
                    case INT, LONG, FLOAT, DOUBLE -> {
                    }
                    default -> {
                        return false;
                    }
                }
            } else if (!(sortField instanceof SortedSetSortField)) {
                return false;
            }
        }
        for (DocValueFormat format : sortAndFormats.formats) {
            if (format == DocValueFormat.IP) {
                return false;
            }
        }
        Object[] searchAfter = in.request().searchAfter();
        if (searchAfter != null && (searchAfter.length != sortFields.length || cursorHitsMissingSentinel(searchAfter, sortFields))) {
            return false;
        }
        return true;
    }

    /**
     * Whether a {@code search_after} value equals the missing-value
     * sentinel its sort field reports for null rows. A client paging
     * past a null row feeds the sentinel back; the strict SQL bound
     * compares stored values only, so such a cursor stays on the
     * Lucene comparator, which knows the sentinel.
     */
    static boolean cursorHitsMissingSentinel(Object[] searchAfter, SortField[] sortFields) {
        for (int i = 0; i < searchAfter.length; i++) {
            Object missing = sortFields[i].getMissingValue();
            if (missing instanceof Number sentinel
                && searchAfter[i] instanceof Number cursor
                && Double.compare(sentinel.doubleValue(), cursor.doubleValue()) == 0) {
                return true;
            }
        }
        return false;
    }

    /** Downgrades since the node started, keyed by {@link Reason#statsKey()}, every reason present. */
    public static Map<String, Long> refinementCounts() {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Reason reason : Reason.values()) {
            LongAdder adder = COUNTS.get(reason);
            counts.put(reason.statsKey(), adder == null ? 0L : adder.sum());
        }
        return counts;
    }
}

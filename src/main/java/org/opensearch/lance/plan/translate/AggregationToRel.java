/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.common.Rounding;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.lance.plan.calcite.LanceOperatorTable;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.BucketSpec;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.BucketUtils;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.AbstractRangeBuilder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesConfig;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates the aggregation tree of a search request to a
 * {@link LanceAggregate} over the scan on top of the given
 * {@link RelBuilder}. The supported set is the set the aggregation
 * pushdown computes inside the Lance scan today: metric only trees, a
 * chain of up to {@link #MAX_BUCKET_DEPTH} bucket levels with metric
 * children, and {@code composite} over {@code terms} / fixed length
 * {@code date_histogram} sources with metric children.
 *
 * <p>Group keys become one expression per bucket level (a field
 * reference for {@code terms}; the bucket ordinal
 * {@code FLOOR(col / interval)} for the histograms, which the executor
 * multiplies back; {@code LANCE_DATE_TRUNC(unit, col)} for a
 * calendar {@code date_histogram}; a {@code CASE WHEN} bit mask for
 * {@code range} / {@code date_range} / {@code filter} / {@code filters}
 * / {@code missing}, under which a row counts toward every matching
 * branch), projected under the aggregate. Metric children at every
 * level become {@link AggregateCall}s. The OpenSearch options Calcite
 * does not model ride along as {@link BucketSpec} / {@link MetricSpec}.
 *
 * <p>Every shape the pushdown refuses throws
 * {@link UnsupportedOperationException} naming the element; the explain
 * endpoint reports the message as a 400. Field resolution follows the
 * pushdown's rules against the Arrow schema and the attach declared
 * multi fields; the mapping type checks of the executor's
 * {@code QueryShardContext} have no equivalent on the coordinating
 * node, so the Arrow column type stands in for the mapping type.
 */
final class AggregationToRel {

    private AggregationToRel() {}

    /** Deepest bucket chain the pushdown builds ({@code terms > terms > terms}). */
    static final int MAX_BUCKET_DEPTH = 3;

    /** Most conditions a {@code CASE WHEN} bit mask encodes without touching the sign bit. */
    static final int MAX_MASK_CONDITIONS = 62;

    /**
     * Translates the request's aggregations over the scan the builder
     * holds. The caller has already rejected pipeline aggregations and
     * an empty tree.
     */
    static RelNode translate(AggregatorFactories.Builder aggregations, LanceSchemas.IndexModel model, RelBuilder relBuilder) {
        List<AggregationBuilder> top = new ArrayList<>(aggregations.getAggregatorFactories());
        Translation translation = new Translation(model.arrowSchema(), model.multiFields(), relBuilder);
        boolean allMetrics = top.stream().allMatch(AggregationToRel::isMetricShaped);
        if (allMetrics) {
            return translation.metricOnly(top);
        }
        if (top.size() != 1) {
            throw unsupported("two top level aggregations");
        }
        AggregationBuilder root = top.get(0);
        if (root instanceof CompositeAggregationBuilder composite) {
            return translation.composite(composite);
        }
        return translation.bucketTree(root);
    }

    /** One metric to become an aggregate call, resolved but not yet bound to input positions. */
    private record MetricIntent(AggregationBuilder builder, MetricSpec.Kind kind, Column column, MetricSpec spec) {
    }

    /** A field resolved to its Lance column. */
    record Column(String name, int index, ArrowType type) {
        boolean isDate() {
            return type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp;
        }

        boolean isUtcTimestamp() {
            if (!(type instanceof ArrowType.Timestamp timestamp)) {
                return false;
            }
            String zone = timestamp.getTimezone();
            return zone == null || zone.equals("UTC") || zone.equals("Etc/UTC") || zone.equals("+00:00");
        }

        boolean isBoolean() {
            return type instanceof ArrowType.Bool;
        }

        boolean isFloating() {
            return type instanceof ArrowType.FloatingPoint;
        }

        boolean isSingleFloat() {
            return type instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == FloatingPointPrecision.SINGLE;
        }

        boolean isUtf8() {
            return type instanceof ArrowType.Utf8;
        }
    }

    /**
     * The state of one translation: the resolved metrics in call order,
     * the group key expressions and specs in level / source order, and
     * the filter predicates parallel to the specs.
     */
    private static final class Translation {

        private final Schema schema;
        private final Map<String, LinkedHashMap<String, String>> multiFields;
        private final RelBuilder relBuilder;
        private final List<RexNode> keyExpressions = new ArrayList<>();
        private final List<BucketSpec> bucketSpecs = new ArrayList<>();
        private final List<List<RexNode>> filterPredicates = new ArrayList<>();
        private final List<MetricIntent> metrics = new ArrayList<>();

        Translation(Schema schema, Map<String, LinkedHashMap<String, String>> multiFields, RelBuilder relBuilder) {
            this.schema = schema;
            this.multiFields = multiFields;
            this.relBuilder = relBuilder;
        }

        /** Metric only: the aggregate sits directly on the scan with an empty group set. */
        RelNode metricOnly(List<AggregationBuilder> top) {
            for (AggregationBuilder builder : top) {
                addMetric(builder);
            }
            RelNode input = relBuilder.build();
            List<AggregateCall> calls = new ArrayList<>(metrics.size());
            for (MetricIntent metric : metrics) {
                calls.add(call(metric, metric.column().index(), 0, input));
            }
            return LanceAggregate.create(input, ImmutableBitSet.of(), calls, List.of(), specsOf(metrics), List.of());
        }

        /** One key and one {@code COMPOSITE_*} spec per source, metric children only. */
        RelNode composite(CompositeAggregationBuilder composite) {
            if (!composite.getPipelineAggregations().isEmpty()) {
                throw unsupported("pipeline aggregation");
            }
            if (composite.sources().isEmpty()) {
                throw unsupported("composite [" + composite.getName() + "] without sources");
            }
            Map<String, Object> after = afterKey(composite);
            for (CompositeValuesSourceBuilder<?> source : composite.sources()) {
                addCompositeSource(source, after, composite.size());
            }
            for (AggregationBuilder sub : composite.getSubAggregations()) {
                if (!isMetricShaped(sub)) {
                    throw unsupported("aggregation type [" + sub.getType() + "] under composite [" + composite.getName() + "]");
                }
                addMetric(sub);
            }
            return assemble();
        }

        /**
         * One key and spec per level of the bucket chain, at most
         * {@link #MAX_BUCKET_DEPTH} levels, metric children plus at most
         * one nested bucket per level.
         */
        RelNode bucketTree(AggregationBuilder root) {
            AggregationBuilder current = root;
            int depth = 0;
            while (current != null) {
                depth++;
                if (depth > MAX_BUCKET_DEPTH) {
                    throw unsupported("bucket tree deeper than " + MAX_BUCKET_DEPTH + " levels");
                }
                if (!current.getPipelineAggregations().isEmpty()) {
                    throw unsupported("pipeline aggregation");
                }
                addBucketLevel(current);
                AggregationBuilder nested = null;
                for (AggregationBuilder sub : current.getSubAggregations()) {
                    if (isMetricShaped(sub)) {
                        addMetric(sub);
                    } else if (nested != null) {
                        throw unsupported("two bucket aggregations under [" + current.getName() + "]");
                    } else {
                        nested = sub;
                    }
                }
                current = nested;
            }
            checkTermsOrders();
            return assemble();
        }

        /**
         * Projects the key expressions followed by a pass through of
         * every scan column, then builds the aggregate over the
         * projection. Key fields take the bucket aggregation names, the
         * pass through columns their own names (the builder uniquifies
         * clashes); metric arguments and the stored filter predicates
         * reference the pass through positions, so both are valid over
         * the aggregate's input row type.
         */
        private RelNode assemble() {
            int keyCount = keyExpressions.size();
            RelDataType scanRowType = relBuilder.peek().getRowType();
            List<RexNode> projections = new ArrayList<>(keyExpressions);
            List<String> names = new ArrayList<>(keyCount + scanRowType.getFieldCount());
            for (BucketSpec spec : bucketSpecs) {
                names.add(spec.aggregationName());
            }
            for (int i = 0; i < scanRowType.getFieldCount(); i++) {
                projections.add(relBuilder.field(i));
                names.add(scanRowType.getFieldList().get(i).getName());
            }
            relBuilder.project(projections, names, true);
            RelNode input = relBuilder.build();
            List<AggregateCall> calls = new ArrayList<>(metrics.size());
            for (MetricIntent metric : metrics) {
                calls.add(call(metric, keyCount + metric.column().index(), keyCount, input));
            }
            // The filter predicates were built over the scan; the pass
            // through puts scan column i at position keyCount + i, so a
            // uniform shift makes them expressions over the projection.
            List<List<RexNode>> shiftedPredicates = new ArrayList<>(filterPredicates.size());
            for (List<RexNode> perKey : filterPredicates) {
                List<RexNode> shifted = new ArrayList<>(perKey.size());
                for (RexNode predicate : perKey) {
                    shifted.add(RexUtil.shift(predicate, keyCount));
                }
                shiftedPredicates.add(shifted);
            }
            return LanceAggregate.create(input, ImmutableBitSet.range(keyCount), calls, bucketSpecs, specsOf(metrics), shiftedPredicates);
        }

        /**
         * A terms order that is neither a count descending nor a key
         * order is honoured by the top-k selection of the single level
         * shape only, ordering on one of the level's own single value
         * metric children, and never next to a sketch metric; every
         * other tree stays on the aggregators, so it throws here.
         */
        private void checkTermsOrders() {
            for (int level = 0; level < bucketSpecs.size(); level++) {
                BucketSpec spec = bucketSpecs.get(level);
                if (spec.kind() != BucketSpec.Kind.TERMS || spec.order().kind() != BucketSpec.OrderSpec.Kind.SUB_AGGREGATION) {
                    continue;
                }
                boolean sketch = metrics.stream()
                    .anyMatch(
                        metric -> metric.kind() == MetricSpec.Kind.CARDINALITY
                            || metric.kind() == MetricSpec.Kind.PERCENTILES
                            || metric.kind() == MetricSpec.Kind.PERCENTILE_RANKS
                    );
                boolean resolvable = bucketSpecs.size() == 1 && !sketch && namesOwnSingleValueMetric(spec.order().path());
                if (!resolvable) {
                    throw unsupported("terms order [" + spec.order().path() + "] on aggregation [" + spec.aggregationName() + "]");
                }
            }
        }

        private boolean namesOwnSingleValueMetric(String path) {
            for (MetricIntent metric : metrics) {
                boolean singleValue = switch (metric.kind()) {
                    case SUM, AVG, MIN, MAX, VALUE_COUNT -> true;
                    default -> false;
                };
                String name = metric.builder().getName();
                if (singleValue && (name.equals(path) || (name + ".value").equals(path))) {
                    return true;
                }
            }
            return false;
        }

        private void addBucketLevel(AggregationBuilder builder) {
            if (builder instanceof FilterAggregationBuilder || builder instanceof FiltersAggregationBuilder) {
                addFilterLevel(builder);
                return;
            }
            if (!(builder instanceof ValuesSourceAggregationBuilder<?> valuesSource)) {
                throw unsupported("aggregation type [" + builder.getType() + "]");
            }
            checkPlainFieldSource(valuesSource);
            Column column = resolveColumn(valuesSource.field(), schema, multiFields);
            if (builder instanceof TermsAggregationBuilder terms) {
                addTermsLevel(terms, column);
            } else if (builder instanceof HistogramAggregationBuilder histogram) {
                addHistogramLevel(histogram, column);
            } else if (builder instanceof DateHistogramAggregationBuilder dateHistogram) {
                addDateHistogramLevel(dateHistogram, column);
            } else if (builder instanceof AbstractRangeBuilder<?, ?> range) {
                addRangeLevel(range, column);
            } else if (builder instanceof MissingAggregationBuilder missing) {
                addMissingLevel(missing, column);
            } else {
                throw unsupported("aggregation type [" + builder.getType() + "]");
            }
        }

        private void addTermsLevel(TermsAggregationBuilder terms, Column column) {
            if (terms.minDocCount() != 1L) {
                throw unsupported("min_doc_count [" + terms.minDocCount() + "] on aggregation [" + terms.getName() + "]");
            }
            if (terms.shardMinDocCount() != 0L) {
                throw unsupported("shard_min_doc_count [" + terms.shardMinDocCount() + "] on aggregation [" + terms.getName() + "]");
            }
            IncludeExclude includeExclude = terms.includeExclude();
            if (includeExclude != null) {
                throw unsupported("include/exclude on aggregation [" + terms.getName() + "]");
            }
            RexNode key;
            if (column.isUtf8() || column.isFloating()) {
                key = relBuilder.field(column.index());
            } else {
                key = numericKey(column);
            }
            addKey(
                key,
                BucketSpec.terms(terms.getName(), terms.size(), shardSizeOf(terms), terms.minDocCount(), orderOf(terms), terms.format()),
                List.of()
            );
        }

        private void addHistogramLevel(HistogramAggregationBuilder histogram, Column column) {
            if (histogram.offset() != 0d) {
                throw unsupported("offset on aggregation [" + histogram.getName() + "]");
            }
            if (histogram.minBound() != Double.POSITIVE_INFINITY || histogram.maxBound() != Double.NEGATIVE_INFINITY) {
                throw unsupported("extended_bounds on aggregation [" + histogram.getName() + "]");
            }
            if (mentionsHardBounds(histogram)) {
                throw unsupported("hard_bounds on aggregation [" + histogram.getName() + "]");
            }
            if (histogram.interval() <= 0d) {
                throw unsupported("interval [" + histogram.interval() + "] on aggregation [" + histogram.getName() + "]");
            }
            if (column.isUtf8() || column.isDate() || column.isBoolean()) {
                throw notNumeric(column, histogram.field());
            }
            RexNode interval = relBuilder.literal(histogram.interval());
            RexNode quotient = relBuilder.call(
                SqlStdOperatorTable.DIVIDE,
                relBuilder.cast(relBuilder.field(column.index()), SqlTypeName.DOUBLE),
                interval
            );
            // The key is the bucket ordinal floor((value) / interval),
            // not the bucket start: the executor multiplies the interval
            // back when it builds the buckets, and the Substrait
            // producer's floor rewrite yields the ordinal with floor
            // (not truncation) semantics for negative values. The cast
            // pins the scan's key column to i64.
            RexNode key = relBuilder.cast(relBuilder.call(SqlStdOperatorTable.FLOOR, quotient), SqlTypeName.BIGINT);
            addKey(
                key,
                BucketSpec.histogram(histogram.getName(), histogram.interval(), histogram.minDocCount(), histogram.format()),
                List.of()
            );
        }

        private void addDateHistogramLevel(DateHistogramAggregationBuilder dateHistogram, Column column) {
            if (dateHistogram.offset() != 0L) {
                throw unsupported("offset on aggregation [" + dateHistogram.getName() + "]");
            }
            if (dateHistogram.timeZone() != null) {
                throw unsupported("time_zone on aggregation [" + dateHistogram.getName() + "]");
            }
            if (dateHistogram.extendedBounds() != null) {
                throw unsupported("extended_bounds on aggregation [" + dateHistogram.getName() + "]");
            }
            if (dateHistogram.hardBounds() != null) {
                throw unsupported("hard_bounds on aggregation [" + dateHistogram.getName() + "]");
            }
            if (!column.isDate()) {
                throw new UnsupportedOperationException(
                    "column [" + column.name() + "] behind aggregation field [" + dateHistogram.field() + "] is not a date"
                );
            }
            String calendarUnit = calendarUnitOf(dateHistogram);
            if (calendarUnit != null) {
                if (!column.isUtcTimestamp()) {
                    throw new UnsupportedOperationException(
                        "calendar_interval on column ["
                            + column.name()
                            + "] behind aggregation field ["
                            + dateHistogram.field()
                            + "] (needs a timestamp column in UTC)"
                    );
                }
                RexNode key = relBuilder.call(
                    LanceOperatorTable.LANCE_DATE_TRUNC,
                    relBuilder.literal(calendarUnit),
                    relBuilder.field(column.index())
                );
                addKey(
                    key,
                    BucketSpec.dateHistogramCalendar(
                        dateHistogram.getName(),
                        calendarUnit,
                        dateHistogram.minDocCount(),
                        dateHistogram.format()
                    ),
                    List.of()
                );
                return;
            }
            if (dateHistogram.getFixedInterval() == null) {
                throw unsupported("interval on aggregation [" + dateHistogram.getName() + "]");
            }
            long intervalMillis = fixedIntervalMillisOrZero(dateHistogram.getFixedInterval().toString());
            if (intervalMillis <= 0L) {
                throw unsupported(
                    "fixed_interval [" + dateHistogram.getFixedInterval() + "] on aggregation [" + dateHistogram.getName() + "]"
                );
            }
            RexNode interval = relBuilder.literal(intervalMillis);
            RexNode quotient = relBuilder.call(SqlStdOperatorTable.DIVIDE, epochMillis(column), interval);
            // The bucket ordinal, as for the numeric histogram; the
            // executor multiplies the interval back.
            RexNode key = relBuilder.call(SqlStdOperatorTable.FLOOR, quotient);
            addKey(
                key,
                BucketSpec.dateHistogramFixed(dateHistogram.getName(), intervalMillis, dateHistogram.minDocCount(), dateHistogram.format()),
                List.of()
            );
        }

        private void addRangeLevel(AbstractRangeBuilder<?, ?> range, Column column) {
            boolean date = range instanceof DateRangeAggregationBuilder;
            if (date && !column.isDate()) {
                throw new UnsupportedOperationException(
                    "column [" + column.name() + "] behind aggregation field [" + range.field() + "] is not a date"
                );
            }
            if (!date && (column.isUtf8() || column.isBoolean())) {
                throw notNumeric(column, range.field());
            }
            List<? extends RangeAggregator.Range> requested = range.ranges();
            if (requested.isEmpty()) {
                throw unsupported("range aggregation [" + range.getName() + "] without ranges");
            }
            if (requested.size() > MAX_MASK_CONDITIONS) {
                throw unsupported("more than " + MAX_MASK_CONDITIONS + " ranges on aggregation [" + range.getName() + "]");
            }
            RangeAggregator.Range[] resolved = resolveRanges(range, column, date);
            List<BucketSpec.RangeSpec> rangeSpecs = new ArrayList<>(resolved.length);
            List<RexNode> conditions = new ArrayList<>(resolved.length);
            // The compared value is epoch millis whenever the column is
            // a date, a plain `range` over a date field included: the
            // aggregator compares the doc value millis, and the resolved
            // bounds are millis.
            RexNode value = relBuilder.cast(column.isDate() ? epochMillis(column) : relBuilder.field(column.index()), SqlTypeName.DOUBLE);
            for (RangeAggregator.Range one : resolved) {
                rangeSpecs.add(new BucketSpec.RangeSpec(one.getKey(), boundOf(one.getFrom(), date), boundOf(one.getTo(), date)));
                conditions.add(rangeCondition(value, one.getFrom(), one.getTo()));
            }
            BucketSpec.Kind kind = date ? BucketSpec.Kind.DATE_RANGE : BucketSpec.Kind.RANGE;
            addKey(matchMask(conditions), BucketSpec.ranges(kind, range.getName(), rangeSpecs, range.format()), List.of());
        }

        private void addFilterLevel(AggregationBuilder builder) {
            List<RexNode> predicates = new ArrayList<>();
            List<String> keys = new ArrayList<>();
            String otherBucketKey = null;
            BucketSpec.Kind kind;
            if (builder instanceof FilterAggregationBuilder filter) {
                kind = BucketSpec.Kind.FILTER;
                predicates.add(FilterQueryToRex.predicate(filter.getFilter(), filter.getName(), schema, multiFields, relBuilder));
                keys.add(filter.getName());
            } else {
                FiltersAggregationBuilder filters = (FiltersAggregationBuilder) builder;
                kind = BucketSpec.Kind.FILTERS;
                if (filters.filters().isEmpty()) {
                    throw unsupported("filters aggregation [" + filters.getName() + "] without filters");
                }
                if (filters.filters().size() > MAX_MASK_CONDITIONS) {
                    throw unsupported("more than " + MAX_MASK_CONDITIONS + " filters on aggregation [" + filters.getName() + "]");
                }
                for (FiltersAggregator.KeyedFilter keyed : filters.filters()) {
                    predicates.add(FilterQueryToRex.predicate(keyed.filter(), filters.getName(), schema, multiFields, relBuilder));
                    keys.add(keyed.key());
                }
                otherBucketKey = filters.otherBucket() ? filters.otherBucketKey() : null;
            }
            addKey(matchMask(predicates), BucketSpec.filters(kind, builder.getName(), keys, otherBucketKey), predicates);
        }

        private void addMissingLevel(MissingAggregationBuilder missing, Column column) {
            RexNode condition = relBuilder.isNull(relBuilder.field(column.index()));
            addKey(matchMask(List.of(condition)), BucketSpec.missing(missing.getName()), List.of());
        }

        private void addCompositeSource(CompositeValuesSourceBuilder<?> source, Map<String, Object> after, int compositeSize) {
            if (source.script() != null) {
                throw unsupported("script on composite source [" + source.name() + "]");
            }
            if (source.userValuetypeHint() != null) {
                throw unsupported("value_type on composite source [" + source.name() + "]");
            }
            if (source.missingBucket()) {
                throw unsupported("missing_bucket on composite source [" + source.name() + "]");
            }
            if (source.field() == null || source.field().isEmpty()) {
                throw unsupported("composite source [" + source.name() + "] without a field");
            }
            Column column = resolveColumn(source.field(), schema, multiFields);
            String sourceOrder = source.order() == SortOrder.ASC ? "ASC" : "DESC";
            if (source instanceof TermsValuesSourceBuilder) {
                RexNode key = column.isUtf8() || column.isFloating() ? relBuilder.field(column.index()) : numericKey(column);
                addKey(key, BucketSpec.compositeTerms(source.name(), sourceOrder, after, compositeSize, source.format()), List.of());
                return;
            }
            if (!(source instanceof DateHistogramValuesSourceBuilder dateHistogram)) {
                throw unsupported("composite source type [" + source.getClass().getSimpleName() + "] on source [" + source.name() + "]");
            }
            if (!column.isDate()) {
                throw new UnsupportedOperationException(
                    "column [" + column.name() + "] behind composite source field [" + source.field() + "] is not a date"
                );
            }
            if (dateHistogram.offset() != 0L) {
                throw unsupported("offset on composite source [" + source.name() + "]");
            }
            if (dateHistogram.timeZone() != null) {
                throw unsupported("time_zone on composite source [" + source.name() + "]");
            }
            long intervalMillis = fixedLengthIntervalMillis(dateHistogram);
            if (intervalMillis <= 0L) {
                throw unsupported("interval on composite source [" + source.name() + "] is not a fixed length");
            }
            RexNode interval = relBuilder.literal(intervalMillis);
            RexNode quotient = relBuilder.call(SqlStdOperatorTable.DIVIDE, epochMillis(column), interval);
            // The bucket ordinal, as for the fixed date_histogram.
            RexNode key = relBuilder.call(SqlStdOperatorTable.FLOOR, quotient);
            addKey(
                key,
                BucketSpec.compositeDateHistogram(source.name(), intervalMillis, sourceOrder, after, compositeSize, source.format()),
                List.of()
            );
        }

        private void addKey(RexNode key, BucketSpec spec, List<RexNode> predicates) {
            keyExpressions.add(key);
            bucketSpecs.add(spec);
            filterPredicates.add(predicates);
        }

        private void addMetric(AggregationBuilder builder) {
            if (!builder.getPipelineAggregations().isEmpty()) {
                throw unsupported("pipeline aggregation");
            }
            if (!builder.getSubAggregations().isEmpty()) {
                throw unsupported("sub aggregation under metric [" + builder.getName() + "]");
            }
            ValuesSourceAggregationBuilder<?> source = (ValuesSourceAggregationBuilder<?>) builder;
            checkPlainFieldSource(source);
            Column column = resolveColumn(source.field(), schema, multiFields);
            MetricSpec.Kind kind = metricKindOf(builder);
            if (kind == MetricSpec.Kind.CARDINALITY) {
                for (MetricIntent other : metrics) {
                    if (other.kind() == MetricSpec.Kind.CARDINALITY) {
                        throw unsupported("second cardinality aggregation [" + builder.getName() + "]");
                    }
                }
            }
            boolean arithmetic = kind != MetricSpec.Kind.VALUE_COUNT && kind != MetricSpec.Kind.CARDINALITY;
            if (arithmetic && column.isUtf8()) {
                throw notNumeric(column, source.field());
            }
            metrics.add(new MetricIntent(builder, kind, column, metricSpecOf(builder, kind, source.format())));
        }

        /** The aggregate call of one resolved metric, over the input's argument column. */
        private AggregateCall call(MetricIntent metric, int argIndex, int groupCount, RelNode input) {
            String name = metric.builder().getName();
            RelDataTypeFactory typeFactory = relBuilder.getTypeFactory();
            boolean distinct = false;
            SqlAggFunction function;
            switch (metric.kind()) {
                case SUM -> function = SqlStdOperatorTable.SUM;
                case AVG -> function = SqlStdOperatorTable.AVG;
                case MIN -> function = SqlStdOperatorTable.MIN;
                case MAX -> function = SqlStdOperatorTable.MAX;
                case VALUE_COUNT -> function = SqlStdOperatorTable.COUNT;
                case CARDINALITY -> {
                    function = SqlStdOperatorTable.COUNT;
                    distinct = true;
                }
                case STATS -> function = LanceOperatorTable.stats(statsRowType(typeFactory, false));
                case EXTENDED_STATS -> function = LanceOperatorTable.extendedStats(statsRowType(typeFactory, true));
                case PERCENTILES -> function = LanceOperatorTable.percentiles(
                    doublesRowType(typeFactory, ((PercentilesAggregationBuilder) metric.builder()).percentiles())
                );
                case PERCENTILE_RANKS -> function = LanceOperatorTable.percentileRanks(
                    doublesRowType(typeFactory, ((PercentileRanksAggregationBuilder) metric.builder()).values())
                );
                default -> throw new IllegalStateException("no aggregate call for metric kind " + metric.kind());
            }
            List<Integer> argList = List.of(argIndex);
            return AggregateCall.create(
                function,
                distinct,
                false,
                false,
                argList,
                -1,
                null,
                RelCollations.EMPTY,
                groupCount,
                input,
                null,
                name
            );
        }

        /** The column as a numeric key: dates as epoch millis, booleans as 0 / 1, numbers as themselves. */
        private RexNode numericKey(Column column) {
            if (column.isDate()) {
                return epochMillis(column);
            }
            if (column.isBoolean()) {
                return relBuilder.cast(relBuilder.field(column.index()), SqlTypeName.BIGINT);
            }
            return relBuilder.field(column.index());
        }

        /**
         * Epoch milliseconds of a date or timestamp column as
         * {@code UNIX_MILLIS}; a {@code DATE} column goes through a cast
         * to timestamp because {@code UNIX_MILLIS} takes a timestamp.
         */
        private RexNode epochMillis(Column column) {
            RexNode reference = relBuilder.field(column.index());
            if (column.type() instanceof ArrowType.Date) {
                reference = relBuilder.cast(reference, SqlTypeName.TIMESTAMP);
            }
            return relBuilder.call(SqlLibraryOperators.UNIX_MILLIS, reference);
        }

        /**
         * The bit mask of the conditions that hold:
         * {@code CASE WHEN c0 THEN 1 ELSE 0 END + CASE WHEN c1 THEN 2
         * ELSE 0 END + ...}. A grouping on it yields one row per
         * combination of matching conditions, so a row in two
         * overlapping ranges or filters counts in both. A null condition
         * contributes 0, as {@code CASE} treats null as not matched.
         */
        private RexNode matchMask(List<RexNode> conditions) {
            RexNode mask = null;
            for (int i = 0; i < conditions.size(); i++) {
                RexNode bit = relBuilder.call(
                    SqlStdOperatorTable.CASE,
                    conditions.get(i),
                    relBuilder.literal(1L << i),
                    relBuilder.literal(0L)
                );
                mask = mask == null ? bit : relBuilder.call(SqlStdOperatorTable.PLUS, mask, bit);
            }
            return mask;
        }

        /** {@code value >= from AND value < to} on doubles, an infinite bound left out; both open means the value exists. */
        private RexNode rangeCondition(RexNode value, double from, double to) {
            RexNode lower = Double.isInfinite(from)
                ? null
                : relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, value, relBuilder.literal(from));
            RexNode upper = Double.isInfinite(to) ? null : relBuilder.call(SqlStdOperatorTable.LESS_THAN, value, relBuilder.literal(to));
            if (lower == null && upper == null) {
                return relBuilder.isNotNull(value);
            }
            if (lower == null) {
                return upper;
            }
            return upper == null ? lower : relBuilder.call(SqlStdOperatorTable.AND, lower, upper);
        }

        /**
         * The request's ranges with their bounds resolved and sorted the
         * way the range aggregator factory prepares them: a string bound
         * is parsed by the field's format ({@code now} against the
         * system clock), a numeric bound of a {@code date_range} is
         * parsed as text as well so numeric formats apply, and the
         * ranges are ordered by {@code from} then {@code to}.
         */
        private RangeAggregator.Range[] resolveRanges(AbstractRangeBuilder<?, ?> builder, Column column, boolean date) {
            DocValueFormat format = date
                ? dateDocValueFormat(builder.format(), builder.timeZone())
                : numericDocValueFormat(builder.format());
            List<? extends RangeAggregator.Range> requested = builder.ranges();
            RangeAggregator.Range[] ranges = new RangeAggregator.Range[requested.size()];
            try {
                for (int i = 0; i < ranges.length; i++) {
                    RangeAggregator.Range range = requested.get(i);
                    double from = range.getFrom();
                    double to = range.getTo();
                    if (range.getFromAsString() != null) {
                        from = format.parseDouble(range.getFromAsString(), false, System::currentTimeMillis);
                    } else if (date && Double.isFinite(from)) {
                        from = format.parseDouble(Long.toString((long) from), false, System::currentTimeMillis);
                    }
                    if (range.getToAsString() != null) {
                        to = format.parseDouble(range.getToAsString(), false, System::currentTimeMillis);
                    } else if (date && Double.isFinite(to)) {
                        to = format.parseDouble(Long.toString((long) to), false, System::currentTimeMillis);
                    }
                    ranges[i] = new RangeAggregator.Range(range.getKey(), from, range.getFromAsString(), to, range.getToAsString());
                }
            } catch (RuntimeException unparseable) {
                throw unsupported("range bound on aggregation [" + builder.getName() + "] does not parse: " + unparseable.getMessage());
            }
            Arrays.sort(
                ranges,
                Comparator.comparingDouble(RangeAggregator.Range::getFrom).thenComparingDouble(RangeAggregator.Range::getTo)
            );
            return ranges;
        }

        private void checkPlainFieldSource(ValuesSourceAggregationBuilder<?> builder) {
            if (builder.script() != null) {
                throw unsupported("script on aggregation [" + builder.getName() + "]");
            }
            if (builder.missing() != null) {
                throw unsupported("missing on aggregation [" + builder.getName() + "]");
            }
            if (builder.userValueTypeHint() != null) {
                throw unsupported("value_type on aggregation [" + builder.getName() + "]");
            }
            if (builder.field() == null || builder.field().isEmpty()) {
                throw unsupported("aggregation [" + builder.getName() + "] without a field");
            }
        }
    }

    /** One bound of a resolved range as the spec carries it: a Double, the date millis as a Long, or null when open. */
    private static Object boundOf(double bound, boolean date) {
        if (Double.isInfinite(bound)) {
            return null;
        }
        return date ? (Object) (long) bound : (Object) bound;
    }

    private static List<MetricSpec> specsOf(List<MetricIntent> metrics) {
        List<MetricSpec> specs = new ArrayList<>(metrics.size());
        for (MetricIntent metric : metrics) {
            specs.add(metric.spec());
        }
        return specs;
    }

    /** Whether the builder is one of the ten metric aggregation types, whatever its options. */
    private static boolean isMetricShaped(AggregationBuilder builder) {
        return builder instanceof SumAggregationBuilder
            || builder instanceof AvgAggregationBuilder
            || builder instanceof MinAggregationBuilder
            || builder instanceof MaxAggregationBuilder
            || builder instanceof ValueCountAggregationBuilder
            || builder instanceof StatsAggregationBuilder
            || builder instanceof ExtendedStatsAggregationBuilder
            || builder instanceof CardinalityAggregationBuilder
            || builder instanceof PercentilesAggregationBuilder
            || builder instanceof PercentileRanksAggregationBuilder;
    }

    private static MetricSpec.Kind metricKindOf(AggregationBuilder builder) {
        if (builder instanceof SumAggregationBuilder) {
            return MetricSpec.Kind.SUM;
        }
        if (builder instanceof AvgAggregationBuilder) {
            return MetricSpec.Kind.AVG;
        }
        if (builder instanceof MinAggregationBuilder) {
            return MetricSpec.Kind.MIN;
        }
        if (builder instanceof MaxAggregationBuilder) {
            return MetricSpec.Kind.MAX;
        }
        if (builder instanceof ValueCountAggregationBuilder) {
            return MetricSpec.Kind.VALUE_COUNT;
        }
        if (builder instanceof StatsAggregationBuilder) {
            return MetricSpec.Kind.STATS;
        }
        if (builder instanceof ExtendedStatsAggregationBuilder) {
            return MetricSpec.Kind.EXTENDED_STATS;
        }
        if (builder instanceof CardinalityAggregationBuilder) {
            return MetricSpec.Kind.CARDINALITY;
        }
        if (builder instanceof PercentilesAggregationBuilder percentiles) {
            if (!isTDigest(percentiles.percentilesConfig())) {
                throw unsupported("hdr percentiles on aggregation [" + builder.getName() + "]");
            }
            return MetricSpec.Kind.PERCENTILES;
        }
        if (builder instanceof PercentileRanksAggregationBuilder ranks) {
            if (!isTDigest(ranks.percentilesConfig())) {
                throw unsupported("hdr percentile_ranks on aggregation [" + builder.getName() + "]");
            }
            return MetricSpec.Kind.PERCENTILE_RANKS;
        }
        throw unsupported("aggregation type [" + builder.getType() + "]");
    }

    private static MetricSpec metricSpecOf(AggregationBuilder builder, MetricSpec.Kind kind, String format) {
        return switch (kind) {
            case PERCENTILES -> {
                PercentilesAggregationBuilder percentiles = (PercentilesAggregationBuilder) builder;
                yield new MetricSpec(builder.getName(), kind, percentiles.percentiles(), null, null, null, percentiles.keyed(), format);
            }
            case PERCENTILE_RANKS -> {
                PercentileRanksAggregationBuilder ranks = (PercentileRanksAggregationBuilder) builder;
                yield new MetricSpec(builder.getName(), kind, null, ranks.values(), null, null, ranks.keyed(), format);
            }
            case EXTENDED_STATS -> new MetricSpec(
                builder.getName(),
                kind,
                null,
                null,
                ((ExtendedStatsAggregationBuilder) builder).sigma(),
                null,
                null,
                format
            );
            case CARDINALITY -> new MetricSpec(
                builder.getName(),
                kind,
                null,
                null,
                null,
                precisionThreshold((CardinalityAggregationBuilder) builder),
                null,
                format
            );
            default -> MetricSpec.of(kind, builder.getName(), format);
        };
    }

    /** The default percentiles method is tdigest, so a request without a {@code tdigest} / {@code hdr} block qualifies. */
    private static boolean isTDigest(PercentilesConfig config) {
        return config == null || config instanceof PercentilesConfig.TDigest;
    }

    /** The planning time type of a {@code stats} / {@code extended_stats} call: a row of doubles named by statistic. */
    private static RelDataType statsRowType(RelDataTypeFactory factory, boolean extended) {
        RelDataTypeFactory.Builder builder = factory.builder();
        builder.add("count", factory.createSqlType(SqlTypeName.DOUBLE));
        builder.add("min", factory.createSqlType(SqlTypeName.DOUBLE));
        builder.add("max", factory.createSqlType(SqlTypeName.DOUBLE));
        builder.add("sum", factory.createSqlType(SqlTypeName.DOUBLE));
        builder.add("avg", factory.createSqlType(SqlTypeName.DOUBLE));
        if (extended) {
            builder.add("sum_of_squares", factory.createSqlType(SqlTypeName.DOUBLE));
            builder.add("variance", factory.createSqlType(SqlTypeName.DOUBLE));
            builder.add("std_deviation", factory.createSqlType(SqlTypeName.DOUBLE));
        }
        return builder.build();
    }

    /** The planning time type of a percentiles call: a row with one double per requested percent or value. */
    private static RelDataType doublesRowType(RelDataTypeFactory factory, double[] keys) {
        RelDataTypeFactory.Builder builder = factory.builder();
        for (double key : keys) {
            builder.add(String.valueOf(key), factory.createSqlType(SqlTypeName.DOUBLE));
        }
        return builder.build();
    }

    /**
     * Maps an aggregation field to a Lance column: the field names a
     * top level column, or a keyword sub-field declared in the attach's
     * multi fields resolves to its base column. The column has to be a
     * scalar the pushdown reads (utf8, signed integer, single or double
     * float, boolean, date, timestamp).
     */
    static Column resolveColumn(String field, Schema schema, Map<String, LinkedHashMap<String, String>> multiFields) {
        String columnName = field;
        if (indexOf(schema, columnName) < 0) {
            int dot = field.lastIndexOf('.');
            if (dot > 0 && multiFields != null) {
                String base = field.substring(0, dot);
                String sub = field.substring(dot + 1);
                LinkedHashMap<String, String> subs = multiFields.get(base);
                if (subs != null && "keyword".equals(subs.get(sub))) {
                    columnName = base;
                }
            }
        }
        int index = indexOf(schema, columnName);
        if (index < 0) {
            throw unsupported("field [" + field + "] does not map to a Lance column");
        }
        ArrowType type = schema.getFields().get(index).getType();
        if (!isSupportedScalar(type)) {
            throw unsupported("column [" + columnName + "] behind field [" + field + "] is not a supported scalar column");
        }
        return new Column(columnName, index, type);
    }

    private static boolean isSupportedScalar(ArrowType type) {
        if (type instanceof ArrowType.Utf8
            || type instanceof ArrowType.Bool
            || type instanceof ArrowType.Date
            || type instanceof ArrowType.Timestamp) {
            return true;
        }
        if (type instanceof ArrowType.Int intType) {
            return intType.getIsSigned();
        }
        return type instanceof ArrowType.FloatingPoint fp
            && (fp.getPrecision() == FloatingPointPrecision.SINGLE || fp.getPrecision() == FloatingPointPrecision.DOUBLE);
    }

    private static int indexOf(Schema schema, String column) {
        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(column)) {
                return i;
            }
        }
        return -1;
    }

    private static UnsupportedOperationException notNumeric(Column column, String field) {
        return new UnsupportedOperationException("column [" + column.name() + "] behind aggregation field [" + field + "] is not numeric");
    }

    /**
     * The date format of a date valued option: the request's pattern
     * when it names one, else the mapping's default date format, at
     * millisecond resolution as the derived date mapping stores epoch
     * millis.
     */
    static DocValueFormat dateDocValueFormat(String pattern, ZoneId zone) {
        DateFormatter formatter = pattern == null ? DateFieldMapper.getDefaultDateTimeFormatter() : DateFormatter.forPattern(pattern);
        return new DocValueFormat.DateTime(formatter, zone == null ? ZoneOffset.UTC : zone, DateFieldMapper.Resolution.MILLISECONDS);
    }

    private static DocValueFormat numericDocValueFormat(String pattern) {
        return pattern == null ? DocValueFormat.RAW : new DocValueFormat.Decimal(pattern);
    }

    /**
     * The {@code terms} order as an {@link BucketSpec.OrderSpec}:
     * {@code _count} descending, {@code _key} in either direction, or
     * one sub aggregation's value. {@code _count} ascending and compound
     * orders beyond the builder's default {@code _key} tie breaker
     * throw; the tree wide check of a sub aggregation order runs after
     * the levels resolve.
     */
    private static BucketSpec.OrderSpec orderOf(TermsAggregationBuilder terms) {
        BucketOrder order = terms.order();
        if (InternalOrder.isCountDesc(order)) {
            return BucketSpec.OrderSpec.of(BucketSpec.OrderSpec.Kind.COUNT_DESC);
        }
        if (InternalOrder.isKeyOrder(order)) {
            return BucketSpec.OrderSpec.of(
                InternalOrder.isKeyAsc(order) ? BucketSpec.OrderSpec.Kind.KEY_ASC : BucketSpec.OrderSpec.Kind.KEY_DESC
            );
        }
        AggregationOrder aggregationOrder = aggregationOrder(order);
        if (aggregationOrder == null) {
            throw unsupported("terms order [" + order + "] on aggregation [" + terms.getName() + "]");
        }
        return BucketSpec.OrderSpec.subAggregation(aggregationOrder.path(), aggregationOrder.ascending());
    }

    /** A terms order on one sub aggregation: its path and direction. */
    private record AggregationOrder(String path, boolean ascending) {
    }

    /**
     * The sub aggregation order of a {@code terms}, or null when the
     * order is not one: the order itself, or the compound of the order
     * and the {@code _key} ascending tie breaker the builder wraps
     * every non key order in (any other compound answers null).
     * {@code InternalOrder.Aggregation} exposes its path but not its
     * direction, and a compound does not expose its elements, so both
     * are read back from the order's wire form (compound is -1 followed
     * by the element count, an aggregation order is 0 followed by the
     * direction and the path, {@code _key} ascending is 4).
     */
    private static AggregationOrder aggregationOrder(BucketOrder order) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            order.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                byte id = in.readByte();
                if (id == -1) {
                    if (in.readVInt() != 2) {
                        return null;
                    }
                    id = in.readByte();
                    if (id != 0) {
                        return null;
                    }
                    boolean ascending = in.readBoolean();
                    String path = in.readString();
                    return in.readByte() == 4 ? new AggregationOrder(path, ascending) : null;
                }
                if (id == 0) {
                    boolean ascending = in.readBoolean();
                    return new AggregationOrder(in.readString(), ascending);
                }
                return null;
            }
        } catch (IOException impossible) {
            return null;
        }
    }

    /**
     * The {@code terms} shard size as the aggregator factory derives
     * it: -1 is the builder's "not set", for which the factory applies
     * the distributed counting heuristic on non key orders.
     */
    private static int shardSizeOf(TermsAggregationBuilder terms) {
        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(
            terms.minDocCount(),
            terms.shardMinDocCount(),
            terms.size(),
            terms.shardSize()
        );
        if (!InternalOrder.isKeyOrder(terms.order()) && thresholds.getShardSize() == -1) {
            thresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(thresholds.getRequiredSize()));
        }
        thresholds.ensureValidity();
        return thresholds.getShardSize();
    }

    /**
     * The DataFusion {@code date_trunc} unit of a {@code date_histogram}
     * {@code calendar_interval}, or {@code null} when the builder has no
     * calendar interval. Every calendar unit OpenSearch accepts
     * ({@code second} through {@code year}) has a {@code date_trunc}
     * counterpart with the same rounding for UTC.
     */
    private static String calendarUnitOf(DateHistogramAggregationBuilder dateHistogram) {
        DateHistogramInterval interval = dateHistogram.getCalendarInterval();
        if (interval == null) {
            return null;
        }
        Rounding.DateTimeUnit unit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString());
        if (unit == null) {
            return null;
        }
        return switch (unit) {
            case SECOND_OF_MINUTE -> "second";
            case MINUTES_OF_HOUR -> "minute";
            case HOUR_OF_DAY -> "hour";
            case DAY_OF_MONTH -> "day";
            case WEEK_OF_WEEKYEAR -> "week";
            case MONTH_OF_YEAR -> "month";
            case QUARTER_OF_YEAR -> "quarter";
            case YEAR_OF_CENTURY -> "year";
        };
    }

    /**
     * Milliseconds of a composite date source's interval when it rounds
     * to a fixed number of milliseconds: a {@code fixed_interval}
     * always, a {@code calendar_interval} when its unit is a day or
     * shorter (in UTC with no offset such a unit rounds exactly like
     * the fixed interval of the same length); 0 otherwise.
     */
    private static long fixedLengthIntervalMillis(DateHistogramValuesSourceBuilder dateHistogram) {
        DateHistogramInterval fixed;
        try {
            fixed = dateHistogram.getIntervalAsFixed();
        } catch (IllegalStateException | IllegalArgumentException notFixed) {
            return 0L;
        }
        return fixed == null ? 0L : fixedIntervalMillisOrZero(fixed.toString());
    }

    /** Milliseconds of a {@code fixed_interval}, parsed the way the builders parse it; 0 when the text is not a time value. */
    private static long fixedIntervalMillisOrZero(String fixedInterval) {
        try {
            return TimeValue.parseTimeValue(fixedInterval, null, "fixed_interval").getMillis();
        } catch (IllegalArgumentException unparseable) {
            return 0L;
        }
    }

    /**
     * The request's {@code after} map, or null. The builder has a
     * setter but no getter for it, so it is read back from the
     * builder's own JSON rendering, which writes {@code after} under
     * the {@code composite} key only when the request carried one.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> afterKey(CompositeAggregationBuilder composite) {
        String json = Strings.toString(XContentType.JSON, composite);
        Map<String, Object> rendered = XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
        Map<String, Object> body = (Map<String, Object>) rendered.get(composite.getName());
        Map<String, Object> definition = (Map<String, Object>) body.get(CompositeAggregationBuilder.NAME);
        return (Map<String, Object>) definition.get(CompositeAggregationBuilder.AFTER_FIELD_NAME.getPreferredName());
    }

    /**
     * The {@code precision_threshold} of a cardinality builder, null
     * when the request named none. The builder has a setter but no
     * getter, so it is read back from the builder's own JSON rendering,
     * which writes the key only when the option was set.
     */
    @SuppressWarnings("unchecked")
    private static Long precisionThreshold(CardinalityAggregationBuilder cardinality) {
        String json = Strings.toString(XContentType.JSON, cardinality);
        Map<String, Object> rendered = XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
        Map<String, Object> body = (Map<String, Object>) rendered.get(cardinality.getName());
        Map<String, Object> definition = (Map<String, Object>) body.get(CardinalityAggregationBuilder.NAME);
        Object threshold = definition.get(CardinalityAggregationBuilder.PRECISION_THRESHOLD_FIELD.getPreferredName());
        return threshold instanceof Number number ? number.longValue() : null;
    }

    /**
     * {@link HistogramAggregationBuilder} exposes no getter for
     * {@code hard_bounds}, so the check goes through the builder's own
     * JSON rendering, which writes the {@code hard_bounds} key only when
     * the option was set.
     */
    private static boolean mentionsHardBounds(HistogramAggregationBuilder histogram) {
        return Strings.toString(XContentType.JSON, histogram).contains("\"" + Histogram.HARD_BOUNDS_FIELD.getPreferredName() + "\"");
    }

    static UnsupportedOperationException unsupported(String element) {
        return new UnsupportedOperationException(element);
    }
}

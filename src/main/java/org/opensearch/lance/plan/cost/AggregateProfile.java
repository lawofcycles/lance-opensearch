/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.ImmutableBitSet;
import org.lance.index.IndexType;
import org.opensearch.lance.plan.calcite.LanceTable;
import org.opensearch.lance.plan.metadata.ColumnStatistics;
import org.opensearch.lance.plan.metadata.TableStatistics;
import org.opensearch.lance.plan.rel.BucketSpec;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceTableScan;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The quantities of one aggregation shape the cost model multiplies its
 * coefficients with, derived from the {@link LanceAggregate} and the
 * table it runs over. The same record describes the pushed form and the
 * Lucene aggregator form of one tree, because both read the same rows
 * and produce the same groups; the two formulas in {@link CostModel}
 * differ in which of these quantities they charge for.
 *
 * <p>The group key kinds are counted in cost classes rather than
 * listed: a string terms key hashes strings, a numeric terms key hashes
 * numbers, a date histogram key truncates timestamps, and so on. The
 * measurements tell these apart and nothing finer.
 *
 * @param tableRows live rows of the table
 * @param groups groups the aggregate produces: the product of the key
 *     domains capped at the row count; a terms key's domain is the
 *     tightest bound its column's indexes give (a bitmap's distinct
 *     count, the integer range of a BTree over an integer column)
 * @param mergedGroups group rows one node's parallel scans hand to the
 *     merge: {@code groups}, or the terms top-k retention when a
 *     single level terms ordered by count or by a metric cuts them
 * @param groupsKnown whether every key domain behind {@code groups}
 *     came from the statistics or from the request itself (a bitmap
 *     distinct count, a BTree integer range, a range or filter count, a
 *     date interval) rather than from Calcite's default share of the
 *     rows for a key without a bound; a guessed domain grows with the
 *     table, so a bound on the groups is only judged when this is true
 * @param columnsRead distinct table columns the keys, metrics and
 *     filter reference
 * @param bytesPerRow estimated stored bytes per row across those columns
 * @param scanPasses full scans the pushed form runs: 2 for percentiles
 *     (bounds, then bins), 1 otherwise
 * @param stringKeys terms or composite terms keys over string columns
 * @param numericKeys terms or composite terms keys over other columns
 * @param dateKeys date histogram and numeric histogram keys
 * @param rangeKeys range and date range keys
 * @param filterKeys filter, filters and missing keys
 * @param compositeDateKeys composite date histogram sources
 * @param composite whether the keys are composite sources
 * @param simpleMetrics sum, avg, min, max, value_count and stats calls
 * @param extendedStats whether an extended_stats call is present
 * @param percentiles whether a percentiles or percentile_ranks call is present
 * @param cardinality whether a cardinality call is present
 * @param cardinalityDistinct distinct values the cardinality column is
 *     estimated to hold; the row count when unknown
 * @param filterSelectivity share of the rows the query filter keeps; 1
 *     without a filter
 */
public record AggregateProfile(double tableRows, double groups, double mergedGroups, boolean groupsKnown, int columnsRead,
    double bytesPerRow, int scanPasses, int stringKeys, int numericKeys, int dateKeys, int rangeKeys, int filterKeys, int compositeDateKeys,
    boolean composite, int simpleMetrics, boolean extendedStats, boolean percentiles, boolean cardinality, double cardinalityDistinct,
    double filterSelectivity) {

    private static final double MILLIS_PER_YEAR = 365.25 * 24 * 60 * 60 * 1000;

    /** Whether the query filter keeps only part of the rows. */
    public boolean filtered() {
        return filterSelectivity < 1.0;
    }

    /** Whether the hash tables of both paths outgrow the caches. */
    public boolean largeGroups() {
        return groups > CostCoefficients.LARGE_GROUPS;
    }

    /** Whether any bucket key groups the rows: a metric under one is collected per bucket ordinal rather than in place. */
    public boolean bucketed() {
        return stringKeys + numericKeys + dateKeys + rangeKeys + filterKeys + compositeDateKeys > 0;
    }

    /** Simple metrics collected under a bucket key; zero for a metric only tree. */
    public int bucketedMetrics() {
        return bucketed() ? simpleMetrics : 0;
    }

    /**
     * Whether the Lucene cardinality aggregator hashes every row's
     * value: it does once the column's distinct values exceed
     * {@link CostCoefficients#LUCENE_CARDINALITY_ORDINALS_MAX_DISTINCT},
     * below that it collects ordinals into a bitset and hashes each
     * distinct term once.
     */
    public boolean cardinalityHashesEveryRow() {
        return cardinality && cardinalityDistinct > CostCoefficients.LUCENE_CARDINALITY_ORDINALS_MAX_DISTINCT;
    }

    /** Terms, range, filter and date keys that are not composite sources: one aggregator level each on the Lucene side. */
    public int nestedLevels() {
        if (composite) {
            return 0;
        }
        return Math.max(0, stringKeys + numericKeys + dateKeys + rangeKeys + filterKeys - 1);
    }

    /** Composite sources of every kind. */
    public int compositeSources() {
        return composite ? stringKeys + numericKeys + compositeDateKeys : 0;
    }

    /**
     * The profile of {@code aggregate} over {@code scan}'s table. The
     * aggregate must be the rebuilt tree the rules carry (its input a
     * concrete {@code Project} / {@code Filter} chain down to a scan),
     * because the key expressions and the filter predicate are read
     * structurally. {@code mq} answers the distinct counts and the
     * filter selectivity.
     */
    public static AggregateProfile of(LanceAggregate aggregate, LanceTableScan scan, RelMetadataQuery mq) {
        LanceTable table = scan.getTable().unwrap(LanceTable.class);
        Optional<TableStatistics> statistics = table == null ? Optional.empty() : table.tableStatistics();
        Schema arrowSchema = table == null ? null : table.arrowSchema();
        double rows = scan.getTable().getRowCount();
        // The table's row type, not the scan's: a scan carrying the pushed
        // aggregate reports the aggregate's row type.
        RelDataType scanRowType = scan.getTable().getRowType();

        Chain chain = Chain.under(aggregate);
        Filter filter = chain.filter();
        int keyCount = aggregate.getGroupSet().cardinality();
        RelDataType inputRowType = aggregate.getInput().getRowType();

        int stringKeys = 0;
        int numericKeys = 0;
        int dateKeys = 0;
        int rangeKeys = 0;
        int filterKeys = 0;
        int compositeDateKeys = 0;
        boolean composite = false;
        double groups = 1.0;
        boolean groupsKnown = true;
        for (int key = 0; key < keyCount; key++) {
            BucketSpec spec = aggregate.bucket(key);
            ImmutableBitSet keyColumns = scanColumns(chain.inputExpression(inputRowType, key));
            boolean string = inputRowType.getFieldList().get(key).getType().getFamily() == SqlTypeFamily.CHARACTER;
            switch (spec.kind()) {
                case TERMS, COMPOSITE_TERMS -> {
                    if (string) {
                        stringKeys++;
                    } else {
                        numericKeys++;
                    }
                    OptionalLong distinct = distinctOf(keyColumns, statistics, scanRowType);
                    groupsKnown &= distinct.isPresent();
                    groups *= termsDomain(distinct, rows);
                    composite |= spec.kind() == BucketSpec.Kind.COMPOSITE_TERMS;
                }
                case HISTOGRAM -> {
                    dateKeys++;
                    groupsKnown = false;
                    groups *= rows * CostCoefficients.UNKNOWN_KEY_DISTINCT_SHARE;
                }
                case DATE_HISTOGRAM_FIXED -> {
                    dateKeys++;
                    groups *= dateBuckets(spec.dateIntervalMillis());
                }
                case DATE_HISTOGRAM_CALENDAR -> {
                    dateKeys++;
                    groups *= calendarBuckets(spec.calendarUnit());
                }
                case RANGE, DATE_RANGE -> {
                    rangeKeys++;
                    groups *= spec.ranges() == null ? 1 : Math.max(1, spec.ranges().size());
                }
                case FILTER, MISSING -> filterKeys++;
                case FILTERS -> {
                    filterKeys++;
                    int buckets = spec.filterKeys() == null ? 1 : spec.filterKeys().size() + (spec.otherBucketKey() != null ? 1 : 0);
                    groups *= Math.max(1, buckets);
                }
                case COMPOSITE_DATE_HISTOGRAM -> {
                    compositeDateKeys++;
                    composite = true;
                    groups *= dateBuckets(spec.dateIntervalMillis());
                }
            }
        }
        groups = Math.max(1.0, Math.min(groups, rows));

        int simpleMetrics = 0;
        boolean extendedStats = false;
        boolean percentiles = false;
        boolean cardinality = false;
        double cardinalityDistinct = rows;
        List<AggregateCall> calls = aggregate.getAggCallList();
        for (int i = 0; i < calls.size(); i++) {
            ImmutableBitSet callColumns = ImmutableBitSet.of();
            for (int argument : calls.get(i).getArgList()) {
                callColumns = callColumns.union(scanColumns(chain.inputExpression(inputRowType, argument)));
            }
            switch (aggregate.metric(i).kind()) {
                case SUM, AVG, MIN, MAX, VALUE_COUNT, STATS -> simpleMetrics++;
                case EXTENDED_STATS -> extendedStats = true;
                case PERCENTILES, PERCENTILE_RANKS -> percentiles = true;
                case CARDINALITY -> {
                    cardinality = true;
                    OptionalLong distinct = distinctOf(callColumns, statistics, scanRowType);
                    cardinalityDistinct = Math.min(cardinalityDistinct, distinct.isPresent() ? distinct.getAsLong() : rows);
                }
            }
        }

        double selectivity = 1.0;
        if (filter != null) {
            Double estimated = mq.getSelectivity(filter.getInput(), filter.getCondition());
            selectivity = estimated != null ? estimated : RelMdUtil.guessSelectivity(filter.getCondition());
            selectivity = Math.max(0.0, Math.min(1.0, selectivity));
        }

        ImmutableBitSet read = columnsRead(aggregate, chain);
        double bytes = 0.0;
        for (int column : read) {
            if (column < scanRowType.getFieldCount()) {
                bytes += bytesPerRow(scanRowType.getFieldList().get(column).getName(), arrowSchema, statistics);
            }
        }

        double mergedGroups = groups;
        if (!composite && keyCount == 1 && aggregate.bucket(0).kind() == BucketSpec.Kind.TERMS) {
            BucketSpec terms = aggregate.bucket(0);
            boolean cut = terms.order() != null
                && terms.order().kind() != BucketSpec.OrderSpec.Kind.KEY_ASC
                && terms.order().kind() != BucketSpec.OrderSpec.Kind.KEY_DESC;
            if (cut && terms.shardSize() != null && terms.shardSize() > 0) {
                mergedGroups = Math.min(groups, (double) terms.shardSize() * CostCoefficients.TERMS_TOP_K_RETENTION_FACTOR);
            }
        }

        return new AggregateProfile(
            rows,
            groups,
            mergedGroups,
            groupsKnown,
            read.cardinality(),
            bytes,
            percentiles ? 2 : 1,
            stringKeys,
            numericKeys,
            dateKeys,
            rangeKeys,
            filterKeys,
            compositeDateKeys,
            composite,
            simpleMetrics,
            extendedStats,
            percentiles,
            cardinality,
            cardinalityDistinct,
            selectivity
        );
    }

    /**
     * The distinct table columns the Lucene aggregators read for
     * {@code aggregate} over {@code scan}'s table, by name in the
     * table's row type order: the columns behind the keys, the metric
     * arguments and the query filter, the same set {@link #of} counts as
     * {@link #columnsRead}. Column positions past the table's row type
     * (none today; a projection over a computed expression) are skipped.
     */
    public static List<String> columnNames(LanceAggregate aggregate, LanceTableScan scan) {
        RelDataType scanRowType = scan.getTable().getRowType();
        List<String> names = new ArrayList<>();
        for (int column : columnsRead(aggregate, Chain.under(aggregate))) {
            if (column < scanRowType.getFieldCount()) {
                names.add(scanRowType.getFieldList().get(column).getName());
            }
        }
        return names;
    }

    /**
     * The chain under an aggregate: an optional key projection over an
     * optional query filter over the scan. The projection sits directly
     * over the scan or over a filter over the scan, and a filter keeps
     * the scan's row type, so the projection's input positions are scan
     * positions.
     */
    private record Chain(Project project, Filter filter) {

        static Chain under(LanceAggregate aggregate) {
            Project project = null;
            Filter filter = null;
            RelNode node = aggregate.getInput();
            while (true) {
                if (node instanceof Project p && project == null) {
                    project = p;
                } else if (node instanceof Filter f && filter == null) {
                    filter = f;
                } else {
                    break;
                }
                node = node.getInput(0);
            }
            return new Chain(project, filter);
        }

        /**
         * The expression the aggregate's input field {@code index} stands
         * for: the projection's expression when there is one, else a
         * reference to the scan column at the same position.
         */
        RexNode inputExpression(RelDataType inputRowType, int index) {
            return project != null
                ? project.getProjects().get(index)
                : new RexInputRef(index, inputRowType.getFieldList().get(index).getType());
        }
    }

    /** The scan columns the keys, the metric arguments and the filter of {@code aggregate} reference. */
    private static ImmutableBitSet columnsRead(LanceAggregate aggregate, Chain chain) {
        RelDataType inputRowType = aggregate.getInput().getRowType();
        ImmutableBitSet.Builder columns = ImmutableBitSet.builder();
        for (int key = 0; key < aggregate.getGroupSet().cardinality(); key++) {
            columns.addAll(scanColumns(chain.inputExpression(inputRowType, key)));
        }
        for (AggregateCall call : aggregate.getAggCallList()) {
            for (int argument : call.getArgList()) {
                columns.addAll(scanColumns(chain.inputExpression(inputRowType, argument)));
            }
        }
        if (chain.filter() != null) {
            columns.addAll(scanColumns(chain.filter().getCondition()));
        }
        return columns.build();
    }

    /**
     * The scan columns an expression over the projection's input
     * references. The projection sits directly over the scan or over a
     * filter over the scan, and a filter keeps the scan's row type, so
     * input positions are scan positions.
     */
    private static ImmutableBitSet scanColumns(RexNode expression) {
        return RelOptUtil.InputFinder.bits(expression);
    }

    /**
     * Distinct values of a terms key: the tightest bound the statistics
     * give (a bitmap's distinct count, the integer range of a BTree over
     * an integer column) when there is one, else Calcite's default share
     * of the rows.
     */
    private static double termsDomain(OptionalLong distinct, double rows) {
        return distinct.isPresent() ? Math.min(distinct.getAsLong(), rows) : rows * CostCoefficients.UNKNOWN_KEY_DISTINCT_SHARE;
    }

    /**
     * The distinct value bound of a single column expression
     * ({@link ColumnStatistics#distinctUpperBound}), empty for several
     * columns or no bound.
     */
    private static OptionalLong distinctOf(ImmutableBitSet columns, Optional<TableStatistics> statistics, RelDataType scanRowType) {
        if (statistics.isEmpty() || columns.cardinality() != 1) {
            return OptionalLong.empty();
        }
        int column = columns.nth(0);
        if (column >= scanRowType.getFieldCount()) {
            return OptionalLong.empty();
        }
        return statistics.get()
            .column(scanRowType.getFieldList().get(column).getName())
            .map(ColumnStatistics::distinctUpperBound)
            .orElse(OptionalLong.empty());
    }

    private static double dateBuckets(Long intervalMillis) {
        if (intervalMillis == null || intervalMillis <= 0) {
            return 1.0;
        }
        return Math.max(1.0, CostCoefficients.DATE_SPAN_ASSUMED_YEARS * MILLIS_PER_YEAR / intervalMillis);
    }

    private static double calendarBuckets(String unit) {
        if (unit == null) {
            return 1.0;
        }
        double perYear = switch (unit.toLowerCase(Locale.ROOT)) {
            case "year", "1y" -> 1;
            case "quarter", "1q" -> 4;
            case "month", "1m" -> 12;
            case "week", "1w" -> 52.18;
            case "day", "1d" -> 365.25;
            case "hour", "1h" -> 365.25 * 24;
            case "minute" -> 365.25 * 24 * 60;
            case "second", "1s" -> 365.25 * 24 * 60 * 60;
            default -> 365.25;
        };
        return Math.max(1.0, CostCoefficients.DATE_SPAN_ASSUMED_YEARS * perYear);
    }

    /**
     * Estimated stored bytes per row of one column: the Arrow type's
     * width for fixed width types, a dictionary width for a string with
     * a bitmap index, a flat assumption for other strings, lists and
     * structs. Lance's file encodings compress further, so these are
     * upper-ish bounds that keep the columns in proportion to each
     * other, which is what the object store read term needs.
     */
    static double bytesPerRow(String column, Schema arrowSchema, Optional<TableStatistics> statistics) {
        Field field = arrowSchema == null ? null : arrowSchema.findField(column);
        if (field == null) {
            return CostCoefficients.OTHER_BYTES_PER_ROW;
        }
        return bytesPerRow(field, column, statistics);
    }

    private static double bytesPerRow(Field field, String column, Optional<TableStatistics> statistics) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Int integer) {
            return integer.getBitWidth() / 8.0;
        }
        if (type instanceof ArrowType.FloatingPoint floating) {
            return floating.getPrecision() == FloatingPointPrecision.DOUBLE ? 8
                : floating.getPrecision() == FloatingPointPrecision.SINGLE ? 4
                : 2;
        }
        if (type instanceof ArrowType.Bool) {
            return 1;
        }
        if (type instanceof ArrowType.Date date) {
            return date.getUnit() == DateUnit.DAY ? 4 : 8;
        }
        if (type instanceof ArrowType.Timestamp || type instanceof ArrowType.Time || type instanceof ArrowType.Duration) {
            return 8;
        }
        if (type instanceof ArrowType.Utf8
            || type instanceof ArrowType.LargeUtf8
            || type instanceof ArrowType.Binary
            || type instanceof ArrowType.LargeBinary) {
            boolean dictionary = statistics.isPresent()
                && statistics.get().column(column).map(stats -> stats.hasIndex(IndexType.BITMAP)).orElse(false);
            return dictionary ? CostCoefficients.DICTIONARY_STRING_BYTES_PER_ROW : CostCoefficients.STRING_BYTES_PER_ROW;
        }
        if (type instanceof ArrowType.FixedSizeList list && field.getChildren().size() == 1) {
            return list.getListSize() * bytesPerRow(field.getChildren().get(0), column, statistics);
        }
        return CostCoefficients.OTHER_BYTES_PER_ROW;
    }
}

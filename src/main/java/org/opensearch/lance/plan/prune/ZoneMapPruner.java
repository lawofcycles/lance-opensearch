/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.prune;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.lance.Dataset;
import org.lance.index.scalar.ZoneStats;
import org.opensearch.lance.plan.calcite.LanceTable;
import org.opensearch.lance.plan.metadata.ColumnStatistics;
import org.opensearch.lance.plan.metadata.TableStatistics;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.lance.plan.rel.LanceTableScan;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Decides, from the zone maps of a table, which fragments no row of the
 * query's scalar predicate can come from, so the fragment executors
 * leave them out of their scans. A zone map index records per zone (a
 * run of rows inside one fragment) the column's minimum, maximum and
 * null count; a fragment is excluded when every one of its zones is
 * proven empty of matching rows. The proof is conservative in every
 * direction: a fragment the zone map does not cover (appended after the
 * index was built), a zone whose bounds are unknown (a null minimum or
 * maximum next to non null rows, a floating point bound that is
 * {@code NaN}), a predicate shape the pruner does not read ({@code NOT},
 * {@code LIKE}, a function call) and a column without a zone map all
 * keep the fragment. Excluding a fragment never changes an answer: a
 * pushed page, a pushed aggregate, a full text or knn prefilter and the
 * Lucene collectors all evaluate the same predicate, and a fragment
 * without a matching row contributes nothing to any of them.
 *
 * <p>The predicate is read off the logical tree the translator built
 * ({@link #prune(RelNode, LanceTable)}): the {@code Filter} directly
 * over the {@link LanceTableScan}, or over the {@link LanceFtsMatch} /
 * {@link LanceKnnSearch} node above the scan whose input carries the
 * shape's scalar clauses. A {@code post_filter} sits higher in the
 * tree and applies to the hits alone, so it is never read. A
 * conjunction excludes a fragment when any branch does, a disjunction
 * only when every branch does, and a comparison, {@code IS NULL} or
 * {@code IS NOT NULL} is judged zone by zone.
 *
 * <p>Zone bounds compare in the column's own representation. Lance
 * reports an integer, date or timestamp bound as a {@code Long} in the
 * column's unit, a floating point bound as a {@code Double}, a string
 * bound as a {@code String} and a boolean bound as a {@code Boolean}.
 * The translator compares an integer column as {@code BIGINT}, a float
 * column as {@code DOUBLE} and a date or timestamp column through
 * {@code UNIX_MILLIS(col)} against an epoch millis literal; the pruner
 * strips those casts and, for a temporal column, widens the zone's
 * bounds to the enclosing milliseconds before comparing. Strings compare
 * by code point, the order Lance computed the bounds in (UTF-8 byte
 * order), not {@code String.compareTo}'s UTF-16 unit order.
 *
 * <p>The zone maps are read from the table lazily
 * ({@link ColumnStatistics#zoneMap(Dataset)}) and the planner runs after
 * the coordinator has closed the dataset, so a caller that wants pruning
 * reads the zone maps of the query's columns while the dataset is open
 * ({@link TableStatistics#readZoneMaps}); the pruner itself consults
 * only what has been read ({@link ColumnStatistics#zoneMapIfRead()}) and
 * performs no I/O.
 */
public final class ZoneMapPruner {

    private static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;

    private ZoneMapPruner() {}

    /**
     * The fragments the query predicate of {@code logical} excludes over
     * {@code table}, in ascending id order; empty when the tree carries
     * no scalar predicate, the table has no statistics, or no zone map
     * of a predicate column has been read
     * ({@link TableStatistics#readZoneMaps}).
     */
    public static SortedSet<Integer> prune(RelNode logical, LanceTable table) {
        Optional<TableStatistics> statistics = table.tableStatistics();
        if (statistics.isEmpty()) {
            return new TreeSet<>();
        }
        Filter filter = queryFilter(logical);
        if (filter == null) {
            return new TreeSet<>();
        }
        return prune(filter.getCondition(), filter.getInput().getRowType(), table.arrowSchema(), statistics.get());
    }

    /**
     * The {@code Filter} carrying the query predicate: the one directly
     * over the scan, or over the full text / knn node whose input is the
     * filter over the scan. Null when the tree carries none.
     */
    static Filter queryFilter(RelNode node) {
        while (node != null) {
            if (node instanceof Filter filter) {
                RelNode input = filter.getInput();
                return input instanceof LanceTableScan ? filter : null;
            }
            if (node instanceof LanceTableScan) {
                return null;
            }
            if (node instanceof LanceFtsMatch || node instanceof LanceKnnSearch) {
                node = node.getInput(0);
                continue;
            }
            if (node.getInputs().size() != 1) {
                return null;
            }
            node = node.getInput(0);
        }
        return null;
    }

    /**
     * The fragments {@code predicate} over {@code rowType} excludes, in
     * ascending id order. A fragment is considered only when the zone
     * map of some column the predicate names covers it; it is excluded
     * when the predicate cannot hold on any of its rows.
     *
     * @param predicate the query predicate over the scan row type
     * @param rowType the row type the predicate's input references index
     * @param arrowSchema the table's Arrow schema, for the column types
     * @param statistics the table statistics whose read zone maps are consulted
     */
    public static SortedSet<Integer> prune(RexNode predicate, RelDataType rowType, Schema arrowSchema, TableStatistics statistics) {
        Zones zones = new Zones(rowType, arrowSchema, statistics);
        SortedSet<Integer> excluded = new TreeSet<>();
        for (Integer fragmentId : zones.coveredFragments()) {
            if (!zones.mayMatch(predicate, fragmentId)) {
                excluded.add(fragmentId);
            }
        }
        return excluded;
    }

    /** One column's zone map grouped by fragment, with the column's Arrow type. */
    private record ColumnZones(ArrowType type, Map<Integer, List<ZoneStats>> byFragment) {
    }

    /**
     * The zone maps the predicate can consult, resolved lazily per
     * column reference and memoised for the evaluation of every fragment.
     */
    private static final class Zones {

        private final RelDataType rowType;
        private final Schema arrowSchema;
        private final TableStatistics statistics;
        /** Column path to its zones; a null value records a column without a usable zone map. */
        private final Map<String, ColumnZones> columns = new HashMap<>();

        Zones(RelDataType rowType, Schema arrowSchema, TableStatistics statistics) {
            this.rowType = rowType;
            this.arrowSchema = arrowSchema;
            this.statistics = statistics;
        }

        /** Every fragment some read zone map covers. */
        Set<Integer> coveredFragments() {
            SortedSet<Integer> fragments = new TreeSet<>();
            for (ColumnStatistics column : statistics.columns().values()) {
                Optional<List<ZoneStats>> zones = column.zoneMapIfRead();
                if (zones.isPresent()) {
                    for (ZoneStats zone : zones.get()) {
                        fragments.add(zone.getFragmentId());
                    }
                }
            }
            return fragments;
        }

        /** Whether some row of {@code fragmentId} may satisfy {@code node}; true whenever unsure. */
        boolean mayMatch(RexNode node, int fragmentId) {
            if (node instanceof RexLiteral literal) {
                return !Boolean.FALSE.equals(literal.getValueAs(Boolean.class));
            }
            if (!(node instanceof RexCall call)) {
                return true;
            }
            switch (call.getKind()) {
                case AND: {
                    for (RexNode operand : call.getOperands()) {
                        if (!mayMatch(operand, fragmentId)) {
                            return false;
                        }
                    }
                    return true;
                }
                case OR: {
                    for (RexNode operand : call.getOperands()) {
                        if (mayMatch(operand, fragmentId)) {
                            return true;
                        }
                    }
                    return false;
                }
                case IS_NULL:
                case IS_NOT_NULL: {
                    List<ZoneStats> zones = zonesOf(call.getOperands().get(0), fragmentId, false);
                    if (zones == null) {
                        return true;
                    }
                    for (ZoneStats zone : zones) {
                        boolean allNull = zone.getNullCount() >= zone.getZoneLength();
                        boolean noneNull = zone.getNullCount() <= 0L;
                        if (call.getKind() == SqlKind.IS_NULL ? !noneNull : !allNull) {
                            return true;
                        }
                    }
                    return false;
                }
                case EQUALS:
                case NOT_EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    return comparisonMayMatch(call, fragmentId);
                default:
                    return true;
            }
        }

        /**
         * A comparison of a column against a literal: false only when
         * every zone of the fragment is proven empty of a satisfying
         * value. The literal may stand on either side.
         */
        private boolean comparisonMayMatch(RexCall call, int fragmentId) {
            RexNode left = call.getOperands().get(0);
            RexNode right = call.getOperands().get(1);
            SqlKind kind = call.getKind();
            if (left instanceof RexLiteral && !(right instanceof RexLiteral)) {
                RexNode swap = left;
                left = right;
                right = swap;
                kind = kind.reverse();
            }
            if (!(right instanceof RexLiteral literal) || literal.isNull()) {
                return true;
            }
            boolean millis = isUnixMillis(left);
            RexNode column = millis ? ((RexCall) stripCasts(left)).getOperands().get(0) : left;
            List<ZoneStats> zones = zonesOf(column, fragmentId, millis);
            if (zones == null) {
                return true;
            }
            ArrowType type = columnType(column);
            for (ZoneStats zone : zones) {
                if (zone.getNullCount() >= zone.getZoneLength()) {
                    // Every row is null; a comparison holds on none of them.
                    continue;
                }
                Bounds bounds = boundsOf(zone, type, millis, literal);
                if (bounds == null || bounds.mayContain(kind)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * The zones of the column {@code reference} names inside
         * {@code fragmentId}, or null when the reference is not a
         * column, the column has no read zone map, the zone map does
         * not cover the fragment, or {@code millis} disagrees with the
         * column's type (an epoch millis comparison over a column that
         * is not a date or timestamp, or a plain comparison over one).
         */
        private List<ZoneStats> zonesOf(RexNode reference, int fragmentId, boolean millis) {
            String path = columnPath(stripCasts(reference));
            if (path == null) {
                return null;
            }
            ColumnZones column = columns.computeIfAbsent(path, this::resolve);
            if (column == null) {
                return null;
            }
            boolean temporal = column.type() instanceof ArrowType.Date || column.type() instanceof ArrowType.Timestamp;
            if (temporal != millis) {
                return null;
            }
            return column.byFragment().get(fragmentId);
        }

        private ArrowType columnType(RexNode reference) {
            ColumnZones column = columns.get(columnPath(stripCasts(reference)));
            return column == null ? null : column.type();
        }

        /** The zone map of {@code path} grouped by fragment, or null when there is none to read. */
        private ColumnZones resolve(String path) {
            ArrowType type = arrowType(path);
            if (type == null) {
                return null;
            }
            Optional<ColumnStatistics> column = statistics.column(path);
            if (column.isEmpty()) {
                return null;
            }
            Optional<List<ZoneStats>> zoneMap = column.get().zoneMapIfRead();
            if (zoneMap.isEmpty() || zoneMap.get().isEmpty()) {
                return null;
            }
            Map<Integer, List<ZoneStats>> byFragment = new LinkedHashMap<>();
            for (ZoneStats zone : zoneMap.get()) {
                byFragment.computeIfAbsent(zone.getFragmentId(), id -> new ArrayList<>()).add(zone);
            }
            return new ColumnZones(type, byFragment);
        }

        /** The dotted column path a reference names, or null for anything but a column or a struct child chain. */
        private String columnPath(RexNode reference) {
            if (reference instanceof RexInputRef ref) {
                List<String> names = rowType.getFieldNames();
                return ref.getIndex() < names.size() ? names.get(ref.getIndex()) : null;
            }
            if (reference instanceof RexFieldAccess access) {
                String parent = columnPath(stripCasts(access.getReferenceExpr()));
                return parent == null ? null : parent + "." + access.getField().getName();
            }
            return null;
        }

        /** The Arrow type behind a dotted path through the table's struct columns, or null when the path names nothing. */
        private ArrowType arrowType(String path) {
            String[] segments = path.split("\\.");
            Field current = null;
            for (Field field : arrowSchema.getFields()) {
                if (field.getName().equals(segments[0])) {
                    current = field;
                    break;
                }
            }
            for (int i = 1; current != null && i < segments.length; i++) {
                Field child = null;
                if (current.getType() instanceof ArrowType.Struct) {
                    for (Field candidate : current.getChildren()) {
                        if (candidate.getName().equals(segments[i])) {
                            child = candidate;
                            break;
                        }
                    }
                }
                current = child;
            }
            return current == null ? null : current.getType();
        }
    }

    /**
     * A zone's bounds and the literal, all three as comparables of one
     * kind, so a comparison operator can be judged against the interval.
     */
    private record Bounds(Comparable<?> min, Comparable<?> max, Comparable<?> value) {

        /** Whether some value in {@code [min, max]} satisfies {@code x <kind> value}. */
        boolean mayContain(SqlKind kind) {
            int minToValue = compare(min, value);
            int maxToValue = compare(max, value);
            return switch (kind) {
                case EQUALS -> minToValue <= 0 && maxToValue >= 0;
                case NOT_EQUALS -> !(minToValue == 0 && maxToValue == 0);
                case LESS_THAN -> minToValue < 0;
                case LESS_THAN_OR_EQUAL -> minToValue <= 0;
                case GREATER_THAN -> maxToValue > 0;
                case GREATER_THAN_OR_EQUAL -> maxToValue >= 0;
                default -> true;
            };
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static int compare(Comparable a, Comparable b) {
            return a.compareTo(b);
        }
    }

    /**
     * The zone's bounds and the literal in one comparable form for the
     * column's type, or null when the zone's bounds are unknown or the
     * literal is not of the type the column compares in. Every
     * conversion widens rather than narrows: a temporal zone's bounds
     * become the milliseconds enclosing them, a floating point bound
     * that is {@code NaN} makes the zone unknown.
     */
    private static Bounds boundsOf(ZoneStats zone, ArrowType type, boolean millis, RexLiteral literal) {
        Object min = zone.getMin();
        Object max = zone.getMax();
        if (min == null || max == null || type == null) {
            return null;
        }
        SqlTypeFamily family = literal.getType().getSqlTypeName().getFamily();
        if (millis) {
            if (!(min instanceof Long lower) || !(max instanceof Long upper) || family != SqlTypeFamily.NUMERIC) {
                return null;
            }
            BigDecimal value = literal.getValueAs(BigDecimal.class);
            long[] range = millisRange(type, lower, upper);
            if (value == null || range == null) {
                return null;
            }
            return new Bounds(BigDecimal.valueOf(range[0]), BigDecimal.valueOf(range[1]), value);
        }
        if (type instanceof ArrowType.Int) {
            if (!(min instanceof Long lower) || !(max instanceof Long upper) || family != SqlTypeFamily.NUMERIC) {
                return null;
            }
            BigDecimal value = literal.getValueAs(BigDecimal.class);
            return value == null ? null : new Bounds(BigDecimal.valueOf(lower), BigDecimal.valueOf(upper), value);
        }
        if (type instanceof ArrowType.FloatingPoint) {
            if (!(min instanceof Double lower) || !(max instanceof Double upper) || family != SqlTypeFamily.NUMERIC) {
                return null;
            }
            if (lower.isNaN() || upper.isNaN() || lower.isInfinite() || upper.isInfinite()) {
                return null;
            }
            BigDecimal value = literal.getValueAs(BigDecimal.class);
            return value == null ? null : new Bounds(new BigDecimal(lower), new BigDecimal(upper), value);
        }
        if (type instanceof ArrowType.Utf8) {
            if (!(min instanceof String lower) || !(max instanceof String upper) || family != SqlTypeFamily.CHARACTER) {
                return null;
            }
            String value = literal.getValueAs(String.class);
            return value == null ? null : new Bounds(new CodePointString(lower), new CodePointString(upper), new CodePointString(value));
        }
        if (type instanceof ArrowType.Bool) {
            if (!(min instanceof Boolean lower) || !(max instanceof Boolean upper) || family != SqlTypeFamily.BOOLEAN) {
                return null;
            }
            Boolean value = literal.getValueAs(Boolean.class);
            return value == null ? null : new Bounds(lower, upper, value);
        }
        return null;
    }

    /**
     * The epoch milliseconds enclosing a temporal zone's bounds: a day
     * count becomes the day's first millisecond and the next day's
     * first millisecond less one, a second count the second's first and
     * last millisecond, a microsecond or nanosecond count the
     * milliseconds it truncates to in either direction. Null for a
     * temporal type the plugin does not map, or for a bound whose
     * conversion overflows.
     */
    static long[] millisRange(ArrowType type, long lower, long upper) {
        try {
            if (type instanceof ArrowType.Date date) {
                if (date.getUnit() == DateUnit.DAY) {
                    return new long[] {
                        Math.multiplyExact(lower, MILLIS_PER_DAY),
                        Math.addExact(Math.multiplyExact(upper, MILLIS_PER_DAY), MILLIS_PER_DAY - 1L) };
                }
                return new long[] { lower, upper };
            }
            if (type instanceof ArrowType.Timestamp timestamp) {
                return switch (timestamp.getUnit()) {
                    case SECOND -> new long[] { Math.multiplyExact(lower, 1000L), Math.addExact(Math.multiplyExact(upper, 1000L), 999L) };
                    case MILLISECOND -> new long[] { lower, upper };
                    case MICROSECOND -> new long[] { Math.floorDiv(lower, 1000L), ceilDiv(upper, 1000L) };
                    case NANOSECOND -> new long[] { Math.floorDiv(lower, 1_000_000L), ceilDiv(upper, 1_000_000L) };
                };
            }
        } catch (ArithmeticException overflow) {
            return null;
        }
        return null;
    }

    private static long ceilDiv(long value, long divisor) {
        return -Math.floorDiv(-value, divisor);
    }

    private static boolean isUnixMillis(RexNode node) {
        RexNode stripped = stripCasts(node);
        return stripped instanceof RexCall call && call.getOperands().size() == 1 && call.getOperator().getName().equals("UNIX_MILLIS");
    }

    private static RexNode stripCasts(RexNode node) {
        while (node.getKind() == SqlKind.CAST) {
            node = ((RexCall) node).getOperands().get(0);
        }
        return node;
    }

    /**
     * A string compared by code point, which orders the way the UTF-8
     * bytes Lance compared the zone bounds in order; {@code String}'s
     * own order compares UTF-16 units and ranks a supplementary
     * character below every BMP character above the surrogate range.
     */
    private record CodePointString(String value) implements Comparable<CodePointString> {

        @Override
        public int compareTo(CodePointString other) {
            int i = 0;
            int j = 0;
            while (i < value.length() && j < other.value.length()) {
                int a = value.codePointAt(i);
                int b = other.value.codePointAt(j);
                if (a != b) {
                    return Integer.compare(a, b);
                }
                i += Character.charCount(a);
                j += Character.charCount(b);
            }
            return Integer.compare(value.length() - i, other.value.length() - j);
        }
    }

    /** The excluded ids as a sorted, distinct array for the wire. */
    public static int[] toArray(Collection<Integer> fragmentIds) {
        SortedSet<Integer> sorted = fragmentIds instanceof SortedSet<Integer> set ? set : new TreeSet<>(fragmentIds);
        int[] array = new int[sorted.size()];
        int i = 0;
        for (Integer id : sorted) {
            array[i++] = id;
        }
        return array;
    }
}

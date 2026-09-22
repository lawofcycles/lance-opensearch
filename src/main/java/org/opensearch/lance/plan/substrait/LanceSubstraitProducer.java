/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.NamedScan;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.lance.plan.rel.MetricSpec;

/**
 * Produces the Substrait bytes Lance's
 * {@code ScanOptions.Builder#substraitAggregate} accepts from a Calcite
 * {@code Aggregate} over a Lance table scan. Lance's consumer decodes
 * the plan's single relation as an {@code AggregateRel} and reads only
 * its grouping expressions and measures, resolved against the dataset
 * schema; the relation's input and every other relation kind are
 * ignored, so the producer emits a {@code NamedScan} input for a well
 * formed tree and folds every computed expression inline.
 *
 * <p>The Calcite side of the conversion is isthmus's
 * {@link RexExpressionConverter}, run per group key and per call
 * argument rather than over the whole tree, because the aggregate
 * calls of the wider OpenSearch metrics ({@code stats},
 * {@code cardinality}, {@code percentiles}) have no Substrait function
 * binding: their expansion comes from the {@link LanceAggregateSpecs}
 * carried by the aggregate node, mirroring what the executor reads
 * back out of the scan. A {@code Project} under the aggregate (Calcite
 * requires group keys to be input fields, so computed keys live in
 * one) is folded before conversion by converting the projected key
 * expressions directly; the emitted tree therefore never contains a
 * {@code Project}.
 *
 * <p>Field references are positions in the dataset schema (the scan's
 * row type preserves the Arrow field order). The DataFusion build
 * inside Lance registers no math functions and builds physical
 * expressions without the coercion pass, so {@code FLOOR} is rewritten
 * to truncating division and comparisons, Calcite's epoch-millis cast
 * of a date or timestamp is rewritten to the unit arithmetic the
 * column's Arrow type needs, and any function outside the set the
 * consumer resolves makes the producer return empty.
 */
public final class LanceSubstraitProducer {

    private LanceSubstraitProducer() {}

    /** Output column of the {@code count(*)} measure every main scan carries. */
    private static final String COUNT_COLUMN = "n";

    /** Output column prefix of the grouping expressions, {@code k0}, {@code k1}, ... */
    private static final String KEY_COLUMN_PREFIX = "k";

    /**
     * Scalar function names the producer may emit: the names Lance's
     * consumer maps onto DataFusion operators, unary expressions or
     * registered UDFs. Anything else in a converted expression makes
     * the producer return empty.
     */
    private static final Set<String> ACCEPTED_SCALARS = Set.of(
        "add",
        "subtract",
        "multiply",
        "divide",
        "modulus",
        "lt",
        "gt",
        "lte",
        "gte",
        "equal",
        "not_equal",
        "and",
        "or",
        "not",
        "is_null",
        "is_not_null",
        "is_true",
        "date_trunc"
    );

    /**
     * Encodes the aggregate as the main Lance scan: the group key
     * expressions in group set order, the distinct-value grouping of
     * every {@code cardinality} call after them, a {@code count(*)}
     * measure and every metric's measures. Empty when the tree is not
     * an {@code Aggregate} implementing {@link LanceAggregateSpecs}
     * over an optional {@code Project} and an optional {@code Filter}
     * (in either order) over a {@code TableScan}, or when an expression
     * needs a function Lance's consumer cannot resolve. A {@code Filter}
     * input is accepted but not encoded: the scan filter travels next
     * to the plan in {@code ScanOptions.filter}, never inside these
     * bytes.
     */
    public static Optional<ByteBuffer> toLanceAggregate(RelNode root) {
        Shape shape = resolve(root);
        if (shape == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(encodeMain(shape));
        } catch (UnsupportedOperationException | IllegalArgumentException outsideTheConsumer) {
            return Optional.empty();
        }
    }

    /**
     * Encodes the bin-count scan a {@code percentiles} /
     * {@code percentile_ranks} call takes after the main scan returned
     * its bounds: the same group keys plus the call's value bucketed
     * into {@code floor((value - min) / width)} with
     * {@code width = (max - min) / bins} (any positive width when
     * {@code min == max}), and one {@code count(field)} measure. Empty
     * under the same conditions as
     * {@link #toLanceAggregate(RelNode)}, when {@code callIndex} does
     * not name a percentiles call, or when the bounds are not finite.
     */
    public static Optional<ByteBuffer> toLancePercentilesBins(RelNode root, int callIndex, double min, double max, int bins) {
        Shape shape = resolve(root);
        if (shape == null
            || callIndex < 0
            || callIndex >= shape.aggregate.getAggCallList().size()
            || bins <= 0
            || !Double.isFinite(min)
            || !Double.isFinite(max)
            || min > max) {
            return Optional.empty();
        }
        MetricSpec.Kind kind = shape.specs.metric(callIndex).kind();
        if (kind != MetricSpec.Kind.PERCENTILES && kind != MetricSpec.Kind.PERCENTILE_RANKS) {
            return Optional.empty();
        }
        try {
            return Optional.of(encodeBins(shape, callIndex, min, max, bins));
        } catch (UnsupportedOperationException | IllegalArgumentException outsideTheConsumer) {
            return Optional.empty();
        }
    }

    /** The accepted tree shape, with the project folded away by construction. */
    private static final class Shape {
        final Aggregate aggregate;
        final LanceAggregateSpecs specs;
        final Project project;
        final TableScan scan;

        Shape(Aggregate aggregate, LanceAggregateSpecs specs, Project project, TableScan scan) {
            this.aggregate = aggregate;
            this.specs = specs;
            this.project = project;
            this.scan = scan;
        }

        /**
         * The Calcite expression behind position {@code inputIndex} of
         * the aggregate's input row: the projected expression when a
         * project sits under the aggregate, the scan field itself
         * otherwise.
         */
        RexNode inputExpression(int inputIndex) {
            if (project != null) {
                return project.getProjects().get(inputIndex);
            }
            return RexInputRef.of(inputIndex, scan.getRowType());
        }
    }

    private static Shape resolve(RelNode root) {
        if (!(root instanceof Aggregate aggregate) || !(root instanceof LanceAggregateSpecs specs)) {
            return null;
        }
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            return null;
        }
        List<AggregateCall> calls = aggregate.getAggCallList();
        for (int slot = 0; slot < calls.size(); slot++) {
            AggregateCall call = calls.get(slot);
            // The translator spells a cardinality as COUNT(DISTINCT col);
            // its expansion comes from the spec kind, not from the
            // Calcite function, so the distinct flag is expected there.
            boolean cardinality = specs.metric(slot).kind() == MetricSpec.Kind.CARDINALITY;
            if ((call.isDistinct() && !cardinality) || call.filterArg >= 0 || !call.getCollation().getFieldCollations().isEmpty()) {
                return null;
            }
        }
        RelNode input = aggregate.getInput();
        Project project = null;
        boolean filtered = false;
        if (input instanceof Filter f) {
            // A filter over the projected keys, as RelBuilder builds it
            // for an aggregate over a filtered project. The condition
            // never travels in the aggregate bytes: Lance's consumer
            // reads the AggregateRel only, and the scan filter is passed
            // through ScanOptions.filter instead.
            filtered = true;
            input = f.getInput();
        }
        if (input instanceof Project p) {
            project = p;
            input = p.getInput();
        }
        if (input instanceof Filter f && !filtered) {
            input = f.getInput();
        }
        if (!(input instanceof TableScan scan)) {
            return null;
        }
        return new Shape(aggregate, specs, project, scan);
    }

    // ---------------------------------------------------------------
    // Encoding
    // ---------------------------------------------------------------

    private static ByteBuffer encodeMain(Shape shape) {
        RexExpressionConverter converter = converter(shape);
        List<Expression> groupings = new ArrayList<>();
        List<String> names = new ArrayList<>();

        List<Integer> keys = shape.aggregate.getGroupSet().asList();
        for (int key = 0; key < keys.size(); key++) {
            RexNode rex = shape.inputExpression(keys.get(key));
            groupings.add(numeric(rex, accepted(rex.accept(converter))));
            names.add(KEY_COLUMN_PREFIX + key);
        }

        List<AggregateCall> calls = shape.aggregate.getAggCallList();
        for (int slot = 0; slot < calls.size(); slot++) {
            if (shape.specs.metric(slot).kind() == MetricSpec.Kind.CARDINALITY) {
                RexNode rex = argument(shape, calls.get(slot));
                groupings.add(distinct(rex, accepted(rex.accept(converter))));
                names.add(prefix(slot) + "_d");
            }
        }

        List<io.substrait.relation.Aggregate.Measure> measures = new ArrayList<>();
        measures.add(measure("count", TypeCreator.NULLABLE.I64));
        names.add(COUNT_COLUMN);
        for (int slot = 0; slot < calls.size(); slot++) {
            addMeasures(shape, converter, calls.get(slot), shape.specs.metric(slot).kind(), slot, measures, names);
        }

        return serialize(shape, groupings, measures, names);
    }

    private static ByteBuffer encodeBins(Shape shape, int callIndex, double min, double max, int bins) {
        RexExpressionConverter converter = converter(shape);
        List<Expression> groupings = new ArrayList<>();
        List<String> names = new ArrayList<>();

        List<Integer> keys = shape.aggregate.getGroupSet().asList();
        for (int key = 0; key < keys.size(); key++) {
            RexNode rex = shape.inputExpression(keys.get(key));
            groupings.add(numeric(rex, accepted(rex.accept(converter))));
            names.add(KEY_COLUMN_PREFIX + key);
        }

        RexNode rex = argument(shape, shape.aggregate.getAggCallList().get(callIndex));
        Expression reference = accepted(rex.accept(converter));
        Expression value = numeric(rex, reference);
        // Every value falls into bin 0 when they are all equal; any
        // positive width does that, and keeps the centre at the value.
        double width = max > min ? (max - min) / bins : 1d;
        groupings.add(floorOrdinal(value, min, width));
        names.add(prefix(callIndex) + "_b");

        // count(field), not count(*): a row without a value has a null
        // bin and must not weigh in.
        List<io.substrait.relation.Aggregate.Measure> measures = new ArrayList<>();
        measures.add(measure("count", TypeCreator.NULLABLE.I64, reference));
        names.add(prefix(callIndex) + "_bc");

        return serialize(shape, groupings, measures, names);
    }

    /**
     * The measures one metric call expands to, mirroring what the
     * executor reads back per output column: {@code avg} as a sum and a
     * count so partial values of several scans add up, {@code stats} /
     * {@code extended_stats} as their five (six) running values,
     * {@code percentiles} as the bounds its bin scan is cut from, and
     * {@code cardinality} as nothing (its grouping is added by the
     * caller).
     */
    private static void addMeasures(
        Shape shape,
        RexExpressionConverter converter,
        AggregateCall call,
        MetricSpec.Kind kind,
        int slot,
        List<io.substrait.relation.Aggregate.Measure> measures,
        List<String> names
    ) {
        if (kind == MetricSpec.Kind.CARDINALITY) {
            return;
        }
        RexNode rex = argument(shape, call);
        Expression value = numeric(rex, accepted(rex.accept(converter)));
        Expression reference = accepted(rex.accept(converter));
        Type type = floating(rex.getType()) ? TypeCreator.NULLABLE.FP64 : TypeCreator.NULLABLE.I64;
        String prefix = prefix(slot);
        switch (kind) {
            case SUM -> {
                measures.add(measure("sum", type, value));
                names.add(prefix);
            }
            case MIN -> {
                measures.add(measure("min", type, value));
                names.add(prefix);
            }
            case MAX -> {
                measures.add(measure("max", type, value));
                names.add(prefix);
            }
            case VALUE_COUNT -> {
                measures.add(measure("count", TypeCreator.NULLABLE.I64, reference));
                names.add(prefix);
            }
            case AVG -> {
                measures.add(measure("sum", type, value));
                names.add(prefix + "_s");
                measures.add(measure("count", TypeCreator.NULLABLE.I64, reference));
                names.add(prefix + "_c");
            }
            case STATS, EXTENDED_STATS -> {
                measures.add(measure("count", TypeCreator.NULLABLE.I64, reference));
                names.add(prefix + "_c");
                measures.add(measure("sum", type, value));
                names.add(prefix + "_s");
                measures.add(measure("min", type, value));
                names.add(prefix + "_mn");
                measures.add(measure("max", type, value));
                names.add(prefix + "_mx");
                if (kind == MetricSpec.Kind.EXTENDED_STATS) {
                    Expression asDouble = cast(TypeCreator.NULLABLE.FP64, value);
                    measures.add(
                        measure("sum", TypeCreator.NULLABLE.FP64, scalar("multiply", TypeCreator.NULLABLE.FP64, asDouble, asDouble))
                    );
                    names.add(prefix + "_q");
                }
            }
            case PERCENTILES, PERCENTILE_RANKS -> {
                // The bounds the bins of the second scan are cut from.
                measures.add(measure("min", type, value));
                names.add(prefix + "_mn");
                measures.add(measure("max", type, value));
                names.add(prefix + "_mx");
            }
            default -> throw new IllegalArgumentException("no measure expansion for " + kind);
        }
    }

    /** The single argument of a metric call as a Calcite expression over the scan row. */
    private static RexNode argument(Shape shape, AggregateCall call) {
        if (call.getArgList().size() != 1) {
            throw new IllegalArgumentException("a metric call takes one argument, not " + call.getArgList().size());
        }
        return shape.inputExpression(call.getArgList().get(0));
    }

    private static String prefix(int slot) {
        return "m" + slot;
    }

    /**
     * Wraps the tree into a plan whose single relation is the
     * aggregate over a {@code NamedScan} of the scan's row type. The
     * JNI side reads the returned buffer through
     * {@code GetDirectBufferAddress}, which returns null for a heap
     * buffer, so the bytes go into a direct buffer.
     */
    private static ByteBuffer serialize(
        Shape shape,
        List<Expression> groupings,
        List<io.substrait.relation.Aggregate.Measure> measures,
        List<String> names
    ) {
        NamedScan namedScan = NamedScan.builder()
            .initialSchema(TypeConverter.DEFAULT.toNamedStruct(shape.scan.getRowType()))
            .addAllNames(shape.scan.getTable().getQualifiedName())
            .build();
        var aggregate = io.substrait.relation.Aggregate.builder().input(namedScan);
        if (!groupings.isEmpty()) {
            aggregate.addGroupings(io.substrait.relation.Aggregate.Grouping.builder().addAllExpressions(groupings).build());
        }
        aggregate.addAllMeasures(measures);
        Plan plan = Plan.builder().addRoots(Plan.Root.builder().input(aggregate.build()).addAllNames(names).build()).build();
        byte[] bytes = new PlanProtoConverter().toProto(plan).toByteArray();
        ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
        direct.put(bytes);
        direct.flip();
        return direct;
    }

    private static io.substrait.relation.Aggregate.Measure measure(String function, Type outputType, Expression... arguments) {
        AggregateFunctionInvocation invocation = ExpressionCreator.aggregateFunction(
            Extensions.aggregate(function),
            outputType,
            Expression.AggregationPhase.INITIAL_TO_RESULT,
            List.of(),
            Expression.AggregationInvocation.ALL,
            arguments
        );
        return io.substrait.relation.Aggregate.Measure.builder().function(invocation).build();
    }

    // ---------------------------------------------------------------
    // Expression conversion and normalisation
    // ---------------------------------------------------------------

    /**
     * The isthmus expression converter, with a Lance converter in
     * front of the defaults for the calls the consumer's DataFusion
     * build cannot take as isthmus spells them.
     */
    private static RexExpressionConverter converter(Shape shape) {
        RelDataTypeFactory typeFactory = shape.aggregate.getCluster().getTypeFactory();
        List<CallConverter> converters = new ArrayList<>();
        converters.add(new LanceCallConverter());
        converters.addAll(CallConverters.defaults(TypeConverter.DEFAULT));
        converters.add(new ScalarFunctionConverter(Extensions.COLLECTION.scalarFunctions(), typeFactory));
        converters.add(CallConverters.CREATE_SEARCH_CONV.apply(new RexBuilder(typeFactory)));
        WindowFunctionConverter windows = new WindowFunctionConverter(Extensions.COLLECTION.windowFunctions(), typeFactory);
        return new RexExpressionConverter(null, converters, windows, TypeConverter.DEFAULT);
    }

    /**
     * Intercepts, before isthmus's own converters run, the calls whose
     * literal Substrait spelling the Lance consumer cannot evaluate:
     *
     * <ul>
     * <li>{@code FLOOR} of an integer division: the translator spells a
     * fixed interval bucket ordinal as {@code FLOOR(millis / interval)},
     * but an integer division truncates toward zero while the
     * aggregator floors, so the quotient is lowered by one when the
     * remainder is negative, the {@code Math.floorDiv} identity
     * {@code (a / b) - ((a % b) < 0 ? 1 : 0)}.</li>
     * <li>{@code FLOOR} of any other integer: the operand itself.</li>
     * <li>{@code FLOOR} of a floating value: DataFusion inside Lance
     * registers no math functions, so floor is derived from a
     * truncating cast and a comparison, cast back to the operand's
     * type. </li>
     * <li>{@code CAST} of a date or timestamp to an integer: Calcite's
     * cast means epoch millis, while DataFusion's cast yields the raw
     * ticks of the column's unit, so the unit arithmetic is spelled
     * out.</li>
     * <li>{@code UNIX_MILLIS}, matched by operator name: the same epoch
     * millis arithmetic. The translator spells epoch millis of a
     * timestamp column with this operator, and of a {@code DATE} column
     * as {@code UNIX_MILLIS(CAST(day AS TIMESTAMP))}; the cast is
     * looked through so the day count is multiplied by 86 400 000
     * directly instead of leaning on DataFusion's date-to-timestamp
     * cast.</li>
     * <li>{@code LANCE_DATE_TRUNC}, matched by operator name: becomes
     * DataFusion's {@code date_trunc(unit, value)} with the unit as a
     * plain string literal.</li>
     * <li>{@code IS TRUE}: no isthmus mapping exists; the consumer
     * resolves the name.</li>
     * </ul>
     */
    private static final class LanceCallConverter implements CallConverter {

        @Override
        public Optional<Expression> convert(RexCall call, Function<RexNode, Expression> converter) {
            if (call.getKind() == SqlKind.FLOOR && call.getOperands().size() == 1) {
                RexNode operand = call.getOperands().get(0);
                SqlTypeName operandType = operand.getType().getSqlTypeName();
                if (SqlTypeName.INT_TYPES.contains(operandType)) {
                    if (operand instanceof RexCall division
                        && division.getKind() == SqlKind.DIVIDE
                        && division.getOperands().size() == 2
                        && SqlTypeName.INT_TYPES.contains(division.getOperands().get(0).getType().getSqlTypeName())
                        && SqlTypeName.INT_TYPES.contains(division.getOperands().get(1).getType().getSqlTypeName())) {
                        Expression dividend = converter.apply(division.getOperands().get(0));
                        Expression divisor = converter.apply(division.getOperands().get(1));
                        return Optional.of(floorDiv(dividend, divisor));
                    }
                    return Optional.of(converter.apply(operand));
                }
                if (SqlTypeName.APPROX_TYPES.contains(operandType)) {
                    Expression value = converter.apply(operand);
                    Type type = TypeConverter.DEFAULT.toSubstrait(operand.getType());
                    return Optional.of(cast(type, floorOrdinal(value, 0d, 1d)));
                }
                return Optional.empty();
            }
            if (call.getKind() == SqlKind.CAST && isDateOrTimestamp(call.getOperands().get(0).getType()) && isInteger(call.getType())) {
                RexNode operand = call.getOperands().get(0);
                return Optional.of(epochMillis(converter.apply(operand), operand.getType()));
            }
            if (call.getOperator().getName().toUpperCase(Locale.ROOT).equals("UNIX_MILLIS") && call.getOperands().size() == 1) {
                RexNode operand = call.getOperands().get(0);
                if (operand instanceof RexCall inner
                    && inner.getKind() == SqlKind.CAST
                    && inner.getOperands().get(0).getType().getSqlTypeName() == SqlTypeName.DATE) {
                    RexNode day = inner.getOperands().get(0);
                    return Optional.of(epochMillis(converter.apply(day), day.getType()));
                }
                return Optional.of(epochMillis(converter.apply(operand), operand.getType()));
            }
            if (call.getOperator().getName().toUpperCase(Locale.ROOT).equals("LANCE_DATE_TRUNC") && call.getOperands().size() == 2) {
                Expression unit = stringLiteral(converter.apply(call.getOperands().get(0)));
                Expression value = converter.apply(call.getOperands().get(1));
                Type type = TypeConverter.DEFAULT.toSubstrait(call.getType());
                return Optional.of(scalar("date_trunc", type, unit, value));
            }
            if (call.getKind() == SqlKind.IS_TRUE && call.getOperands().size() == 1) {
                Expression value = converter.apply(call.getOperands().get(0));
                return Optional.of(scalar("is_true", TypeCreator.NULLABLE.BOOLEAN, value));
            }
            return Optional.empty();
        }
    }

    /**
     * {@code Math.floorDiv(a, b)} on {@code i64} values: the truncating
     * division lowered by one when the remainder is negative, the
     * arithmetic the fragment leaf reader's date bucketing applies.
     */
    private static Expression floorDiv(Expression dividend, Expression divisor) {
        Expression quotient = scalar("divide", TypeCreator.NULLABLE.I64, dividend, divisor);
        Expression remainder = scalar("modulus", TypeCreator.NULLABLE.I64, dividend, divisor);
        Expression negative = scalar("lt", TypeCreator.NULLABLE.BOOLEAN, remainder, i64(0L));
        return scalar("subtract", TypeCreator.NULLABLE.I64, quotient, cast(TypeCreator.NULLABLE.I64, negative));
    }

    private static boolean isDateOrTimestamp(RelDataType type) {
        SqlTypeName name = type.getSqlTypeName();
        return name == SqlTypeName.DATE || name == SqlTypeName.TIMESTAMP || name == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    private static boolean isInteger(RelDataType type) {
        return SqlTypeName.INT_TYPES.contains(type.getSqlTypeName());
    }

    private static boolean floating(RelDataType type) {
        return SqlTypeName.APPROX_TYPES.contains(type.getSqlTypeName());
    }

    /**
     * The expression as the number the executor reads: dates and
     * timestamps as epoch millis, booleans as 0 / 1, everything else
     * unchanged. {@code rex} supplies the Calcite type; {@code value}
     * is its converted form.
     */
    private static Expression numeric(RexNode rex, Expression value) {
        if (isDateOrTimestamp(rex.getType())) {
            return epochMillis(value, rex.getType());
        }
        if (rex.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            return cast(TypeCreator.NULLABLE.I64, value);
        }
        return value;
    }

    /**
     * The grouping expression a {@code cardinality} adds: strings and
     * floating values as themselves (the executor hashes them in their
     * own type), everything else as the number.
     */
    private static Expression distinct(RexNode rex, Expression value) {
        SqlTypeName type = rex.getType().getSqlTypeName();
        if (SqlTypeName.CHAR_TYPES.contains(type) || SqlTypeName.APPROX_TYPES.contains(type)) {
            return value;
        }
        return numeric(rex, value);
    }

    /**
     * Epoch milliseconds of a date or timestamp as an {@code i64},
     * converted the way the fragment leaf reader converts the column
     * for doc values: date days are multiplied by 86 400 000, second
     * precision by 1000, micro and nanoseconds are divided with
     * truncation toward zero. The Calcite precision stands in for the
     * Arrow unit (0, 3, 6 and 9 are the units Arrow has).
     */
    private static Expression epochMillis(Expression value, RelDataType type) {
        Expression asInt64 = cast(TypeCreator.NULLABLE.I64, value);
        if (type.getSqlTypeName() == SqlTypeName.DATE) {
            return scalar("multiply", TypeCreator.NULLABLE.I64, asInt64, i64(86_400_000L));
        }
        return switch (type.getPrecision()) {
            case 0 -> scalar("multiply", TypeCreator.NULLABLE.I64, asInt64, i64(1000L));
            case 3 -> asInt64;
            case 6 -> scalar("divide", TypeCreator.NULLABLE.I64, asInt64, i64(1000L));
            case 9 -> scalar("divide", TypeCreator.NULLABLE.I64, asInt64, i64(1_000_000L));
            default -> throw new IllegalArgumentException("no Arrow unit behind timestamp precision " + type.getPrecision());
        };
    }

    /**
     * {@code Math.floor((value - offset) / interval)} on {@code fp64}
     * values as an {@code i64}. The quotient is truncated by a cast and
     * lowered by one when the truncation moved it up, because
     * DataFusion's cast truncates toward zero.
     */
    private static Expression floorOrdinal(Expression value, double offset, double interval) {
        Expression asDouble = cast(TypeCreator.NULLABLE.FP64, value);
        Expression shifted = offset == 0d ? asDouble : scalar("subtract", TypeCreator.NULLABLE.FP64, asDouble, fp64(offset));
        Expression quotient = interval == 1d ? shifted : scalar("divide", TypeCreator.NULLABLE.FP64, shifted, fp64(interval));
        Expression truncated = cast(TypeCreator.NULLABLE.I64, quotient);
        Expression movedUp = scalar("gt", TypeCreator.NULLABLE.BOOLEAN, cast(TypeCreator.NULLABLE.FP64, truncated), quotient);
        return scalar("subtract", TypeCreator.NULLABLE.I64, truncated, cast(TypeCreator.NULLABLE.I64, movedUp));
    }

    /** A char or varchar literal as the plain string literal {@code date_trunc} takes its unit in. */
    private static Expression stringLiteral(Expression literal) {
        if (literal instanceof Expression.FixedCharLiteral fixedChar) {
            return ExpressionCreator.string(false, fixedChar.value());
        }
        if (literal instanceof Expression.VarCharLiteral varChar) {
            return ExpressionCreator.string(false, varChar.value());
        }
        return literal;
    }

    private static Expression scalar(String function, Type outputType, Expression... arguments) {
        return ExpressionCreator.scalarFunction(Extensions.scalar(function), outputType, arguments);
    }

    private static Expression cast(Type type, Expression input) {
        return ExpressionCreator.cast(type, input, Expression.FailureBehavior.THROW_EXCEPTION);
    }

    private static Expression i64(long value) {
        return ExpressionCreator.i64(false, value);
    }

    private static Expression fp64(double value) {
        return ExpressionCreator.fp64(false, value);
    }

    // ---------------------------------------------------------------
    // Acceptance
    // ---------------------------------------------------------------

    /**
     * Refuses any expression node the Lance consumer is not known to
     * evaluate. The walk is a conservative allow list: field
     * references into the root struct, the literal kinds the consumer
     * decodes into plain Arrow scalars, casts to the four types the
     * producer spells, {@code CASE} and the accepted scalar functions.
     * Date and timestamp literals are refused (the consumer's decoding
     * of them predates the unit fix), as is everything unrecognised.
     */
    private static Expression accepted(Expression expression) {
        if (!isAccepted(expression)) {
            throw new IllegalArgumentException("expression outside the Lance consumer's vocabulary: " + expression);
        }
        return expression;
    }

    private static boolean isAccepted(Expression expression) {
        if (expression instanceof FieldReference reference) {
            return reference.isSimpleRootReference()
                && reference.segments().get(0) instanceof FieldReference.StructField
                && reference.outerReferenceStepsOut().isEmpty();
        }
        if (expression instanceof Expression.BoolLiteral
            || expression instanceof Expression.I8Literal
            || expression instanceof Expression.I16Literal
            || expression instanceof Expression.I32Literal
            || expression instanceof Expression.I64Literal
            || expression instanceof Expression.FP32Literal
            || expression instanceof Expression.FP64Literal
            || expression instanceof Expression.StrLiteral
            || expression instanceof Expression.FixedCharLiteral
            || expression instanceof Expression.VarCharLiteral) {
            return true;
        }
        if (expression instanceof Expression.NullLiteral nullLiteral) {
            // Calcite expands a boolean cast into CASE(IS NOT NULL(x),
            // CASE(x, 1, 0), NULL): the typed null of a scalar type is a
            // plain null scalar to the consumer.
            Type type = nullLiteral.type();
            return type instanceof Type.Bool
                || type instanceof Type.I8
                || type instanceof Type.I16
                || type instanceof Type.I32
                || type instanceof Type.I64
                || type instanceof Type.FP32
                || type instanceof Type.FP64
                || type instanceof Type.Str;
        }
        if (expression instanceof Expression.Cast cast) {
            Type target = cast.type();
            boolean spellable = target instanceof Type.I64
                || target instanceof Type.FP64
                || target instanceof Type.Bool
                || target instanceof Type.Str;
            return spellable && isAccepted(cast.input());
        }
        if (expression instanceof Expression.IfThen ifThen) {
            for (Expression.IfClause clause : ifThen.ifClauses()) {
                if (!isAccepted(clause.condition()) || !isAccepted(clause.then())) {
                    return false;
                }
            }
            return isAccepted(ifThen.elseClause());
        }
        if (expression instanceof Expression.ScalarFunctionInvocation invocation) {
            if (!ACCEPTED_SCALARS.contains(invocation.declaration().name())) {
                return false;
            }
            for (FunctionArg argument : invocation.arguments()) {
                if (!(argument instanceof Expression nested) || !isAccepted(nested)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    // ---------------------------------------------------------------
    // Extension declarations
    // ---------------------------------------------------------------

    /**
     * The function declarations the producer emits references to. The
     * standard catalog carries everything except {@code date_trunc}
     * (DataFusion's datetime UDF, not a Substrait standard function)
     * and {@code is_true}, which a small inline extension supplies.
     * Lance's consumer reads only the name before the signature colon
     * and ignores the extension URIs, so the first variant of a name
     * is as good as any.
     */
    private static final class Extensions {

        private static final String LANCE_FUNCTIONS = """
            %YAML 1.2
            ---
            scalar_functions:
              - name: "date_trunc"
                description: >-
                  DataFusion's date_trunc: the first instant of the calendar
                  unit that contains the timestamp, as a timestamp of the
                  same unit.
                impls:
                  - args:
                      - name: unit
                        value: string
                      - name: value
                        value: timestamp
                    return: timestamp
                  - args:
                      - name: unit
                        value: string
                      - name: value
                        value: timestamp_tz
                    return: timestamp_tz
              - name: "is_true"
                description: >-
                  True when the argument is true, false when it is false or
                  null; the two valued collapse a must_not clause needs.
                impls:
                  - args:
                      - name: value
                        value: boolean?
                    return: boolean
            """;

        static final SimpleExtension.ExtensionCollection COLLECTION = SimpleExtension.loadDefaults()
            .merge(SimpleExtension.load("extension:org.opensearch.lance:functions_lance", LANCE_FUNCTIONS));

        private static final Map<String, SimpleExtension.ScalarFunctionVariant> SCALARS = indexScalars();
        private static final Map<String, SimpleExtension.AggregateFunctionVariant> AGGREGATES = indexAggregates();

        private static Map<String, SimpleExtension.ScalarFunctionVariant> indexScalars() {
            Map<String, SimpleExtension.ScalarFunctionVariant> byName = new LinkedHashMap<>();
            for (SimpleExtension.ScalarFunctionVariant variant : COLLECTION.scalarFunctions()) {
                byName.putIfAbsent(variant.name(), variant);
            }
            return byName;
        }

        private static Map<String, SimpleExtension.AggregateFunctionVariant> indexAggregates() {
            Map<String, SimpleExtension.AggregateFunctionVariant> byName = new LinkedHashMap<>();
            for (SimpleExtension.AggregateFunctionVariant variant : COLLECTION.aggregateFunctions()) {
                byName.putIfAbsent(variant.name(), variant);
            }
            return byName;
        }

        static SimpleExtension.ScalarFunctionVariant scalar(String name) {
            SimpleExtension.ScalarFunctionVariant variant = SCALARS.get(name);
            if (variant == null) {
                throw new IllegalArgumentException("no declaration for scalar function " + name);
            }
            return variant;
        }

        static SimpleExtension.AggregateFunctionVariant aggregate(String name) {
            SimpleExtension.AggregateFunctionVariant variant = AGGREGATES.get(name);
            if (variant == null) {
                throw new IllegalArgumentException("no declaration for aggregate function " + name);
            }
            return variant;
        }

        private Extensions() {}
    }
}

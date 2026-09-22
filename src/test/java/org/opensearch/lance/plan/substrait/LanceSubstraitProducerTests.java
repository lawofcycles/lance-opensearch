/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.substrait.proto.AggregateRel;
import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchema;
import org.opensearch.lance.plan.calcite.LanceTable;
import org.opensearch.lance.plan.substrait.LanceAggregateSpecs.BucketKind;
import org.opensearch.lance.plan.substrait.LanceAggregateSpecs.BucketSpec;
import org.opensearch.lance.plan.substrait.LanceAggregateSpecs.MetricKind;
import org.opensearch.lance.plan.substrait.LanceAggregateSpecs.MetricSpec;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Pins the producer's contract without a Lance dataset: the accepted
 * tree shapes, the refusals, the project fold, the function mapping
 * and the serialized proto layout. The scan-result equivalence against
 * the legacy producer runs in the integration test.
 */
public class LanceSubstraitProducerTests extends OpenSearchTestCase {

    private static final Schema SCHEMA = new Schema(
        List.of(
            field("id", new ArrowType.Int(32, true)),
            field("rating", new ArrowType.Int(32, true)),
            field("price", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            field("category", new ArrowType.Utf8()),
            field("flag", new ArrowType.Bool()),
            field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
            field("ts_s", new ArrowType.Timestamp(TimeUnit.SECOND, null)),
            field("ts_ns", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null))
        )
    );

    private static final SqlFunction LANCE_DATE_TRUNC = new SqlFunction(
        "LANCE_DATE_TRUNC",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG1_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.TIMEDATE
    );

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    private RelBuilder relBuilder() {
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        LanceSchema schema = new LanceSchema(Map.of("t", new LanceTable("t", SCHEMA, () -> 512L)));
        return factory.relBuilder(schema).transform(config -> config.withSimplify(false));
    }

    private static AggregateCall call(SqlAggFunction function, RelNode input, int groupCount, int argument, String name) {
        return AggregateCall.create(function, false, false, List.of(argument), -1, groupCount, input, null, name);
    }

    private static Plan parse(ByteBuffer buffer) throws Exception {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return Plan.parseFrom(bytes);
    }

    private static AggregateRel aggregateOf(Plan plan) {
        assertEquals(1, plan.getRelationsCount());
        Rel input = plan.getRelations(0).getRoot().getInput();
        assertTrue("root relation must be an aggregate", input.hasAggregate());
        return input.getAggregate();
    }

    private static List<String> functionNames(Plan plan) {
        List<String> names = new ArrayList<>();
        for (SimpleExtensionDeclaration declaration : plan.getExtensionsList()) {
            if (declaration.hasExtensionFunction()) {
                names.add(declaration.getExtensionFunction().getName().split(":", 2)[0]);
            }
        }
        return names;
    }

    private static Map<Integer, String> functionsByAnchor(Plan plan) {
        Map<Integer, String> names = new HashMap<>();
        for (SimpleExtensionDeclaration declaration : plan.getExtensionsList()) {
            if (declaration.hasExtensionFunction()) {
                names.put(
                    declaration.getExtensionFunction().getFunctionAnchor(),
                    declaration.getExtensionFunction().getName().split(":", 2)[0]
                );
            }
        }
        return names;
    }

    private static Expression onlyGrouping(Plan plan) {
        AggregateRel rel = aggregateOf(plan);
        assertEquals(1, rel.getGroupingsCount());
        assertEquals(1, rel.getGroupings(0).getGroupingExpressionsCount());
        return rel.getGroupings(0).getGroupingExpressions(0);
    }

    public void testNonAggregateRootIsEmpty() {
        RelNode scan = relBuilder().scan("lance", "t").build();
        assertEquals(Optional.empty(), LanceSubstraitProducer.toLanceAggregate(scan));
    }

    public void testAggregateWithoutSpecsIsEmpty() {
        RelBuilder builder = relBuilder();
        RelNode aggregate = builder.scan("lance", "t").aggregate(builder.groupKey(), builder.count(false, "n")).build();
        assertEquals(Optional.empty(), LanceSubstraitProducer.toLanceAggregate(aggregate));
    }

    public void testAggregateOverJoinIsEmpty() {
        RelBuilder builder = relBuilder();
        RelNode join = builder.scan("lance", "t").scan("lance", "t").join(JoinRelType.INNER, builder.literal(true)).build();
        SpecAggregate aggregate = new SpecAggregate(
            join,
            ImmutableBitSet.of(),
            List.of(call(SqlStdOperatorTable.SUM, join, 0, 1, "m0")),
            List.of(),
            List.of(MetricSpec.of(MetricKind.SUM, "m"))
        );
        assertEquals(Optional.empty(), LanceSubstraitProducer.toLanceAggregate(aggregate));
    }

    public void testDistinctCallIsEmpty() {
        RelNode scan = relBuilder().scan("lance", "t").build();
        AggregateCall distinct = AggregateCall.create(SqlStdOperatorTable.SUM, true, false, List.of(1), -1, 0, scan, null, "m0");
        SpecAggregate aggregate = new SpecAggregate(
            scan,
            ImmutableBitSet.of(),
            List.of(distinct),
            List.of(),
            List.of(MetricSpec.of(MetricKind.SUM, "m"))
        );
        assertEquals(Optional.empty(), LanceSubstraitProducer.toLanceAggregate(aggregate));
    }

    public void testFunctionOutsideTheConsumerIsEmpty() {
        RelBuilder builder = relBuilder();
        builder.scan("lance", "t");
        RexNode power = builder.call(SqlStdOperatorTable.POWER, builder.field("rating"), builder.literal(2));
        builder.project(power, builder.field("rating"));
        RelNode input = builder.build();
        SpecAggregate aggregate = new SpecAggregate(
            input,
            ImmutableBitSet.of(0),
            List.of(call(SqlStdOperatorTable.SUM, input, 1, 1, "m0")),
            List.of(BucketSpec.of(BucketKind.TERMS, "k")),
            List.of(MetricSpec.of(MetricKind.SUM, "m"))
        );
        assertEquals(Optional.empty(), LanceSubstraitProducer.toLanceAggregate(aggregate));
    }

    /**
     * The same tree as the refusal above with an accepted function in
     * the projected key: the refusal there comes from the function, not
     * from the project under the aggregate.
     */
    public void testAcceptedProjectedKeyIsNotRefused() throws Exception {
        RelBuilder builder = relBuilder();
        builder.scan("lance", "t");
        RexNode key = builder.call(
            SqlStdOperatorTable.FLOOR,
            builder.call(
                SqlStdOperatorTable.DIVIDE,
                builder.cast(builder.field("rating"), SqlTypeName.DOUBLE),
                builder.getRexBuilder().makeApproxLiteral(BigDecimal.TEN)
            )
        );
        builder.project(key, builder.field("rating"));
        RelNode input = builder.build();
        SpecAggregate aggregate = new SpecAggregate(
            input,
            ImmutableBitSet.of(0),
            List.of(call(SqlStdOperatorTable.SUM, input, 1, 1, "m0")),
            List.of(BucketSpec.of(BucketKind.HISTOGRAM, "k")),
            List.of(MetricSpec.of(MetricKind.SUM, "m"))
        );

        Plan plan = parse(LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow());
        // The floor rewrite truncates on i64 and casts back to the
        // operand's floating type, so the key's top node is that cast.
        assertTrue(onlyGrouping(plan).hasCast());
        assertEquals(List.of("k0", "n", "m0"), plan.getRelations(0).getRoot().getNamesList());
    }

    /**
     * RelBuilder puts a filter over the projected keys when the
     * condition is added after the projection, so the producer accepts
     * that ordering too. The condition still never travels in the
     * bytes.
     */
    public void testFilterAboveTheProjectedKeysIsAccepted() throws Exception {
        RelBuilder builder = relBuilder();
        builder.scan("lance", "t");
        builder.project(builder.cast(builder.field("rating"), SqlTypeName.BIGINT), builder.field("rating"));
        builder.filter(builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field(0), builder.literal(0L)));
        RelNode input = builder.build();
        assertTrue("the fixture must be a filter over the project", input instanceof Filter);
        assertTrue(((Filter) input).getInput() instanceof Project);
        SpecAggregate aggregate = new SpecAggregate(
            input,
            ImmutableBitSet.of(0),
            List.of(call(SqlStdOperatorTable.SUM, input, 1, 1, "m0")),
            List.of(BucketSpec.of(BucketKind.TERMS, "k")),
            List.of(MetricSpec.of(MetricKind.SUM, "m"))
        );

        Plan plan = parse(LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow());
        AggregateRel rel = aggregateOf(plan);
        assertTrue("the aggregate input must be the scan, the filter travels out of band", rel.getInput().hasRead());
        assertEquals(1, rel.getGroupingsCount());
        assertEquals(List.of("k0", "n", "m0"), plan.getRelations(0).getRoot().getNamesList());
    }

    public void testProjectFoldsIntoTheGroupingExpressions() throws Exception {
        RelBuilder builder = relBuilder();
        builder.scan("lance", "t");
        // A histogram-shaped key: FLOOR(CAST(rating AS DOUBLE) / 10.0) AS BIGINT.
        RexNode asDouble = builder.cast(builder.field("rating"), SqlTypeName.DOUBLE);
        RexNode quotient = builder.call(SqlStdOperatorTable.DIVIDE, asDouble, builder.getRexBuilder().makeApproxLiteral(BigDecimal.TEN));
        RexNode key = builder.cast(builder.call(SqlStdOperatorTable.FLOOR, quotient), SqlTypeName.BIGINT);
        builder.project(key, builder.field("rating"));
        RelNode input = builder.build();
        SpecAggregate aggregate = new SpecAggregate(
            input,
            ImmutableBitSet.of(0),
            List.of(call(SqlStdOperatorTable.SUM, input, 1, 1, "m0")),
            List.of(BucketSpec.of(BucketKind.HISTOGRAM, "h")),
            List.of(MetricSpec.of(MetricKind.SUM, "s"))
        );

        ByteBuffer buffer = LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow();
        assertTrue("the JNI side needs a direct buffer", buffer.isDirect());
        Plan plan = parse(buffer);
        AggregateRel rel = aggregateOf(plan);
        assertTrue("the aggregate input must be the scan, not a project", rel.getInput().hasRead());
        assertEquals(1, rel.getGroupingsCount());
        assertEquals("the key expression is folded inline", 1, rel.getGroupings(0).getGroupingExpressionsCount());
        assertEquals("count(*) plus the sum", 2, rel.getMeasuresCount());
        assertEquals(List.of("k0", "n", "m0"), plan.getRelations(0).getRoot().getNamesList());
        List<String> functions = functionNames(plan);
        assertFalse("floor is rewritten away: " + functions, functions.contains("floor"));
        assertTrue("sum measure: " + functions, functions.contains("sum"));
        assertTrue("count measure: " + functions, functions.contains("count"));
        assertTrue("the floor rewrite compares the truncation: " + functions, functions.contains("gt"));
    }

    public void testLanceDateTruncBecomesDataFusionDateTrunc() throws Exception {
        RelBuilder builder = relBuilder();
        builder.scan("lance", "t");
        RexNode key = builder.call(LANCE_DATE_TRUNC, builder.literal("month"), builder.field("ts"));
        builder.project(key);
        RelNode input = builder.build();
        SpecAggregate aggregate = new SpecAggregate(
            input,
            ImmutableBitSet.of(0),
            List.of(),
            List.of(BucketSpec.of(BucketKind.DATE_HISTOGRAM_CALENDAR, "d")),
            List.of()
        );

        Plan plan = parse(LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow());
        Map<Integer, String> functions = functionsByAnchor(plan);
        assertFalse("the Calcite operator name must not leak: " + functions, functions.containsValue("lance_date_trunc"));
        // The microsecond timestamp key is brought to epoch millis around
        // the truncation: divide(cast_i64(date_trunc('month', ts)), 1000).
        Expression grouping = onlyGrouping(plan);
        assertTrue(grouping.hasScalarFunction());
        assertEquals("divide", functions.get(grouping.getScalarFunction().getFunctionReference()));
        Expression cast = grouping.getScalarFunction().getArguments(0).getValue();
        assertTrue(cast.hasCast());
        Expression truncation = cast.getCast().getInput();
        assertTrue(truncation.hasScalarFunction());
        assertEquals("date_trunc", functions.get(truncation.getScalarFunction().getFunctionReference()));
        assertEquals(2, truncation.getScalarFunction().getArgumentsCount());
        Expression unit = truncation.getScalarFunction().getArguments(0).getValue();
        assertEquals("the unit rides first, as a plain string", "month", unit.getLiteral().getString());
        Expression column = truncation.getScalarFunction().getArguments(1).getValue();
        assertTrue("the value rides second, as a field reference", column.hasSelection());
        assertEquals(5, column.getSelection().getDirectReference().getStructField().getField());
    }

    public void testTimestampCastBecomesUnitArithmetic() throws Exception {
        assertEpochChain("ts", 5, "divide", 1000L);
        assertEpochChain("ts_s", 6, "multiply", 1000L);
        assertEpochChain("ts_ns", 7, "divide", 1_000_000L);
    }

    private void assertEpochChain(String column, int fieldIndex, String function, long factor) throws Exception {
        RelBuilder builder = relBuilder();
        builder.scan("lance", "t");
        builder.project(builder.cast(builder.field(column), SqlTypeName.BIGINT));
        RelNode input = builder.build();
        SpecAggregate aggregate = new SpecAggregate(
            input,
            ImmutableBitSet.of(0),
            List.of(),
            List.of(BucketSpec.of(BucketKind.TERMS, "t")),
            List.of()
        );

        Plan plan = parse(LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow());
        Map<Integer, String> functions = functionsByAnchor(plan);
        Expression grouping = onlyGrouping(plan);
        assertTrue(column + ": the key is the unit arithmetic", grouping.hasScalarFunction());
        assertEquals(column, function, functions.get(grouping.getScalarFunction().getFunctionReference()));
        Expression rawTicks = grouping.getScalarFunction().getArguments(0).getValue();
        assertTrue(column + ": the left argument is the raw tick cast", rawTicks.hasCast());
        assertTrue(column, rawTicks.getCast().getType().hasI64());
        assertTrue(column, rawTicks.getCast().getInput().hasSelection());
        assertEquals(column, fieldIndex, rawTicks.getCast().getInput().getSelection().getDirectReference().getStructField().getField());
        Expression unitFactor = grouping.getScalarFunction().getArguments(1).getValue();
        assertEquals(column, Expression.Literal.LiteralTypeCase.I64, unitFactor.getLiteral().getLiteralTypeCase());
        assertEquals(column, factor, unitFactor.getLiteral().getI64());
    }

    public void testCardinalityAddsTheDistinctGrouping() throws Exception {
        RelNode scan = relBuilder().scan("lance", "t").build();
        SpecAggregate aggregate = new SpecAggregate(
            scan,
            ImmutableBitSet.of(),
            List.of(call(SqlStdOperatorTable.COUNT, scan, 0, 3, "m0")),
            List.of(),
            List.of(MetricSpec.of(MetricKind.CARDINALITY, "c"))
        );

        Plan plan = parse(LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow());
        AggregateRel rel = aggregateOf(plan);
        assertEquals(1, rel.getGroupingsCount());
        assertEquals("the distinct values group the scan", 1, rel.getGroupings(0).getGroupingExpressionsCount());
        assertEquals("count(*) only, cardinality adds no measure", 1, rel.getMeasuresCount());
        assertEquals(List.of("m0_d", "n"), plan.getRelations(0).getRoot().getNamesList());
    }

    public void testPercentilesBinsScanShape() throws Exception {
        RelNode scan = relBuilder().scan("lance", "t").build();
        SpecAggregate aggregate = new SpecAggregate(
            scan,
            ImmutableBitSet.of(),
            List.of(call(SqlStdOperatorTable.COUNT, scan, 0, 1, "m0")),
            List.of(),
            List.of(MetricSpec.of(MetricKind.PERCENTILES, "p"))
        );

        Plan main = parse(LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow());
        assertEquals(List.of("n", "m0_mn", "m0_mx"), main.getRelations(0).getRoot().getNamesList());

        Plan bins = parse(LanceSubstraitProducer.toLancePercentilesBins(aggregate, 0, 0d, 100d, 10).orElseThrow());
        AggregateRel rel = aggregateOf(bins);
        assertEquals(1, rel.getGroupingsCount());
        assertEquals(1, rel.getGroupings(0).getGroupingExpressionsCount());
        assertEquals("the bin count is the only measure", 1, rel.getMeasuresCount());
        assertEquals(List.of("m0_b", "m0_bc"), bins.getRelations(0).getRoot().getNamesList());
    }

    public void testPercentilesBinsRefusesOtherCalls() {
        RelNode scan = relBuilder().scan("lance", "t").build();
        SpecAggregate aggregate = new SpecAggregate(
            scan,
            ImmutableBitSet.of(),
            List.of(call(SqlStdOperatorTable.SUM, scan, 0, 1, "m0")),
            List.of(),
            List.of(MetricSpec.of(MetricKind.SUM, "s"))
        );
        assertEquals(Optional.empty(), LanceSubstraitProducer.toLancePercentilesBins(aggregate, 0, 0d, 100d, 10));
        assertEquals(Optional.empty(), LanceSubstraitProducer.toLancePercentilesBins(aggregate, 5, 0d, 100d, 10));
    }

    public void testStatsExpandIntoTheScanMeasures() throws Exception {
        RelNode scan = relBuilder().scan("lance", "t").build();
        SpecAggregate aggregate = new SpecAggregate(
            scan,
            ImmutableBitSet.of(),
            List.of(call(SqlStdOperatorTable.SUM, scan, 0, 1, "m0")),
            List.of(),
            List.of(MetricSpec.of(MetricKind.EXTENDED_STATS, "e"))
        );

        Plan plan = parse(LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow());
        assertEquals(List.of("n", "m0_c", "m0_s", "m0_mn", "m0_mx", "m0_q"), plan.getRelations(0).getRoot().getNamesList());
        assertEquals(6, aggregateOf(plan).getMeasuresCount());
    }
}

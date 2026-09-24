/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.substrait.proto.Expression;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.lancesql.RexToLanceSql;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Pins the filter producer's contract without a Lance dataset: the
 * message layout (base schema, one boolean expression), the function
 * names DataFusion's consumer resolves, the rewrites the translator's
 * vocabulary needs (flattened connectives, the IN list, the epoch millis
 * comparison, the LIKE escape) and the refusals. The evaluation
 * equivalence against the SQL encoding runs in the integration tests.
 */
public class LanceSubstraitFilterProducerTests extends OpenSearchTestCase {

    private RelBuilder builder;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        builder = PlanTestFixtures.factory().relBuilder(PlanTestFixtures.model().schema()).transform(config -> config.withSimplify(false));
        builder.scan(LancePlannerFactory.SCHEMA_NAME, "idx");
        rowType = builder.peek().getRowType();
    }

    private Optional<ByteBuffer> produce(RexNode condition) {
        return LanceSubstraitFilterProducer.toLanceFilter(condition, rowType, builder.getTypeFactory());
    }

    private static ExtendedExpression parse(ByteBuffer buffer) throws Exception {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return ExtendedExpression.parseFrom(bytes);
    }

    private static Map<Integer, String> functionsByAnchor(ExtendedExpression message) {
        Map<Integer, String> names = new HashMap<>();
        for (SimpleExtensionDeclaration declaration : message.getExtensionsList()) {
            if (declaration.hasExtensionFunction()) {
                names.put(
                    declaration.getExtensionFunction().getFunctionAnchor(),
                    declaration.getExtensionFunction().getName().split(":", 2)[0]
                );
            }
        }
        return names;
    }

    private static Expression rootExpression(ExtendedExpression message) {
        assertEquals(1, message.getReferredExprCount());
        assertEquals(List.of("filter"), message.getReferredExpr(0).getOutputNamesList());
        assertTrue(message.getReferredExpr(0).hasExpression());
        return message.getReferredExpr(0).getExpression();
    }

    private static String functionName(ExtendedExpression message, Expression expression) {
        assertTrue("a scalar function: " + expression, expression.hasScalarFunction());
        return functionsByAnchor(message).get(expression.getScalarFunction().getFunctionReference());
    }

    private static List<Expression> arguments(Expression call) {
        List<Expression> arguments = new ArrayList<>();
        for (io.substrait.proto.FunctionArgument argument : call.getScalarFunction().getArgumentsList()) {
            arguments.add(argument.getValue());
        }
        return arguments;
    }

    private static int fieldIndex(Expression expression) {
        assertTrue("a field reference: " + expression, expression.hasSelection());
        return expression.getSelection().getDirectReference().getStructField().getField();
    }

    private RexNode field(String name) {
        return builder.field(name);
    }

    private RexNode equalsText(String column, String value) {
        return builder.call(SqlStdOperatorTable.EQUALS, field(column), builder.literal(value));
    }

    public void testBaseSchemaIsTheScanRowTypeInOrder() throws Exception {
        ExtendedExpression message = parse(produce(equalsText("category", "c0")).orElseThrow());
        assertEquals(rowType.getFieldNames(), message.getBaseSchema().getNamesList());
        assertEquals(rowType.getFieldCount(), message.getBaseSchema().getStruct().getTypesCount());
    }

    public void testEqualityOnAStringColumn() throws Exception {
        ExtendedExpression message = parse(produce(equalsText("category", "c0")).orElseThrow());
        Expression root = rootExpression(message);
        assertEquals("equal", functionName(message, root));
        List<Expression> arguments = arguments(root);
        assertEquals(rowType.getFieldNames().indexOf("category"), fieldIndex(arguments.get(0)));
        assertTrue(arguments.get(1).hasLiteral());
        assertEquals("c0", arguments.get(1).getLiteral().getFixedChar());
    }

    public void testWideningCastsAroundTheColumnAreDropped() throws Exception {
        RexNode condition = builder.call(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            builder.cast(field("rating"), SqlTypeName.BIGINT),
            builder.literal(3L)
        );
        ExtendedExpression message = parse(produce(condition).orElseThrow());
        Expression root = rootExpression(message);
        assertEquals("gte", functionName(message, root));
        assertEquals(rowType.getFieldNames().indexOf("rating"), fieldIndex(arguments(root).get(0)));
        assertEquals(3L, arguments(root).get(1).getLiteral().getI32());
    }

    public void testSameKindConnectivesFlattenIntoOneCall() throws Exception {
        RexNode a = equalsText("category", "a");
        RexNode b = builder.isNotNull(field("rating"));
        RexNode c = builder.call(SqlStdOperatorTable.EQUALS, field("flag"), builder.literal(true));
        RexNode condition = builder.call(SqlStdOperatorTable.AND, a, builder.call(SqlStdOperatorTable.AND, b, c));
        ExtendedExpression message = parse(produce(condition).orElseThrow());
        Expression root = rootExpression(message);
        assertEquals("and", functionName(message, root));
        List<Expression> arguments = arguments(root);
        assertEquals(3, arguments.size());
        assertEquals("equal", functionName(message, arguments.get(0)));
        assertEquals("is_not_null", functionName(message, arguments.get(1)));
        assertEquals("equal", functionName(message, arguments.get(2)));
    }

    public void testDisjunctionOfEqualitiesOnOneColumnBecomesAnInList() throws Exception {
        RexNode condition = builder.call(
            SqlStdOperatorTable.OR,
            builder.call(SqlStdOperatorTable.OR, equalsText("category", "a"), equalsText("category", "b")),
            equalsText("category", "c")
        );
        ExtendedExpression message = parse(produce(condition).orElseThrow());
        Expression root = rootExpression(message);
        assertTrue("an IN list: " + root, root.hasSingularOrList());
        assertEquals(rowType.getFieldNames().indexOf("category"), fieldIndex(root.getSingularOrList().getValue()));
        assertEquals(3, root.getSingularOrList().getOptionsCount());
        assertEquals("b", root.getSingularOrList().getOptions(1).getLiteral().getFixedChar());
    }

    public void testDisjunctionAcrossColumnsStaysAnOr() throws Exception {
        RexNode condition = builder.call(SqlStdOperatorTable.OR, equalsText("category", "a"), builder.isNotNull(field("rating")));
        ExtendedExpression message = parse(produce(condition).orElseThrow());
        Expression root = rootExpression(message);
        assertEquals("or", functionName(message, root));
        assertEquals(2, arguments(root).size());
    }

    public void testMustNotSpellsNotIsTrue() throws Exception {
        RexNode condition = builder.call(SqlStdOperatorTable.NOT, builder.call(SqlStdOperatorTable.IS_TRUE, equalsText("category", "a")));
        ExtendedExpression message = parse(produce(condition).orElseThrow());
        Expression root = rootExpression(message);
        assertEquals("not", functionName(message, root));
        Expression isTrue = arguments(root).get(0);
        assertEquals("is_true", functionName(message, isTrue));
        assertEquals("equal", functionName(message, arguments(isTrue).get(0)));
    }

    public void testEpochMillisComparisonBecomesATimestampLiteral() throws Exception {
        long millis = 1_700_000_000_000L;
        RexNode condition = builder.call(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            builder.call(SqlLibraryOperators.UNIX_MILLIS, field("ts")),
            builder.literal(millis)
        );
        ExtendedExpression message = parse(produce(condition).orElseThrow());
        Expression root = rootExpression(message);
        assertEquals("gte", functionName(message, root));
        List<Expression> arguments = arguments(root);
        assertEquals(rowType.getFieldNames().indexOf("ts"), fieldIndex(arguments.get(0)));
        assertTrue(arguments.get(1).getLiteral().hasPrecisionTimestamp());
        assertEquals(3, arguments.get(1).getLiteral().getPrecisionTimestamp().getPrecision());
        assertEquals(millis, arguments.get(1).getLiteral().getPrecisionTimestamp().getValue());
    }

    public void testDateColumnEpochMillisLooksThroughTheTimestampCast() throws Exception {
        long millis = 86_400_000L * 20_000L;
        RexNode condition = builder.call(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            builder.call(SqlLibraryOperators.UNIX_MILLIS, builder.cast(field("day"), SqlTypeName.TIMESTAMP)),
            builder.literal(millis)
        );
        ExtendedExpression message = parse(produce(condition).orElseThrow());
        Expression root = rootExpression(message);
        assertEquals("lte", functionName(message, root));
        assertEquals(rowType.getFieldNames().indexOf("day"), fieldIndex(arguments(root).get(0)));
        assertEquals(millis, arguments(root).get(1).getLiteral().getPrecisionTimestamp().getValue());
    }

    public void testLikeCarriesTheEscapeOperandAsAStringLiteral() throws Exception {
        RexNode condition = builder.call(SqlStdOperatorTable.LIKE, field("category"), builder.literal("c\\_%"), builder.literal("\\"));
        ExtendedExpression message = parse(produce(condition).orElseThrow());
        Expression root = rootExpression(message);
        assertEquals("like", functionName(message, root));
        List<Expression> arguments = arguments(root);
        assertEquals(3, arguments.size());
        assertEquals("c\\_%", arguments.get(1).getLiteral().getString());
        assertEquals("\\", arguments.get(2).getLiteral().getString());
    }

    public void testIlikeRegexpLikeStartsWithAndLower() throws Exception {
        RexNode ilike = builder.call(SqlLibraryOperators.ILIKE, field("category"), builder.literal("C%"), builder.literal("\\"));
        ExtendedExpression ilikeMessage = parse(produce(ilike).orElseThrow());
        assertEquals("ilike", functionName(ilikeMessage, rootExpression(ilikeMessage)));

        RexNode rlike = builder.call(SqlLibraryOperators.RLIKE, field("category"), builder.literal("^(?:c.*)$"));
        ExtendedExpression rlikeMessage = parse(produce(rlike).orElseThrow());
        assertEquals("regexp_like", functionName(rlikeMessage, rootExpression(rlikeMessage)));

        RexNode prefix = builder.call(
            SqlLibraryOperators.STARTS_WITH,
            builder.call(SqlStdOperatorTable.LOWER, field("category")),
            builder.call(SqlStdOperatorTable.LOWER, builder.literal("C"))
        );
        ExtendedExpression prefixMessage = parse(produce(prefix).orElseThrow());
        Expression root = rootExpression(prefixMessage);
        assertEquals("starts_with", functionName(prefixMessage, root));
        assertEquals("lower", functionName(prefixMessage, arguments(root).get(0)));
    }

    public void testArithmeticTheSqlPrinterRefusesIsEncoded() throws Exception {
        RexNode condition = builder.call(
            SqlStdOperatorTable.EQUALS,
            builder.call(SqlStdOperatorTable.PLUS, field("rating"), builder.literal(1)),
            builder.literal(2)
        );
        assertTrue("the SQL printer refuses arithmetic", RexToLanceSql.print(condition, rowType).isEmpty());
        ExtendedExpression message = parse(produce(condition).orElseThrow());
        Expression root = rootExpression(message);
        assertEquals("equal", functionName(message, root));
        assertEquals("add", functionName(message, arguments(root).get(0)));
    }

    public void testStructChildReferenceIsRefused() {
        RelBuilder nested = PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.queryModel().schema())
            .transform(config -> config.withSimplify(false));
        nested.scan(LancePlannerFactory.SCHEMA_NAME, "idx");
        RelNode scan = nested.peek();
        RexNode child = nested.getRexBuilder().makeFieldAccess(nested.field("meta"), "region", true);
        RexNode condition = nested.call(SqlStdOperatorTable.EQUALS, child, nested.literal("eu"));
        assertTrue(
            "the SQL printer spells the dotted path",
            RexToLanceSql.print(condition, scan.getRowType()).orElseThrow().startsWith("meta.region")
        );
        assertTrue(LanceSubstraitFilterProducer.toLanceFilter(condition, scan.getRowType(), nested.getTypeFactory()).isEmpty());
    }

    public void testNonBooleanExpressionIsRefused() {
        assertTrue(produce(builder.call(SqlStdOperatorTable.PLUS, field("rating"), builder.literal(1))).isEmpty());
    }

    public void testFunctionOutsideTheVocabularyIsRefused() {
        RexNode condition = builder.call(
            SqlStdOperatorTable.EQUALS,
            builder.call(SqlStdOperatorTable.CHAR_LENGTH, field("category")),
            builder.literal(2)
        );
        assertTrue(produce(condition).isEmpty());
    }

    public void testBytesAreADirectBuffer() {
        ByteBuffer bytes = produce(equalsText("category", "c0")).orElseThrow();
        assertTrue("the JNI side reads a direct buffer", bytes.isDirect());
        assertTrue(bytes.remaining() > 0);
    }
}

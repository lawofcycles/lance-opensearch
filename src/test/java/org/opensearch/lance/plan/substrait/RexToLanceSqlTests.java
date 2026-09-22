/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pins the exact SQL string of every construct {@link RexToLanceSql}
 * prints, plus the escaping edge cases and the refusals that must
 * leave a {@code Filter} in place.
 */
public class RexToLanceSqlTests extends OpenSearchTestCase {

    private static final Schema SCHEMA = new Schema(
        List.of(
            field("rating", new ArrowType.Int(32, true)),
            field("price", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            field("category", new ArrowType.Utf8()),
            field("body", new ArrowType.Utf8()),
            field("flag", new ArrowType.Bool()),
            field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
            field("day", new ArrowType.Date(DateUnit.DAY)),
            new Field("meta", new FieldType(true, new ArrowType.Struct(), null), List.of(field("region", new ArrowType.Utf8()))),
            field("we\"ird", new ArrowType.Utf8())
        )
    );

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    private RelBuilder builder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LanceSchemas.IndexModel model = LanceSchemas.model("idx", SCHEMA, Map.of(), () -> 16L);
        builder = new LancePlannerFactory(1L << 30, 1L << 30).relBuilder(model.schema()).transform(config -> config.withSimplify(false));
        builder.scan(LancePlannerFactory.SCHEMA_NAME, "idx");
    }

    private RelDataType rowType() {
        return builder.peek().getRowType();
    }

    private String printed(RexNode node) {
        Optional<String> sql = RexToLanceSql.print(node, rowType());
        assertTrue("expected a printable predicate: " + node, sql.isPresent());
        return sql.get();
    }

    private RexNode eq(String column, Object value) {
        return builder.call(SqlStdOperatorTable.EQUALS, builder.field(column), builder.literal(value));
    }

    public void testEqualsOnAStringColumn() {
        assertEquals("\"category\" = 'c0'", printed(eq("category", "c0")));
    }

    public void testSingleQuoteInLiteralDoubles() {
        assertEquals("\"category\" = 'it''s'", printed(eq("category", "it's")));
    }

    public void testDoubleQuoteInIdentifierDoubles() {
        assertEquals("\"we\"\"ird\" = 'x'", printed(eq("we\"ird", "x")));
    }

    public void testCastsAroundTheColumnUnwrap() {
        RexNode compared = builder.call(
            SqlStdOperatorTable.EQUALS,
            builder.cast(builder.field("rating"), SqlTypeName.BIGINT),
            builder.literal(100L)
        );
        assertEquals("\"rating\" = 100", printed(compared));
    }

    public void testComparisonOperators() {
        assertEquals(
            "\"price\" > 1.5",
            printed(builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("price"), builder.literal(1.5d)))
        );
        assertEquals("\"rating\" <> 3", printed(builder.call(SqlStdOperatorTable.NOT_EQUALS, builder.field("rating"), builder.literal(3))));
        assertEquals(
            "\"rating\" <= 3",
            printed(builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, builder.field("rating"), builder.literal(3)))
        );
    }

    public void testBooleanLiterals() {
        assertEquals("\"flag\" = true", printed(eq("flag", true)));
        assertEquals("true", printed(builder.literal(true)));
        assertEquals("false", printed(builder.literal(false)));
    }

    public void testConjunctionFlattensWithOneParenthesis() {
        RexNode and = builder.call(
            SqlStdOperatorTable.AND,
            builder.call(SqlStdOperatorTable.AND, eq("category", "a"), eq("flag", true)),
            eq("body", "b")
        );
        assertEquals("(\"category\" = 'a' AND \"flag\" = true AND \"body\" = 'b')", printed(and));
    }

    public void testDisjunctionOfMixedComparisonsKeepsOr() {
        RexNode or = builder.call(
            SqlStdOperatorTable.OR,
            eq("category", "a"),
            builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("rating"), builder.literal(1))
        );
        assertEquals("(\"category\" = 'a' OR \"rating\" > 1)", printed(or));
    }

    public void testDisjunctionOfEqualitiesOnOneColumnCollapsesToIn() {
        RexNode or = builder.call(
            SqlStdOperatorTable.OR,
            builder.call(SqlStdOperatorTable.OR, eq("category", "a"), eq("category", "b")),
            eq("category", "c")
        );
        assertEquals("\"category\" IN ('a', 'b', 'c')", printed(or));
    }

    public void testDisjunctionAcrossColumnsDoesNotCollapse() {
        RexNode or = builder.call(SqlStdOperatorTable.OR, eq("category", "a"), eq("body", "b"));
        assertEquals("(\"category\" = 'a' OR \"body\" = 'b')", printed(or));
    }

    public void testNotOverIsTruePrintsOnePairOfParentheses() {
        RexNode negated = builder.call(SqlStdOperatorTable.NOT, builder.call(SqlStdOperatorTable.IS_TRUE, eq("category", "a")));
        assertEquals("NOT ((\"category\" = 'a') IS TRUE)", printed(negated));
    }

    public void testIsNullAndIsNotNull() {
        assertEquals("\"category\" IS NULL", printed(builder.isNull(builder.field("category"))));
        assertEquals("\"category\" IS NOT NULL", printed(builder.isNotNull(builder.field("category"))));
    }

    public void testLikeWithEscapeClause() {
        RexNode like = builder.call(SqlStdOperatorTable.LIKE, builder.field("body"), builder.literal("a%c_"), builder.literal("\\"));
        assertEquals("\"body\" LIKE 'a%c_' ESCAPE '\\'", printed(like));
    }

    public void testLikePatternWithEscapedMetacharacters() {
        RexNode like = builder.call(
            SqlStdOperatorTable.LIKE,
            builder.field("body"),
            builder.literal("50\\%\\_%\\\\"),
            builder.literal("\\")
        );
        assertEquals("\"body\" LIKE '50\\%\\_%\\\\' ESCAPE '\\'", printed(like));
    }

    public void testIlike() {
        RexNode ilike = builder.call(SqlLibraryOperators.ILIKE, builder.field("body"), builder.literal("A%"), builder.literal("\\"));
        assertEquals("\"body\" ILIKE 'A%' ESCAPE '\\'", printed(ilike));
    }

    public void testRlikePrintsAsRegexpLike() {
        RexNode rlike = builder.call(SqlLibraryOperators.RLIKE, builder.field("body"), builder.literal("^(?:a.*)$"));
        assertEquals("regexp_like(\"body\", '^(?:a.*)$')", printed(rlike));
    }

    public void testRegexpPatternWithItsOwnAnchorPassesThrough() {
        RexNode rlike = builder.call(SqlLibraryOperators.RLIKE, builder.field("body"), builder.literal("^(?:^abc$)$"));
        assertEquals("regexp_like(\"body\", '^(?:^abc$)$')", printed(rlike));
    }

    public void testStartsWith() {
        RexNode call = builder.call(SqlLibraryOperators.STARTS_WITH, builder.field("body"), builder.literal("ab"));
        assertEquals("starts_with(\"body\", 'ab')", printed(call));
    }

    public void testCaseInsensitivePrefixLowersBothSides() {
        RexNode call = builder.call(
            SqlLibraryOperators.STARTS_WITH,
            builder.call(SqlStdOperatorTable.LOWER, builder.field("body")),
            builder.call(SqlStdOperatorTable.LOWER, builder.literal("AB"))
        );
        assertEquals("starts_with(lower(\"body\"), lower('AB'))", printed(call));
    }

    public void testUnixMillisComparisonPrintsToTimestampMillis() {
        RexNode bound = builder.call(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            builder.call(SqlLibraryOperators.UNIX_MILLIS, builder.field("ts")),
            builder.literal(1706745600000L)
        );
        assertEquals("\"ts\" >= to_timestamp_millis(1706745600000)", printed(bound));
    }

    public void testUnixMillisOverTheDateCastUnwrapsToTheColumn() {
        RexNode bound = builder.call(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            builder.call(SqlLibraryOperators.UNIX_MILLIS, builder.cast(builder.field("day"), SqlTypeName.TIMESTAMP)),
            builder.literal(1709337599999L)
        );
        assertEquals("\"day\" <= to_timestamp_millis(1709337599999)", printed(bound));
    }

    public void testStructChildPrintsAsDottedQuotedPath() {
        RexNode access = builder.getRexBuilder().makeFieldAccess(builder.field("meta"), "region", true);
        RexNode compared = builder.call(SqlStdOperatorTable.EQUALS, access, builder.literal("east"));
        assertEquals("\"meta\".\"region\" = 'east'", printed(compared));
    }

    public void testArithmeticIsUnprintable() {
        RexNode sum = builder.call(SqlStdOperatorTable.PLUS, builder.field("rating"), builder.literal(1));
        RexNode compared = builder.call(SqlStdOperatorTable.EQUALS, sum, builder.literal(2));
        assertEquals(Optional.empty(), RexToLanceSql.print(compared, rowType()));
    }

    public void testCaseIsUnprintable() {
        RexNode masked = builder.call(SqlStdOperatorTable.CASE, eq("category", "a"), builder.literal(1L), builder.literal(0L));
        assertEquals(Optional.empty(), RexToLanceSql.print(masked, rowType()));
    }

    public void testUnixMillisOutsideAComparisonIsUnprintable() {
        RexNode millis = builder.call(SqlLibraryOperators.UNIX_MILLIS, builder.field("ts"));
        RexNode compared = builder.call(SqlStdOperatorTable.EQUALS, millis, builder.field("rating"));
        assertEquals(Optional.empty(), RexToLanceSql.print(compared, rowType()));
    }

    public void testUnprintableBranchInsideAConjunctionRefusesTheWholeTree() {
        RexNode and = builder.call(
            SqlStdOperatorTable.AND,
            eq("category", "a"),
            builder.call(
                SqlStdOperatorTable.EQUALS,
                builder.call(SqlStdOperatorTable.PLUS, builder.field("rating"), builder.literal(1)),
                builder.literal(2)
            )
        );
        assertEquals(Optional.empty(), RexToLanceSql.print(and, rowType()));
    }
}

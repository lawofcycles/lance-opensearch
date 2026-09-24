/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.lance.Dataset;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.lance.plan.lancesql.RexToLanceSql;
import org.opensearch.lance.query.LanceScanFilter;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Runs the producer's bytes through a real Lance dataset scan and checks
 * they select the same rows as the SQL spelling of the same predicate:
 * the evidence that Lance's Substrait consumer accepts the message
 * layout, the function names and the literal shapes the producer emits
 * for every construct the predicate translator uses.
 *
 * <p>Fixture: {@link LanceTableFactory#writeMultiFragmentTable} with
 * twelve rows in three fragments of four ({@code id = i}, {@code body}
 * {@code "hello lance i"} for even {@code i} and {@code "quick brown
 * fox i"} for odd, {@code title} {@code "sunny morning i"} for even and
 * {@code "cloudy morning i"} for odd, a vector column the predicates
 * never touch but the base schema must still describe), and
 * {@link LanceTableFactory#writeDatedTable} for a microsecond timestamp
 * column.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceSubstraitFilterScanTests extends OpenSearchTestCase {

    private static final List<Integer> ALL_FRAGMENTS = List.of(0, 1, 2);

    private String uri;
    private String datedUri;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratch = createTempDir();
        uri = LanceTableFactory.writeMultiFragmentTable(scratch, "substrait-filter-" + getTestName(), 12, 4);
        datedUri = LanceTableFactory.writeDatedTable(scratch, "substrait-dated-" + getTestName());
    }

    private static RelBuilder builderOver(Dataset dataset) {
        Schema schema = dataset.getSchema();
        LanceSchemas.IndexModel model = LanceSchemas.model("t", schema, Map.of(), () -> 12L);
        RelBuilder builder = new LancePlannerFactory(1L << 30, 1L << 30).relBuilder(model.schema())
            .transform(config -> config.withSimplify(false));
        builder.scan(LancePlannerFactory.SCHEMA_NAME, "t");
        return builder;
    }

    /** The count of the predicate through its Substrait bytes, asserting the SQL spelling counts the same. */
    private static long countBothWays(Dataset dataset, RelBuilder builder, RexNode condition, List<Integer> fragments) throws Exception {
        ByteBuffer bytes = LanceSubstraitFilterProducer.toLanceFilter(condition, builder.peek().getRowType(), builder.getTypeFactory())
            .orElseThrow(() -> new AssertionError("the producer refused " + condition));
        byte[] copy = new byte[bytes.remaining()];
        bytes.duplicate().get(copy);
        long viaSubstrait = PlanExecutor.countScalarFilter(
            dataset,
            LanceScanFilter.substrait(copy, null),
            fragments,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE,
            LanceCancellation.NONE
        ).value();
        String sql = RexToLanceSql.print(condition, builder.peek().getRowType()).orElseThrow();
        long viaSql = PlanExecutor.countScalarFilter(dataset, sql, fragments, SearchContext.TRACK_TOTAL_HITS_ACCURATE).value();
        assertEquals("Substrait and SQL disagree on " + sql, viaSql, viaSubstrait);
        return viaSubstrait;
    }

    private static RexNode idCompare(RelBuilder b, SqlOperator op, long value) {
        return b.call(op, b.cast(b.field("id"), SqlTypeName.BIGINT), b.literal(value));
    }

    public void testComparisonsAndConnectives() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            RelBuilder b = builderOver(dataset);
            assertEquals(8L, countBothWays(dataset, b, idCompare(b, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, 4L), ALL_FRAGMENTS));
            assertEquals(4L, countBothWays(dataset, b, idCompare(b, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, 4L), List.of(1)));
            RexNode range = b.call(
                SqlStdOperatorTable.AND,
                idCompare(b, SqlStdOperatorTable.GREATER_THAN, 2L),
                idCompare(b, SqlStdOperatorTable.LESS_THAN_OR_EQUAL, 6L)
            );
            assertEquals(4L, countBothWays(dataset, b, range, ALL_FRAGMENTS));
            RexNode either = b.call(
                SqlStdOperatorTable.OR,
                idCompare(b, SqlStdOperatorTable.LESS_THAN, 3L),
                b.call(SqlStdOperatorTable.AND, b.isNotNull(b.field("title")), idCompare(b, SqlStdOperatorTable.GREATER_THAN, 9L))
            );
            assertEquals(5L, countBothWays(dataset, b, either, ALL_FRAGMENTS));
            RexNode notEqual = idCompare(b, SqlStdOperatorTable.NOT_EQUALS, 0L);
            assertEquals(11L, countBothWays(dataset, b, notEqual, ALL_FRAGMENTS));
        }
    }

    public void testInListAndMustNot() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            RelBuilder b = builderOver(dataset);
            RexNode in = b.call(
                SqlStdOperatorTable.OR,
                b.call(SqlStdOperatorTable.OR, idCompare(b, SqlStdOperatorTable.EQUALS, 1L), idCompare(b, SqlStdOperatorTable.EQUALS, 5L)),
                idCompare(b, SqlStdOperatorTable.EQUALS, 9L)
            );
            assertEquals(3L, countBothWays(dataset, b, in, ALL_FRAGMENTS));
            RexNode mustNot = b.call(
                SqlStdOperatorTable.NOT,
                b.call(SqlStdOperatorTable.IS_TRUE, b.call(SqlStdOperatorTable.EQUALS, b.field("title"), b.literal("sunny morning 0")))
            );
            assertEquals(11L, countBothWays(dataset, b, mustNot, ALL_FRAGMENTS));
        }
    }

    public void testStringPredicates() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            RelBuilder b = builderOver(dataset);
            RexNode like = b.call(SqlStdOperatorTable.LIKE, b.field("body"), b.literal("hello%"), b.literal("\\"));
            assertEquals(6L, countBothWays(dataset, b, like, ALL_FRAGMENTS));
            RexNode ilike = b.call(SqlLibraryOperators.ILIKE, b.field("body"), b.literal("HELLO\\_LANCE%"), b.literal("\\"));
            assertEquals(0L, countBothWays(dataset, b, ilike, ALL_FRAGMENTS));
            RexNode ilikeSpace = b.call(SqlLibraryOperators.ILIKE, b.field("body"), b.literal("HELLO LANCE%"), b.literal("\\"));
            assertEquals(6L, countBothWays(dataset, b, ilikeSpace, ALL_FRAGMENTS));
            RexNode regexp = b.call(SqlLibraryOperators.RLIKE, b.field("body"), b.literal("^(?:quick.*[13579])$"));
            assertEquals(6L, countBothWays(dataset, b, regexp, ALL_FRAGMENTS));
            RexNode prefix = b.call(
                SqlLibraryOperators.STARTS_WITH,
                b.call(SqlStdOperatorTable.LOWER, b.field("title")),
                b.call(SqlStdOperatorTable.LOWER, b.literal("Sunny"))
            );
            assertEquals(6L, countBothWays(dataset, b, prefix, ALL_FRAGMENTS));
            RexNode equalText = b.call(SqlStdOperatorTable.EQUALS, b.field("title"), b.literal("cloudy morning 3"));
            assertEquals(1L, countBothWays(dataset, b, equalText, ALL_FRAGMENTS));
        }
    }

    public void testDeepBoolTree() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            RelBuilder b = builderOver(dataset);
            // Alternating conjunctions and disjunctions twelve levels
            // deep over two columns, the shape a nested bool query
            // produces; every level keeps the rows with id >= 2.
            RexNode tree = idCompare(b, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, 2L);
            for (int level = 0; level < 12; level++) {
                RexNode clause = level % 2 == 0
                    ? b.call(SqlStdOperatorTable.LIKE, b.field("body"), b.literal("%" + (level % 3) + "%"), b.literal("\\"))
                    : idCompare(b, SqlStdOperatorTable.NOT_EQUALS, 100L + level);
                tree = level % 2 == 0 ? b.call(SqlStdOperatorTable.AND, tree, clause) : b.call(SqlStdOperatorTable.OR, tree, clause);
            }
            // The outermost OR (level 11) carries id <> 111, which every
            // row satisfies, so the tree matches every row.
            assertEquals(12L, countBothWays(dataset, b, tree, ALL_FRAGMENTS));
        }
    }

    public void testTimestampBounds() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(datedUri, StorageOptions.empty())) {
            RelBuilder b = builderOver(dataset);
            long march = Instant.parse("2024-03-01T00:00:00Z").toEpochMilli();
            long april = Instant.parse("2024-04-01T00:00:00Z").toEpochMilli();
            RexNode millis = b.call(SqlLibraryOperators.UNIX_MILLIS, b.field("ts"));
            RexNode inMarch = b.call(
                SqlStdOperatorTable.AND,
                b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, millis, b.literal(march)),
                b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, millis, b.literal(april - 1L))
            );
            assertEquals(2L, countBothWays(dataset, b, inMarch, List.of(0)));
            RexNode onOrAfterApril = b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, millis, b.literal(april));
            assertEquals(2L, countBothWays(dataset, b, onOrAfterApril, List.of(0)));
        }
    }
}

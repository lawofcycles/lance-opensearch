/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.LanceTopK;
import org.opensearch.lance.plan.rel.PushedOperation.PushedTopK;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * The top-k pushdown rule over the logical chains it matches: a score
 * sort folds together with the FTS it scores with, a column sort folds
 * a scalar chain with the Lance orderings (and the search_after cursor
 * as a strict SQL bound), the hit shape folds with the top-k and hands
 * the scan its row type, and the mixed, misplaced and untypeable
 * shapes leave the plan alone for the Lucene collector.
 */
public class PushSortLimitIntoLanceScanTests extends OpenSearchTestCase {

    private LanceTableScan scan() {
        return (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
    }

    private static LanceFtsMatch ftsOver(RelNode input) {
        return new LanceFtsMatch(
            input.getCluster(),
            input.getCluster().traitSetOf(Convention.NONE),
            input,
            LanceFtsMatch.Kind.MATCH,
            List.of("body"),
            new LanceMatchQueryBuilder("body", "hello")
        );
    }

    private static LogicalFilter filterOver(LanceTableScan scan) {
        RexNode condition = scan.getCluster()
            .getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.IS_NOT_NULL,
                scan.getCluster().getRexBuilder().makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1)
            );
        return LogicalFilter.create(scan, condition);
    }

    private static LanceTopK topK(RelNode input, List<RelFieldCollation> collations, List<Object> searchAfter) {
        return new LanceTopK(input.getCluster(), input.getCluster().traitSetOf(Convention.NONE), input, collations, 10, 0, searchAfter);
    }

    private static RelNode hep(RelNode root) {
        HepProgramBuilder program = new HepProgramBuilder();
        for (PushSortLimitIntoLanceScan rule : PushSortLimitIntoLanceScan.rules()) {
            program.addRuleInstance(rule);
        }
        HepPlanner planner = new HepPlanner(program.build());
        planner.setRoot(root);
        return planner.findBestExp();
    }

    private static int fieldIndex(RelNode node, String name) {
        return node.getRowType().getFieldNames().indexOf(name);
    }

    public void testScoreSortFoldsWithTheFts() {
        LanceFtsMatch fts = ftsOver(filterOver(scan()));
        RelFieldCollation score = new RelFieldCollation(
            fieldIndex(fts, "_score"),
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        LanceTableScan folded = (LanceTableScan) hep(topK(fts, List.of(score), null));
        PushedTopK pushed = folded.pushedTopK().orElseThrow();
        assertTrue("a score page needs no orderings", pushed.toScanOrderings().isEmpty());
        assertNull(pushed.cursorSql());
        assertTrue(folded.pushedFts().isPresent());
        assertEquals("the filter rides the FTS as its prefilter", "rating IS NOT NULL", folded.pushedFts().orElseThrow().filterSql());
    }

    public void testValueSortFoldsWithTheLanceOrderings() {
        LogicalFilter filter = filterOver(scan());
        RelFieldCollation rating = new RelFieldCollation(
            fieldIndex(filter, "rating"),
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        RelFieldCollation category = new RelFieldCollation(
            fieldIndex(filter, "category"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST
        );
        LanceTableScan folded = (LanceTableScan) hep(topK(filter, List.of(rating, category), null));
        assertEquals("rating IS NOT NULL", folded.pushedFilter().orElseThrow().sql());
        PushedTopK pushed = folded.pushedTopK().orElseThrow();
        assertEquals(2, pushed.toScanOrderings().size());
        assertEquals("rating", pushed.toScanOrderings().get(0).getColumnName());
        assertFalse(pushed.toScanOrderings().get(0).isAscending());
        assertFalse(pushed.toScanOrderings().get(0).isNullFirst());
        assertEquals("category", pushed.toScanOrderings().get(1).getColumnName());
        assertTrue(pushed.toScanOrderings().get(1).isAscending());
        assertTrue(pushed.toScanOrderings().get(1).isNullFirst());
        assertEquals(10, pushed.fetch());
        assertNull(pushed.cursorSql());
    }

    public void testUnprintableFilterPasses() {
        LanceTableScan scan = scan();
        RexNode unprintable = scan.getCluster()
            .getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.EQUALS,
                scan.getCluster()
                    .getRexBuilder()
                    .makeCall(
                        SqlStdOperatorTable.PLUS,
                        scan.getCluster().getRexBuilder().makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1),
                        scan.getCluster().getRexBuilder().makeExactLiteral(java.math.BigDecimal.ONE)
                    ),
                scan.getCluster().getRexBuilder().makeExactLiteral(java.math.BigDecimal.TEN)
            );
        LogicalFilter filter = LogicalFilter.create(scan, unprintable);
        RelFieldCollation rating = new RelFieldCollation(
            fieldIndex(filter, "rating"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        assertTrue(hep(topK(filter, List.of(rating), null)) instanceof LanceTopK);
    }

    public void testMixedScoreAndValueSortPasses() {
        LanceFtsMatch fts = ftsOver(scan());
        RelFieldCollation score = new RelFieldCollation(
            fieldIndex(fts, "_score"),
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        RelFieldCollation price = new RelFieldCollation(
            fieldIndex(fts, "price"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        RelNode best = hep(topK(fts, List.of(score, price), null));
        assertTrue("the mixed page stays on the Lucene collector: " + best, best instanceof LanceTopK);
    }

    public void testValueSortOverAnFtsPasses() {
        LanceFtsMatch fts = ftsOver(scan());
        RelFieldCollation price = new RelFieldCollation(
            fieldIndex(fts, "price"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        RelNode best = hep(topK(fts, List.of(price), null));
        assertTrue("a column sort over an FTS stays on Lucene: " + best, best instanceof LanceTopK);
    }

    public void testUnorderableColumnPasses() {
        // A list column has no Lance ordering; the collation resolves
        // (the row type carries the column) but toOrderings refuses.
        Schema withList = new Schema(
            java.util.stream.Stream.concat(
                PlanTestFixtures.SCHEMA.getFields().stream(),
                java.util.stream.Stream.of(
                    new Field(
                        "tags",
                        FieldType.nullable(new ArrowType.List()),
                        List.of(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null))
                    )
                )
            ).toList()
        );
        LanceSchemas.IndexModel model = LanceSchemas.model("idx", withList, java.util.Map.of(), () -> 512L);
        LanceTableScan scan = (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(model.schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
        RelFieldCollation tags = new RelFieldCollation(
            fieldIndex(scan, "tags"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        assertTrue(hep(topK(scan, List.of(tags), null)) instanceof LanceTopK);
    }

    public void testCursorFoldsAsAStrictBound() {
        LogicalFilter filter = filterOver(scan());
        RelFieldCollation rating = new RelFieldCollation(
            fieldIndex(filter, "rating"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        LanceTableScan folded = (LanceTableScan) hep(topK(filter, List.of(rating), List.of(100)));
        assertEquals("(rating > 100 OR rating IS NULL)", folded.pushedTopK().orElseThrow().cursorSql());
        assertEquals("rating IS NOT NULL", folded.pushedFilter().orElseThrow().sql());

        RelFieldCollation descFirst = new RelFieldCollation(
            fieldIndex(filter, "rating"),
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.FIRST
        );
        LanceTableScan desc = (LanceTableScan) hep(topK(filterOver(scan()), List.of(descFirst), List.of(100)));
        assertEquals("rating < 100", desc.pushedTopK().orElseThrow().cursorSql());
    }

    public void testCursorOnAKeywordAndATimestampColumn() {
        LanceTableScan scan = scan();
        RelFieldCollation category = new RelFieldCollation(
            fieldIndex(scan, "category"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        LanceTableScan keyword = (LanceTableScan) hep(topK(scan, List.of(category), List.of("c1")));
        assertEquals("(category > 'c1' OR category IS NULL)", keyword.pushedTopK().orElseThrow().cursorSql());

        RelFieldCollation ts = new RelFieldCollation(
            fieldIndex(scan, "ts"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        LanceTableScan timestamp = (LanceTableScan) hep(topK(scan(), List.of(ts), List.of(1_700_000_000_000L)));
        assertEquals("(ts > to_timestamp_millis(1700000000000) OR ts IS NULL)", timestamp.pushedTopK().orElseThrow().cursorSql());
    }

    public void testCursorShapesThatCannotSpellPass() {
        LanceTableScan scan = scan();
        RelFieldCollation rating = new RelFieldCollation(
            fieldIndex(scan, "rating"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        RelFieldCollation category = new RelFieldCollation(
            fieldIndex(scan, "category"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        // A multi-collation cursor, a null cursor value, a value the
        // column's type cannot compare, and a boolean column all pass.
        assertTrue(hep(topK(scan, List.of(rating, category), List.of(1, "c1"))) instanceof LanceTopK);
        java.util.List<Object> nullCursor = new java.util.ArrayList<>();
        nullCursor.add(null);
        assertTrue(hep(topK(scan, List.of(rating), nullCursor)) instanceof LanceTopK);
        assertTrue(hep(topK(scan, List.of(rating), List.of("not a number"))) instanceof LanceTopK);
        RelFieldCollation flag = new RelFieldCollation(
            fieldIndex(scan, "flag"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        assertTrue(hep(topK(scan, List.of(flag), List.of(1))) instanceof LanceTopK);
    }

    public void testEmptyCollationsFoldAsABarePage() {
        LanceTableScan folded = (LanceTableScan) hep(topK(filterOver(scan()), List.of(), null));
        PushedTopK pushed = folded.pushedTopK().orElseThrow();
        assertTrue(pushed.toScanOrderings().isEmpty());
        assertNull(pushed.cursorSql());
        assertTrue(folded.pushedFilter().isPresent());
    }

    public void testHitShapeFoldsWithTheTopKAndSetsTheRowType() {
        LogicalFilter filter = filterOver(scan());
        RelFieldCollation rating = new RelFieldCollation(
            fieldIndex(filter, "rating"),
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST
        );
        LanceTopK topK = topK(filter, List.of(rating), null);
        LanceHitShape hitShape = new LanceHitShape(
            filter.getCluster(),
            filter.getCluster().traitSetOf(Convention.NONE),
            topK,
            List.of("id", "rating"),
            true,
            true,
            false,
            true
        );
        LanceTableScan folded = (LanceTableScan) hep(hitShape);
        assertEquals(hitShape.getRowType(), folded.getRowType());
        LanceHitShape pushed = folded.pushedTopK().orElseThrow().hitShape();
        assertNotNull(pushed);
        assertEquals(hitShape.outputColumns(), pushed.outputColumns());
        assertTrue(pushed.includeSortValues());
        assertFalse(pushed.includeScore());
    }
}

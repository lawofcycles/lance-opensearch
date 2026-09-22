/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.rex.RexNode;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.substrait.LanceSubstraitProducer;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import org.lance.ipc.ColumnOrdering;

import java.util.List;

/**
 * The top-k node: it keeps the input row type, caps the row estimate
 * at its page, prints its collations and cursor into the digest, and
 * the scan's push preconditions accept it next to a pushed filter but
 * refuse it next to a pushed aggregate.
 */
public class LanceTopKTests extends OpenSearchTestCase {

    private LanceTableScan scan() {
        return (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
    }

    private static LanceTopK topK(RelNode input, List<RelFieldCollation> collations, int fetch, List<Object> searchAfter) {
        return new LanceTopK(input.getCluster(), input.getCluster().traitSetOf(Convention.NONE), input, collations, fetch, 0, searchAfter);
    }

    public void testRowTypeIsTheInputRowType() {
        LanceTableScan scan = scan();
        LanceTopK topK = topK(scan, List.of(new RelFieldCollation(0)), 10, null);
        assertEquals(scan.getRowType(), topK.getRowType());
    }

    public void testEstimateRowCountIsCappedAtThePage() {
        LanceTableScan scan = scan();
        // The fixture table reports 512 rows.
        assertEquals(10.0, topK(scan, List.of(), 10, null).estimateRowCount(scan.getCluster().getMetadataQuery()), 0.0);
        assertEquals(512.0, topK(scan, List.of(), 100_000, null).estimateRowCount(scan.getCluster().getMetadataQuery()), 0.0);
    }

    public void testDigestCarriesCollationsFetchAndCursor() {
        LanceTableScan scan = scan();
        String bare = topK(scan, List.of(new RelFieldCollation(0)), 10, null).getDigest().toString();
        String cursored = topK(scan, List.of(new RelFieldCollation(0)), 10, List.of(5)).getDigest().toString();
        String smaller = topK(scan, List.of(new RelFieldCollation(0)), 3, null).getDigest().toString();
        assertNotEquals(bare, cursored);
        assertNotEquals(bare, smaller);
        assertTrue(cursored, cursored.contains("searchAfter=[5]"));
        assertTrue(bare, bare.contains("fetch=10"));
    }

    public void testWithCollationsCopiesEverythingElse() {
        LanceTableScan scan = scan();
        LanceTopK topK = topK(scan, List.of(new RelFieldCollation(0)), 10, List.of(5));
        LanceTopK recollated = topK.withCollations(List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)));
        assertEquals(List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)), recollated.collations());
        assertEquals(10, recollated.fetch());
        assertEquals(0, recollated.offset());
        assertEquals(List.of((Object) 5), recollated.searchAfter());
        assertSame(topK.getInput(), recollated.getInput());
    }

    public void testRefusesAnEmptyPage() {
        LanceTableScan scan = scan();
        expectThrows(IllegalArgumentException.class, () -> topK(scan, List.of(), 0, null));
    }

    public void testTopKCombinesWithAPushedFilter() {
        LanceTableScan scan = scan();
        RexNode condition = scan.getCluster()
            .getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.IS_NOT_NULL,
                scan.getCluster().getRexBuilder().makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1)
            );
        LanceTableScan filtered = scan.withPushedFilter(condition, "rating IS NOT NULL");
        LanceTopK topK = topK(filtered, List.of(new RelFieldCollation(1)), 10, null);
        ColumnOrdering.Builder orderingBuilder = new ColumnOrdering.Builder();
        orderingBuilder.setColumnName("rating");
        orderingBuilder.setAscending(true);
        orderingBuilder.setNullFirst(false);
        ColumnOrdering ordering = orderingBuilder.build();
        LanceTableScan pushed = filtered.withPushedTopK(topK, null, List.of(ordering), null);
        assertTrue(pushed.pushedFilter().isPresent());
        assertTrue(pushed.pushedTopK().isPresent());
        assertEquals(filtered.getRowType(), pushed.getRowType());
        assertEquals(10.0, pushed.estimateRowCount(pushed.getCluster().getMetadataQuery()), 0.0);
        assertTrue("the digest names the pushed page: " + pushed.getDigest(), pushed.getDigest().toString().contains("topk{"));

        IllegalStateException doubled = expectThrows(
            IllegalStateException.class,
            () -> pushed.withPushedTopK(topK, null, List.of(ordering), null)
        );
        assertTrue(doubled.getMessage(), doubled.getMessage().contains("already carries a pushed top-k"));
        IllegalStateException filterOverTopK = expectThrows(
            IllegalStateException.class,
            () -> scan().withPushedTopK(topK, null, List.of(ordering), null).withPushedFilter(condition, "rating IS NOT NULL")
        );
        assertTrue(filterOverTopK.getMessage(), filterOverTopK.getMessage().contains("cannot push below a pushed top-k"));
    }

    public void testTopKRefusesAPushedAggregate() throws Exception {
        RelNode logical = PlanTestFixtures.translate(
            PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}")
        );
        LanceAggregate aggregate = (LanceAggregate) logical;
        LanceTableScan bare = (LanceTableScan) aggregate.getInput();
        LanceTableScan pushedAggregate = bare.withPushedAggregate(
            aggregate,
            LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow()
        );
        LanceTopK topK = topK(bare, List.of(), 10, null);
        IllegalStateException refusal = expectThrows(
            IllegalStateException.class,
            () -> pushedAggregate.withPushedTopK(topK, null, List.of(), null)
        );
        assertTrue(refusal.getMessage(), refusal.getMessage().contains("does not combine with a pushed aggregate"));
        IllegalStateException reverse = expectThrows(
            IllegalStateException.class,
            () -> bare.withPushedTopK(topK, null, List.of(), null)
                .withPushedAggregate(aggregate, LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow())
        );
        assertTrue(reverse.getMessage(), reverse.getMessage().contains("does not combine with a pushed top-k"));
    }
}

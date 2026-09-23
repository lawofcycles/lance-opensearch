/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.calcite.rel.RelNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * {@link FragmentPlan#of} reads every physical root the planner
 * produces into the plan the fragment executor runs, and every kind of
 * plan survives the wire.
 */
public class FragmentPlanTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, List.of(new LancePlugin())).getNamedWriteables()
    );

    private static RelNode physical(String json) throws IOException {
        SearchSourceBuilder source = PlanTestFixtures.parse(json);
        int size = source.size() < 0 ? 10 : source.size();
        ExecutionShape shape = new ExecutionShape(
            source.query(),
            source.postFilter(),
            source.sorts() == null ? List.of() : source.sorts(),
            source.searchAfter(),
            0,
            size,
            source.aggregations(),
            false
        );
        RelNode logical = SearchRequestToRel.translateForExecution(shape, PlanTestFixtures.model(), PlanTestFixtures.factory());
        return PlanTestFixtures.factory().plan(logical);
    }

    private static FragmentPlan roundTrip(FragmentPlan plan) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            plan.writeTo(out);
            try (
                StreamInput raw = out.bytes().streamInput();
                NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(raw, REGISTRY)
            ) {
                return new FragmentPlan(in);
            }
        }
    }

    public void testBareScanIsTheShapesLuceneKindWithoutAQueryPart() throws IOException {
        RelNode root = physical("{\"size\":0}");
        assertTrue(root instanceof LanceTableScan);
        FragmentPlan count = FragmentPlan.of(root, false, false);
        assertEquals(FragmentPlan.Kind.LUCENE_COUNT, count.kind());
        assertNull(count.filterSql());
        assertNull(count.lanceClause());
        assertNull(count.topK());
        assertNull(count.aggregate());
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, FragmentPlan.of(root, true, false).kind());
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, FragmentPlan.of(root, false, true).kind());
        assertEquals(count, roundTrip(count));
    }

    public void testPushedFilterScanCarriesTheSql() throws IOException {
        RelNode root = physical("{\"size\":0,\"query\":{\"term\":{\"rating\":5}}}");
        assertTrue(root instanceof LanceTableScan scan && scan.pushedFilter().isPresent());
        FragmentPlan plan = FragmentPlan.of(root, false, false);
        assertEquals(FragmentPlan.Kind.LUCENE_COUNT, plan.kind());
        assertEquals("rating = 5", plan.filterSql());
        assertEquals("rating = 5", plan.scalarFilterSql());
        assertEquals(plan, roundTrip(plan));
    }

    public void testPushedFtsScanCarriesTheClauseAndPrefilter() throws IOException {
        String body = "{\"size\":0,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
            + "\"filter\":[{\"term\":{\"rating\":5}}]}}}";
        RelNode root = physical(body);
        assertTrue(root instanceof LanceTableScan scan && scan.pushedFts().isPresent());
        FragmentPlan plan = FragmentPlan.of(root, false, false);
        assertEquals(FragmentPlan.Kind.LUCENE_COUNT, plan.kind());
        assertTrue(plan.lanceClause() instanceof LanceMatchQueryBuilder);
        assertEquals("rating = 5", plan.filterSql());
        assertNull(plan.scalarFilterSql());
        assertFalse(plan.isKnn());
        FragmentPlan restored = roundTrip(plan);
        assertEquals(plan, restored);
        assertEquals(Set.of("body"), ((LanceMatchQueryBuilder) restored.lanceClause()).referencedFields());
    }

    public void testPushedKnnScanCarriesTheClauseWithoutItsFilter() throws IOException {
        String body = "{\"size\":0,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[0.1,0.2],\"k\":5,"
            + "\"filter\":{\"range\":{\"id\":{\"gte\":10}}}}}}";
        RelNode root = physical(body);
        assertTrue(root instanceof LanceTableScan scan && scan.pushedKnn().isPresent());
        FragmentPlan plan = FragmentPlan.of(root, false, false);
        assertTrue(plan.isKnn());
        assertNull(((LanceKnnQueryBuilder) plan.lanceClause()).filter());
        assertEquals("id >= 10", plan.filterSql());
        FragmentPlan restored = roundTrip(plan);
        assertEquals(plan, restored);
        assertEquals(5, ((LanceKnnQueryBuilder) restored.lanceClause()).k());
    }

    public void testPushedTopKScanCarriesOrderingsFetchAndCursor() throws IOException {
        RelNode root = physical("{\"size\":10,\"query\":{\"term\":{\"rating\":5}},\"sort\":[{\"price\":\"desc\"}],\"search_after\":[2.5]}");
        assertTrue(root instanceof LanceTableScan scan && scan.pushedTopK().isPresent());
        FragmentPlan plan = FragmentPlan.of(root, false, true);
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertEquals(List.of(new FragmentPlan.ScanOrdering("price", false, false)), plan.topK().orderings());
        assertEquals(10, plan.topK().fetch());
        assertEquals("(price < 2.5 OR price IS NULL)", plan.topK().cursorSql());
        assertEquals("rating = 5", plan.filterSql());
        assertEquals("(rating = 5) AND ((price < 2.5 OR price IS NULL))", plan.topK().scanFilterSql(plan.filterSql()));
        FragmentPlan restored = roundTrip(plan);
        assertEquals(plan, restored);
        assertEquals("price", restored.topK().toColumnOrderings().get(0).getColumnName());
        assertFalse(restored.topK().toColumnOrderings().get(0).isAscending());
    }

    public void testPushedAggregateScanCarriesTheBytesAndTheShape() throws IOException {
        RelNode root = physical(
            "{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},"
                + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"price\"}},\"m\":{\"max\":{\"field\":\"id\"}}}}}}"
        );
        assertTrue(root instanceof LanceTableScan scan && scan.pushedAggregate().isPresent());
        FragmentPlan plan = FragmentPlan.of(root, true, false);
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertEquals("rating = 5", plan.filterSql());
        FragmentPlan.Aggregate aggregate = plan.aggregate();
        assertEquals(1, aggregate.groupCount());
        assertEquals(
            List.of(new FragmentPlan.MetricSlot("a", MetricSpec.Kind.AVG), new FragmentPlan.MetricSlot("m", MetricSpec.Kind.MAX)),
            aggregate.metrics()
        );
        assertTrue(aggregate.substraitBytes().length > 0);
        assertTrue("the JNI side reads a direct buffer", aggregate.substraitDirect().isDirect());
        FragmentPlan restored = roundTrip(plan);
        assertEquals(plan, restored);
        assertArrayEquals(aggregate.substraitBytes(), restored.aggregate().substraitBytes());
        assertEquals(1, restored.aggregate().toPushedShape().groupCount());
        assertEquals("a", restored.aggregate().toPushedShape().metrics().get(0).name());
    }

    public void testLuceneAggregateExecRootIsALuceneAggregateOverTheFilter() throws IOException {
        // The pushdown rule refuses a tree carrying a cardinality, so
        // the converter's Lucene operator wins.
        RelNode root = physical(
            "{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"category\"}}}}"
        );
        assertTrue("saw " + root.getClass().getSimpleName(), root instanceof LuceneAggregateExec);
        FragmentPlan plan = FragmentPlan.of(root, true, false);
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, plan.kind());
        assertNull(plan.aggregate());
        assertEquals("rating = 5", plan.filterSql());
        assertEquals(plan, roundTrip(plan));
    }

    public void testHeapTopKExecRootIsALuceneTopKOverTheQuery() throws IOException {
        // A page mixing a score collation with a column needs Lucene's
        // comparator; the fused full text query still travels.
        String body = "{\"size\":10,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
            + "\"filter\":[{\"term\":{\"rating\":5}}]}},\"sort\":[{\"price\":\"desc\"}]}";
        RelNode root = physical(body);
        assertTrue("saw " + root.getClass().getSimpleName(), root instanceof HeapTopKExec);
        FragmentPlan plan = FragmentPlan.of(root, false, true);
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, plan.kind());
        assertNull(plan.topK());
        assertTrue(plan.lanceClause() instanceof LanceMatchQueryBuilder);
        assertEquals("rating = 5", plan.filterSql());
        assertEquals(plan, roundTrip(plan));
    }

    public void testLucenePlanFactoryAndRefinementCopies() throws IOException {
        FragmentPlan lucene = FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, "rating = 5");
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, lucene.kind());
        assertEquals("rating = 5", lucene.filterSql());
        expectThrows(IllegalArgumentException.class, () -> FragmentPlan.lucene(FragmentPlan.Kind.PUSHED_SCAN, null));

        FragmentPlan page = FragmentPlan.of(
            physical("{\"size\":10,\"query\":{\"term\":{\"rating\":5}},\"sort\":[{\"price\":\"desc\"}]}"),
            false,
            true
        );
        FragmentPlan collector = page.withoutTopK();
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, collector.kind());
        assertNull(collector.topK());
        assertEquals("rating = 5", collector.filterSql());
        assertSame("nothing to drop returns the same instance", collector, collector.withoutTopK());
        assertSame(collector, collector.withoutAggregate());
        assertSame(collector, collector.withoutLanceClause());

        FragmentPlan aggregate = FragmentPlan.of(physical("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}"), true, false);
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, aggregate.withoutAggregate().kind());

        FragmentPlan fts = FragmentPlan.of(
            physical("{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"),
            false,
            true
        );
        FragmentPlan lucenePage = fts.withoutTopK().withoutLanceClause();
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, lucenePage.kind());
        assertNull(lucenePage.lanceClause());
        assertNull(lucenePage.filterSql());
    }

    public void testAPlanCannotCarryBothAPageAndAnAggregate() {
        FragmentPlan.TopK topK = new FragmentPlan.TopK(List.of(), 10, null);
        FragmentPlan.Aggregate aggregate = new FragmentPlan.Aggregate(new byte[] { 1 }, 0, List.of());
        expectThrows(IllegalArgumentException.class, () -> new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, null, null, topK, aggregate));
        expectThrows(IllegalArgumentException.class, () -> new FragmentPlan(FragmentPlan.Kind.LUCENE_TOPK, null, null, topK, null));
    }
}

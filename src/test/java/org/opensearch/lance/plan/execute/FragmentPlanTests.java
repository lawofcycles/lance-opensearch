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
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.cost.PerfTableFixture;
import org.opensearch.lance.plan.cost.StorageKind;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceScanFilter;
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
        return physical(json, PlanTestFixtures.model(), PlanTestFixtures.factory(), CostInputs.local());
    }

    private static RelNode physical(String json, LanceSchemas.IndexModel model, LancePlannerFactory factory, CostInputs inputs)
        throws IOException {
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
        RelNode logical = SearchRequestToRel.translateForExecution(shape, model, factory);
        return factory.plan(logical, inputs);
    }

    /** {@link FragmentPlan#of} under the local cost inputs the fixture is planned with. */
    private static FragmentPlan of(RelNode root, boolean hasAggregations, boolean hits) {
        return FragmentPlan.of(root, hasAggregations, hits, CostInputs.local());
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
        FragmentPlan count = of(root, false, false);
        assertEquals(FragmentPlan.Kind.LUCENE_COUNT, count.kind());
        assertNull(count.filterSql());
        assertNull(count.lanceClause());
        assertNull(count.topK());
        assertNull(count.aggregate());
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, of(root, true, false).kind());
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, of(root, false, true).kind());
        assertEquals(count, roundTrip(count));
    }

    public void testStreamOpensWithTheWireVersionAndCarriesTwoBlocks() throws IOException {
        FragmentPlan plan = FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_COUNT, "rating = 5");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            plan.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(FragmentPlan.WIRE_VERSION, in.readVInt());
                assertEquals(FragmentPlan.Kind.LUCENE_COUNT, in.readEnum(FragmentPlan.Kind.class));
                assertEquals("rating = 5", in.readOptionalString());
                assertFalse("no Lance clause", in.readBoolean());
                assertFalse("no pushed page", in.readBoolean());
                assertFalse("no pushed aggregate", in.readBoolean());
                assertEquals("the pruning block is optional", 0, in.readVInt());
                assertArrayEquals(new int[0], StreamInput.wrap(in.readByteArray()).readVIntArray());
                assertEquals("the Substrait block is optional while no filter is set", 0, in.readVInt());
                assertFalse(StreamInput.wrap(in.readByteArray()).readBoolean());
                assertEquals("nothing follows", -1, in.read());
            }
        }
    }

    public void testPushedFilterScanCarriesTheSqlAndTheSubstraitBytes() throws IOException {
        RelNode root = physical("{\"size\":0,\"query\":{\"term\":{\"rating\":5}}}");
        assertTrue(root instanceof LanceTableScan scan && scan.pushedFilter().isPresent());
        // The planner picks the Substrait encoding for a scalar filter;
        // the SQL travels next to the bytes for the column loads.
        assertTrue(((LanceTableScan) root).pushedFilter().orElseThrow().usesSubstrait());
        FragmentPlan plan = of(root, false, false);
        assertEquals(FragmentPlan.Kind.LUCENE_COUNT, plan.kind());
        assertEquals("rating = 5", plan.filterSql());
        assertEquals("rating = 5", plan.scalarFilterSql());
        assertNotNull(plan.filterSubstrait());
        assertTrue(plan.filterSubstrait().length > 0);
        LanceScanFilter scalar = plan.scalarFilter();
        assertTrue(scalar.usesSubstrait());
        assertEquals("rating = 5", scalar.sql());
        assertArrayEquals(plan.filterSubstrait(), scalar.substraitBytes());
        FragmentPlan back = roundTrip(plan);
        assertEquals(plan, back);
        assertArrayEquals(plan.filterSubstrait(), back.filterSubstrait());
        assertTrue(plan.toString().contains("filterSubstraitBytes=" + plan.filterSubstrait().length));
        // The bytes survive the exclusion of a fragment.
        FragmentPlan pruned = plan.withExcludedFragments(new int[] { 1 });
        assertArrayEquals(plan.filterSubstrait(), pruned.filterSubstrait());
        assertEquals(pruned, roundTrip(pruned));
    }

    public void testSqlOnlyPlanHasNoSubstraitBytes() throws IOException {
        FragmentPlan plan = FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_COUNT, "rating = 5");
        assertNull(plan.filterSubstrait());
        assertFalse(plan.scalarFilter().usesSubstrait());
        assertEquals("rating = 5", plan.scalarFilter().sql());
        assertEquals(plan, roundTrip(plan));
        assertNotEquals(plan, new FragmentPlan(FragmentPlan.Kind.LUCENE_COUNT, "rating = 5", new byte[] { 1, 2, 3 }, null, null, null));
    }

    public void testSubstraitBytesRefuseAClauseAndAnEmptyArray() {
        LanceMatchQueryBuilder clause = new LanceMatchQueryBuilder("body", "hello");
        expectThrows(
            IllegalArgumentException.class,
            () -> new FragmentPlan(FragmentPlan.Kind.LUCENE_COUNT, "rating = 5", new byte[] { 1 }, clause, null, null)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new FragmentPlan(FragmentPlan.Kind.LUCENE_COUNT, "rating = 5", new byte[0], null, null, null)
        );
    }

    public void testPushedFtsScanCarriesTheClauseAndPrefilter() throws IOException {
        String body = "{\"size\":0,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
            + "\"filter\":[{\"term\":{\"rating\":5}}]}}}";
        RelNode root = physical(body);
        assertTrue(root instanceof LanceTableScan scan && scan.pushedFts().isPresent());
        FragmentPlan plan = of(root, false, false);
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
        FragmentPlan plan = of(root, false, false);
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
        FragmentPlan plan = of(root, false, true);
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
        FragmentPlan plan = of(root, true, false);
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
        FragmentPlan plan = of(root, true, false);
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
        FragmentPlan plan = of(root, false, true);
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

        FragmentPlan page = of(physical("{\"size\":10,\"query\":{\"term\":{\"rating\":5}},\"sort\":[{\"price\":\"desc\"}]}"), false, true);
        FragmentPlan collector = page.withoutTopK();
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, collector.kind());
        assertNull(collector.topK());
        assertEquals("rating = 5", collector.filterSql());
        assertSame("nothing to drop returns the same instance", collector, collector.withoutTopK());
        assertSame(collector, collector.withoutAggregate());
        assertSame(collector, collector.withoutLanceClause());

        FragmentPlan aggregate = of(physical("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}"), true, false);
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, aggregate.withoutAggregate().kind());

        FragmentPlan fts = of(physical("{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"), false, true);
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

    public void testAggregateCostFieldsSurviveTheWire() throws IOException {
        FragmentPlan.Aggregate aggregate = new FragmentPlan.Aggregate(
            new byte[] { 1, 2, 3 },
            1,
            List.of(new FragmentPlan.MetricSlot("s", MetricSpec.Kind.SUM)),
            812.5,
            346.25,
            List.of("category", "price")
        );
        FragmentPlan plan = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, "rating = 5", null, null, aggregate);
        FragmentPlan restored = roundTrip(plan);
        assertEquals(plan, restored);
        assertEquals(812.5, restored.aggregate().pushedMillis(), 0.0);
        assertEquals(346.25, restored.aggregate().luceneWarmMillis(), 0.0);
        assertEquals(List.of("category", "price"), restored.aggregate().luceneColumns());
        assertTrue(restored.aggregate().toString(), restored.aggregate().toString().contains("luceneColumns=[category, price]"));

        FragmentPlan.Aggregate placeholder = new FragmentPlan.Aggregate(new byte[] { 1, 2, 3 }, 1, aggregate.metrics());
        assertEquals(0.0, placeholder.pushedMillis(), 0.0);
        assertEquals(0.0, placeholder.luceneWarmMillis(), 0.0);
        assertEquals(List.of(), placeholder.luceneColumns());
        assertNotEquals("the cost fields take part in equality", aggregate, placeholder);
        FragmentPlan.Aggregate placeholderWithColumns = new FragmentPlan.Aggregate(
            new byte[] { 1 },
            1,
            aggregate.metrics(),
            0.0,
            0.0,
            List.of("category")
        );
        String rendered = placeholderWithColumns.toString();
        assertTrue("the columns print below the fitted range too: " + rendered, rendered.contains("luceneColumns=[category]"));
        assertFalse("the zero costs do not print: " + rendered, rendered.contains("pushedMs"));
        expectThrows(
            IllegalArgumentException.class,
            () -> new FragmentPlan.Aggregate(new byte[] { 1 }, 0, List.of(), -1.0, 0.0, List.of())
        );
    }

    public void testPlannedAggregateCarriesTheCostOfTheAlternativeOverTheFittedRange() throws IOException {
        // perf1b on one local node with the slicing switched off keeps
        // the pushed scan; the plan ships both predictions and the
        // columns the aggregators would have read (the filter's and the
        // key's), so a data node can compare them against its store.
        CostInputs localUnsliced = new CostInputs(1, StorageKind.LOCAL, 64, 32, 1);
        RelNode root = physical(
            "{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}",
            PerfTableFixture.perf1b(),
            PlanTestFixtures.factory(),
            localUnsliced
        );
        assertTrue("saw " + root.getClass().getSimpleName(), root instanceof LanceTableScan scan && scan.pushedAggregate().isPresent());
        FragmentPlan plan = FragmentPlan.of(root, true, false, localUnsliced);
        FragmentPlan.Aggregate aggregate = plan.aggregate();
        assertTrue(aggregate.toString(), aggregate.pushedMillis() > 0.0);
        assertTrue(aggregate.toString(), aggregate.luceneWarmMillis() > 0.0);
        assertTrue("locally the pushed scan was the cheaper form: " + aggregate, aggregate.pushedMillis() <= aggregate.luceneWarmMillis());
        assertEquals(List.of("category", "rating"), aggregate.luceneColumns());
        assertEquals(plan, roundTrip(plan));
    }

    /** One physical root the planner produces today, with the query part it must yield. */
    private record RootCase(String body, boolean hasAggregations, boolean hits, Class<?> root, String filterSql, Class<?> clause) {
    }

    public void testEveryPlannerRootYieldsItsQueryPart() throws IOException {
        // Every physical root the planner can produce today, planned by
        // the planner rather than built by hand, each with a filter and,
        // where the shape allows one, a full text or knn clause under it:
        // the plan must carry the filter SQL and the clause whatever the
        // root. A root outside the expected class fails the test, which
        // is the point: a new operator must be added to the allow list
        // FragmentPlan.of climbs through, or the query part is silently
        // dropped.
        String ftsBool =
            "{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],\"filter\":[{\"term\":{\"rating\":5}}]}}";
        String term = "{\"term\":{\"rating\":5}}";
        List<RootCase> cases = List.of(
            // A pushed aggregate over the filter.
            new RootCase(
                "{\"size\":0,\"query\":" + term + ",\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}",
                true,
                false,
                LanceTableScan.class,
                "rating = 5",
                null
            ),
            // The Lucene aggregate operator over the filter (cardinality is not pushed).
            new RootCase(
                "{\"size\":0,\"query\":" + term + ",\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"category\"}}}}",
                true,
                false,
                LuceneAggregateExec.class,
                "rating = 5",
                null
            ),
            // Aggregations over a full text query: the fused scan carries the
            // query and the aggregators run over it as the shape's Lucene kind.
            new RootCase(
                "{\"size\":0,\"query\":" + ftsBool + ",\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}",
                true,
                false,
                LanceTableScan.class,
                "rating = 5",
                LanceMatchQueryBuilder.class
            ),
            // The heap top-k over the filter (a post filter keeps the page on the collector).
            new RootCase(
                "{\"size\":10,\"query\":" + term + ",\"post_filter\":{\"term\":{\"flag\":true}},\"sort\":[{\"price\":\"desc\"}]}",
                false,
                true,
                HeapTopKExec.class,
                "rating = 5",
                null
            ),
            // The heap top-k over the fused full text query (a column sort on a full text page).
            new RootCase(
                "{\"size\":10,\"query\":" + ftsBool + ",\"sort\":[{\"price\":\"desc\"}]}",
                false,
                true,
                HeapTopKExec.class,
                "rating = 5",
                LanceMatchQueryBuilder.class
            ),
            // A pushed page over the filter.
            new RootCase(
                "{\"size\":10,\"query\":" + term + ",\"sort\":[{\"price\":\"desc\"}]}",
                false,
                true,
                LanceTableScan.class,
                "rating = 5",
                null
            ),
            // A pushed full text page with its prefilter.
            new RootCase(
                "{\"size\":10,\"query\":" + ftsBool + "}",
                false,
                true,
                LanceTableScan.class,
                "rating = 5",
                LanceMatchQueryBuilder.class
            ),
            // A pushed knn page with its prefilter.
            new RootCase(
                "{\"size\":5,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[0.1,0.2],\"k\":5,"
                    + "\"filter\":{\"range\":{\"id\":{\"gte\":10}}}}}}",
                false,
                true,
                LanceTableScan.class,
                "id >= 10",
                LanceKnnQueryBuilder.class
            ),
            // The count shapes: a pushed filter scan, and a pushed full text scan.
            new RootCase("{\"size\":0,\"query\":" + term + "}", false, false, LanceTableScan.class, "rating = 5", null),
            new RootCase(
                "{\"size\":0,\"query\":" + ftsBool + "}",
                false,
                false,
                LanceTableScan.class,
                "rating = 5",
                LanceMatchQueryBuilder.class
            )
        );
        for (RootCase c : cases) {
            RelNode root = physical(c.body());
            assertEquals("root of " + c.body() + " is " + root, c.root(), root.getClass());
            FragmentPlan plan = of(root, c.hasAggregations(), c.hits());
            assertEquals("filter of " + c.body() + " on " + plan, c.filterSql(), plan.filterSql());
            if (c.clause() == null) {
                assertNull("no clause for " + c.body() + " on " + plan, plan.lanceClause());
            } else {
                assertNotNull("clause of " + c.body() + " on " + plan, plan.lanceClause());
                assertEquals("clause of " + c.body() + " on " + plan, c.clause(), plan.lanceClause().getClass());
            }
        }
    }
}

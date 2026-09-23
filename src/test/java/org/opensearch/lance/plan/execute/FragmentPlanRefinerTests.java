/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.calcite.rel.RelNode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.dispatch.LanceFragmentQueryRequest;
import org.opensearch.lance.execute.LanceAggregateResults;
import org.opensearch.lance.execute.LanceAggregateResultsTestSupport;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.cost.AggregateProfile;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.cost.CostModel;
import org.opensearch.lance.plan.cost.PerfTableFixture;
import org.opensearch.lance.plan.cost.StorageKind;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner.Inputs;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner.Reason;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner.Refined;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * The fragment executor's node local guards: which pushed operations a
 * reader wrapper, the Lucene sort field types, the aggregate
 * resolution and the column store's residency move to the Lucene side,
 * that nothing else changes, and that a plan the guards leave alone is
 * returned as the same instance.
 */
public class FragmentPlanRefinerTests extends OpenSearchTestCase {

    private static final FragmentPlan.Aggregate AGGREGATE = new FragmentPlan.Aggregate(
        new byte[] { 1, 2, 3 },
        1,
        List.of(new FragmentPlan.MetricSlot("s", MetricSpec.Kind.SUM))
    );
    private static final FragmentPlan.TopK SORTED_PAGE = new FragmentPlan.TopK(
        List.of(new FragmentPlan.ScanOrdering("rating", false, false)),
        10,
        null
    );
    private static final FragmentPlan.TopK SCAN_ORDER_PAGE = new FragmentPlan.TopK(List.of(), 10, null);

    private static LanceFragmentQueryRequest request(FragmentPlan plan, List<SortBuilder<?>> sorts, Object[] searchAfter) {
        return new LanceFragmentQueryRequest(
            "table",
            "idx",
            StorageOptions.empty(),
            -1L,
            plan,
            null,
            null,
            sorts,
            searchAfter,
            10,
            null,
            List.of(),
            false,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
    }

    private static SortAndFormats numericSort(SortField.Type type, Object missing) {
        SortedNumericSortField field = new SortedNumericSortField("rating", type, true);
        if (missing != null) {
            field.setMissingValue(missing);
        }
        return new SortAndFormats(new Sort(field), new DocValueFormat[] { DocValueFormat.RAW });
    }

    private static Inputs inputs(boolean wrapper, SortAndFormats sort, LanceFragmentQueryRequest request) {
        return new Inputs(wrapper, sort, request, aggregate -> null, null);
    }

    /** Inputs whose aggregate resolution succeeds and whose column store answers {@code resident} for every column list. */
    private static Inputs resolving(LanceFragmentQueryRequest request, Predicate<List<String>> resident) {
        return new Inputs(false, null, request, aggregate -> STUB, resident);
    }

    private static final LanceAggregateResults STUB = LanceAggregateResultsTestSupport.resolvedStub(AGGREGATE.toPushedShape());

    /** The pushed aggregate of {@link #AGGREGATE} with the shipped costs and columns set. */
    private static FragmentPlan.Aggregate costed(double pushedMillis, double luceneWarmMillis, List<String> columns) {
        return new FragmentPlan.Aggregate(
            AGGREGATE.substraitBytes(),
            AGGREGATE.groupCount(),
            AGGREGATE.metrics(),
            pushedMillis,
            luceneWarmMillis,
            columns
        );
    }

    public void testAPlanTheGuardsLeaveAloneIsReturnedAsIs() {
        FragmentPlan plan = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, "rating = 5", null, SORTED_PAGE, null);
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("rating"));
        Refined refined = new FragmentPlanRefiner().refine(
            plan,
            inputs(false, numericSort(SortField.Type.INT, null), request(plan, sorts, null))
        );
        assertSame(plan, refined.plan());
        assertFalse(refined.refined());
        assertEquals(List.of(), refined.reasons());
        assertNull(refined.aggregate());
    }

    public void testSecurityWrapperDropsAggregatePageAndFullTextClause() {
        FragmentPlanRefiner refiner = new FragmentPlanRefiner();
        FragmentPlan aggregate = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, "rating = 5", null, null, AGGREGATE);
        Refined refinedAggregate = refiner.refine(aggregate, inputs(true, null, request(aggregate, List.of(), null)));
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, refinedAggregate.plan().kind());
        assertNull(refinedAggregate.plan().aggregate());
        assertEquals("the scalar filter stays", "rating = 5", refinedAggregate.plan().filterSql());
        assertEquals(List.of(Reason.SECURITY_WRAPPER), refinedAggregate.reasons());

        FragmentPlan page = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, "rating = 5", null, SORTED_PAGE, null);
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("rating"));
        Refined refinedPage = refiner.refine(page, inputs(true, numericSort(SortField.Type.INT, null), request(page, sorts, null)));
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, refinedPage.plan().kind());
        assertNull(refinedPage.plan().topK());
        assertEquals("rating = 5", refinedPage.plan().filterSql());
        assertEquals(List.of(Reason.SECURITY_WRAPPER), refinedPage.reasons());

        FragmentPlan fts = new FragmentPlan(
            FragmentPlan.Kind.PUSHED_SCAN,
            "rating = 5",
            new LanceMatchQueryBuilder("body", "hello"),
            SCAN_ORDER_PAGE,
            null
        );
        Refined refinedFts = refiner.refine(fts, inputs(true, null, request(fts, List.of(), null)));
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, refinedFts.plan().kind());
        assertNull("the whole bool goes back to the Lucene composition", refinedFts.plan().lanceClause());
        assertNull("its prefilter goes with it", refinedFts.plan().filterSql());
        assertEquals(List.of(Reason.SECURITY_WRAPPER), refinedFts.reasons());
    }

    public void testSecurityWrapperKeepsAPushedKnn() {
        FragmentPlan knn = new FragmentPlan(
            FragmentPlan.Kind.PUSHED_SCAN,
            "id >= 10",
            new LanceKnnQueryBuilder("embedding", new float[] { 0.1f, 0.2f }, 5),
            SCAN_ORDER_PAGE,
            null
        );
        Refined refined = new FragmentPlanRefiner().refine(knn, inputs(true, null, request(knn, List.of(), null)));
        assertTrue(refined.plan().isKnn());
        assertEquals("the knn prefilter is the caller's own predicate", "id >= 10", refined.plan().filterSql());
        assertNull("the page cut moves to the collector", refined.plan().topK());
        assertEquals(List.of(Reason.SECURITY_WRAPPER), refined.reasons());

        FragmentPlan knnCount = new FragmentPlan(
            FragmentPlan.Kind.LUCENE_COUNT,
            "id >= 10",
            new LanceKnnQueryBuilder("embedding", new float[] { 0.1f, 0.2f }, 5),
            null,
            null
        );
        Refined same = new FragmentPlanRefiner().refine(knnCount, inputs(true, null, request(knnCount, List.of(), null)));
        assertSame("nothing wrapper sensitive to drop", knnCount, same.plan());
        assertFalse(same.refined());
    }

    public void testSortFieldTypeGuards() {
        FragmentPlanRefiner refiner = new FragmentPlanRefiner();
        FragmentPlan page = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, null, null, SORTED_PAGE, null);
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("rating"));

        for (SortField.Type ok : List.of(SortField.Type.INT, SortField.Type.LONG, SortField.Type.FLOAT, SortField.Type.DOUBLE)) {
            assertFalse(ok.name(), refiner.refine(page, inputs(false, numericSort(ok, null), request(page, sorts, null))).refined());
        }
        SortAndFormats sortedSet = new SortAndFormats(
            new Sort(new SortedSetSortField("category", false)),
            new DocValueFormat[] { DocValueFormat.RAW }
        );
        assertFalse(refiner.refine(page, inputs(false, sortedSet, request(page, sorts, null))).refined());

        Refined noSort = refiner.refine(page, inputs(false, null, request(page, sorts, null)));
        assertEquals(List.of(Reason.SORT_FIELD_TYPE), noSort.reasons());
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, noSort.plan().kind());
        assertNull(noSort.plan().topK());

        SortAndFormats otherType = new SortAndFormats(
            new Sort(new SortField("rating", SortField.Type.STRING)),
            new DocValueFormat[] { DocValueFormat.RAW }
        );
        assertEquals(List.of(Reason.SORT_FIELD_TYPE), refiner.refine(page, inputs(false, otherType, request(page, sorts, null))).reasons());

        SortAndFormats ip = new SortAndFormats(new Sort(new SortedSetSortField("addr", false)), new DocValueFormat[] { DocValueFormat.IP });
        assertEquals(List.of(Reason.SORT_FIELD_TYPE), refiner.refine(page, inputs(false, ip, request(page, sorts, null))).reasons());

        SortAndFormats twoFields = new SortAndFormats(
            new Sort(new SortedNumericSortField("rating", SortField.Type.INT, true), new SortedNumericSortField("id", SortField.Type.INT)),
            new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW }
        );
        assertEquals(
            "one Lucene sort field per request sort clause",
            List.of(Reason.SORT_FIELD_TYPE),
            refiner.refine(page, inputs(false, twoFields, request(page, sorts, null))).reasons()
        );

        FragmentPlan cursorPage = new FragmentPlan(
            FragmentPlan.Kind.PUSHED_SCAN,
            null,
            null,
            new FragmentPlan.TopK(SORTED_PAGE.orderings(), 10, "rating < 7"),
            null
        );
        SortAndFormats withSentinel = numericSort(SortField.Type.INT, Integer.MIN_VALUE);
        assertFalse(
            "a cursor at a stored value keeps the pushed page",
            refiner.refine(cursorPage, inputs(false, withSentinel, request(cursorPage, sorts, new Object[] { 7 }))).refined()
        );
        assertEquals(
            "a cursor at the missing value sentinel stays on the comparator",
            List.of(Reason.SORT_FIELD_TYPE),
            refiner.refine(cursorPage, inputs(false, withSentinel, request(cursorPage, sorts, new Object[] { Integer.MIN_VALUE })))
                .reasons()
        );

        FragmentPlan scanOrder = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, null, null, SCAN_ORDER_PAGE, null);
        assertFalse(
            "a page without orderings needs no sort field",
            refiner.refine(scanOrder, inputs(false, null, request(scanOrder, List.of(), null))).refined()
        );
    }

    public void testAggregateResolutionRefusalDropsTheAggregate() {
        FragmentPlan plan = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, "rating = 5", null, null, AGGREGATE);
        Refined refused = new FragmentPlanRefiner().refine(
            plan,
            new Inputs(false, null, request(plan, List.of(), null), aggregate -> null, null)
        );
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, refused.plan().kind());
        assertNull(refused.plan().aggregate());
        assertEquals("rating = 5", refused.plan().filterSql());
        assertEquals(List.of(Reason.AGGREGATE_RESOLUTION), refused.reasons());
        assertNull(refused.aggregate());

        Refined noResolver = new FragmentPlanRefiner().refine(plan, new Inputs(false, null, request(plan, List.of(), null), null, null));
        assertEquals(List.of(Reason.AGGREGATE_RESOLUTION), noResolver.reasons());
    }

    public void testWarmColumnStoreMovesACheaperAggregateToTheAggregators() {
        // The coordinator priced the pushed scan over an object store
        // above the aggregators over resident columns; the node holds
        // both columns, so the aggregators run and the resolved executor
        // is dropped.
        List<String> columns = List.of("category", "price");
        FragmentPlan plan = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, "rating = 5", null, null, costed(800.0, 350.0, columns));
        List<List<String>> asked = new ArrayList<>();
        Refined warm = new FragmentPlanRefiner().refine(plan, resolving(request(plan, List.of(), null), asked::add));
        assertEquals(List.of(Reason.COLUMN_STORE_WARM), warm.reasons());
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, warm.plan().kind());
        assertNull(warm.plan().aggregate());
        assertNull("the resolved executor is not handed out", warm.aggregate());
        assertEquals("the scalar filter stays", "rating = 5", warm.plan().filterSql());
        assertEquals("the store is asked about the shipped columns", List.of(columns), asked);
    }

    public void testColdColumnKeepsThePushedAggregate() {
        FragmentPlan plan = new FragmentPlan(
            FragmentPlan.Kind.PUSHED_SCAN,
            null,
            null,
            null,
            costed(800.0, 350.0, List.of("category", "price"))
        );
        // The store holds category but not price.
        Refined cold = new FragmentPlanRefiner().refine(
            plan,
            resolving(request(plan, List.of(), null), columns -> columns.stream().allMatch("category"::equals))
        );
        assertFalse(cold.refined());
        assertSame(plan, cold.plan());
        assertSame("the resolved executor runs the scan", STUB, cold.aggregate());
    }

    public void testPushedScanCheaperThanWarmAggregatorsStaysPushed() {
        FragmentPlan cheaperPushed = new FragmentPlan(
            FragmentPlan.Kind.PUSHED_SCAN,
            null,
            null,
            null,
            costed(300.0, 350.0, List.of("price"))
        );
        Refined kept = new FragmentPlanRefiner().refine(cheaperPushed, resolving(request(cheaperPushed, List.of(), null), columns -> true));
        assertFalse(kept.refined());
        assertSame(STUB, kept.aggregate());

        FragmentPlan tie = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, null, null, null, costed(350.0, 350.0, List.of("price")));
        assertFalse(
            "equal costs keep the coordinator's choice",
            new FragmentPlanRefiner().refine(tie, resolving(request(tie, List.of(), null), columns -> true)).refined()
        );

        FragmentPlan placeholder = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, null, null, null, AGGREGATE);
        assertFalse(
            "zeros are the placeholder regime, never a cheaper alternative",
            new FragmentPlanRefiner().refine(placeholder, resolving(request(placeholder, List.of(), null), columns -> true)).refined()
        );
    }

    public void testUncachedSnapshotNeverAsksTheColumnStore() {
        FragmentPlan plan = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, null, null, null, costed(800.0, 350.0, List.of("price")));
        Refined uncached = new FragmentPlanRefiner().refine(plan, resolving(request(plan, List.of(), null), null));
        assertFalse(uncached.refined());
        assertSame(STUB, uncached.aggregate());
    }

    public void testLocalStorageTableCannotFireTheColumnStoreGuard() throws IOException {
        // The numbers a coordinator over a local table ships: the pushed
        // cost and the warm Lucene cost computed by the real formulas
        // over the perf1b statistics. Over local storage the Lucene cost
        // has no object store term, so the warm cost is the cost the
        // planner compared and lost against, and the guard cannot fire
        // even with every column resident.
        LancePlannerFactory factory = PlanTestFixtures.factory();
        CostInputs unsliced = new CostInputs(1, StorageKind.LOCAL, 64, 32, 1);
        SearchSourceBuilder source = PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}");
        ExecutionShape shape = new ExecutionShape(source.query(), null, List.of(), null, 0, 0, source.aggregations(), true);
        RelNode logical = SearchRequestToRel.translateForExecution(shape, PerfTableFixture.perf1b(), factory);
        RelNode physical = factory.plan(logical, unsliced);
        assertTrue("the pushed scan wins locally without slicing: " + physical, physical instanceof LanceTableScan);
        LanceTableScan scan = (LanceTableScan) physical;
        AggregateProfile profile = AggregateProfile.of(
            scan.pushedAggregate().get().aggregate(),
            scan,
            scan.getCluster().getMetadataQuery()
        );
        double pushed = CostModel.pushedAggregateMillis(unsliced, profile);
        double luceneWarm = CostModel.luceneAggregateMillis(unsliced.withStorage(StorageKind.LOCAL), profile);
        assertEquals(
            "over local storage the warm cost is the compared cost",
            CostModel.luceneAggregateMillis(unsliced, profile),
            luceneWarm,
            0.0
        );
        assertTrue(pushed + " vs " + luceneWarm, pushed <= luceneWarm);

        FragmentPlan plan = FragmentPlan.of(physical, true, false, unsliced);
        assertEquals(pushed, plan.aggregate().pushedMillis(), 0.0);
        assertEquals(luceneWarm, plan.aggregate().luceneWarmMillis(), 0.0);
        Refined refined = new FragmentPlanRefiner().refine(plan, resolving(request(plan, List.of(), null), columns -> true));
        assertFalse("every column resident, still the pushed scan", refined.refined());
        assertSame(STUB, refined.aggregate());
    }

    public void testWarmColumnStoreIsCountedUnderItsOwnKey() {
        Map<String, Long> before = FragmentPlanRefiner.refinementCounts();
        FragmentPlan plan = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, null, null, null, costed(800.0, 350.0, List.of("price")));
        new FragmentPlanRefiner().refine(plan, resolving(request(plan, List.of(), null), columns -> true));
        Map<String, Long> after = FragmentPlanRefiner.refinementCounts();
        assertEquals(before.get("column_store_warm") + 1L, (long) after.get("column_store_warm"));
        assertEquals(before.get("aggregate_resolution"), after.get("aggregate_resolution"));
    }

    public void testRefinementCountsReportEveryReason() {
        Map<String, Long> before = FragmentPlanRefiner.refinementCounts();
        assertEquals(
            List.of("security_wrapper", "sort_field_type", "aggregate_resolution", "column_store_warm"),
            List.copyOf(before.keySet())
        );
        FragmentPlan plan = new FragmentPlan(FragmentPlan.Kind.PUSHED_SCAN, "rating = 5", null, null, AGGREGATE);
        new FragmentPlanRefiner().refine(plan, inputs(true, null, request(plan, List.of(), null)));
        Map<String, Long> after = FragmentPlanRefiner.refinementCounts();
        assertEquals(before.get("security_wrapper") + 1L, (long) after.get("security_wrapper"));
        assertEquals(before.get("sort_field_type"), after.get("sort_field_type"));
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.traits;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.calcite.ShardPathConvention;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceShardPathShape;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.LanceTopK;
import org.opensearch.lance.plan.rel.ShardPathReason;
import org.opensearch.lance.plan.rel.physical.FanOutExec;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.plan.rel.physical.LuceneHandoffExec;
import org.opensearch.lance.plan.rel.physical.MergeExec;
import org.opensearch.lance.plan.rel.physical.ShardPathFallbackExec;
import org.opensearch.lance.plan.substrait.LanceSubstraitProducer;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * The two request demandable traits: the order of their values, what
 * every operator declares, how the planner factory reads a
 * {@link PlanRequirement} at the root, and the refusal it raises when
 * no plan meets it.
 */
public class PlanTraitsTests extends OpenSearchTestCase {

    private static RelNode translate(String body) throws IOException {
        return PlanTestFixtures.translate(PlanTestFixtures.parse(body));
    }

    private static LanceTableScan scanBelow(RelNode node) {
        RelNode current = node;
        while (!(current instanceof LanceTableScan)) {
            current = current.getInput(0);
        }
        return (LanceTableScan) current;
    }

    private static Accuracy accuracyOf(RelNode rel) {
        return rel.getTraitSet().getTrait(Accuracy.Def.INSTANCE);
    }

    private static TieStability tieStabilityOf(RelNode rel) {
        return rel.getTraitSet().getTrait(TieStability.Def.INSTANCE);
    }

    private static LanceTableScan pushedAggregate(String body) throws IOException {
        LanceAggregate aggregate = (LanceAggregate) translate(body);
        ByteBuffer bytes = LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow();
        return scanBelow(aggregate).withPushedAggregate(aggregate, bytes);
    }

    private static LuceneAggregateExec luceneAggregate(String body) throws IOException {
        LanceAggregate aggregate = (LanceAggregate) translate(body);
        LanceTableScan scan = scanBelow(aggregate);
        return new LuceneAggregateExec(scan.getCluster(), scan.getCluster().traitSetOf(LuceneConvention.INSTANCE), scan, aggregate);
    }

    private static HeapTopKExec heapTopK(String body) throws IOException {
        LanceHitShape hitShape = (LanceHitShape) translate(body);
        LanceTopK topK = (LanceTopK) hitShape.getInput();
        LanceTableScan scan = scanBelow(topK);
        return new HeapTopKExec(scan.getCluster(), scan.getCluster().traitSetOf(LuceneConvention.INSTANCE), scan, topK, hitShape);
    }

    /** The physical per node plan the factory chooses for {@code body} under the local inputs. */
    private static RelNode physical(String body) throws IOException {
        RelNode logical = translate(body);
        return PlanTestFixtures.factory().plan(logical);
    }

    public void testAccuracyOrdersExactAboveApproximate() {
        assertTrue(Accuracy.EXACT.satisfies(Accuracy.EXACT));
        assertTrue(Accuracy.EXACT.satisfies(Accuracy.APPROXIMATE));
        assertTrue(Accuracy.APPROXIMATE.satisfies(Accuracy.APPROXIMATE));
        assertFalse(Accuracy.APPROXIMATE.satisfies(Accuracy.EXACT));
        assertSame("the default is the weakest declaration", Accuracy.APPROXIMATE, Accuracy.Def.INSTANCE.getDefault());
        assertFalse(Accuracy.Def.INSTANCE.canConvert(null, Accuracy.APPROXIMATE, Accuracy.EXACT));
        assertNull(Accuracy.Def.INSTANCE.convert(null, null, Accuracy.EXACT, true));
        assertEquals("exact", Accuracy.EXACT.toString());
    }

    public void testTieStabilityStableValuesSatisfyOnlyThemselvesAndUnstable() {
        for (TieStability stable : List.of(TieStability.STABLE_ROWADDR, TieStability.STABLE_KEY)) {
            assertTrue(stable.satisfies(stable));
            assertTrue(stable.satisfies(TieStability.UNSTABLE));
        }
        assertFalse(
            "row address order is not the key order a cursor was typed against",
            TieStability.STABLE_ROWADDR.satisfies(TieStability.STABLE_KEY)
        );
        assertFalse(TieStability.STABLE_KEY.satisfies(TieStability.STABLE_ROWADDR));
        assertTrue(TieStability.UNSTABLE.satisfies(TieStability.UNSTABLE));
        assertFalse(TieStability.UNSTABLE.satisfies(TieStability.STABLE_KEY));
        assertFalse(TieStability.UNSTABLE.satisfies(TieStability.STABLE_ROWADDR));
        assertSame("the default is the weakest declaration", TieStability.UNSTABLE, TieStability.Def.INSTANCE.getDefault());
        assertFalse(TieStability.Def.INSTANCE.canConvert(null, TieStability.UNSTABLE, TieStability.STABLE_KEY));
        assertNull(TieStability.Def.INSTANCE.convert(null, null, TieStability.STABLE_KEY, true));
        assertEquals("stable_rowaddr", TieStability.STABLE_ROWADDR.toString());
    }

    public void testEveryTraitSetOfTheClusterCarriesBothDefaults() {
        LancePlannerFactory factory = PlanTestFixtures.factory();
        RelTraitSet logical = factory.newCluster().traitSetOf(Convention.NONE);
        assertSame(Accuracy.APPROXIMATE, logical.getTrait(Accuracy.Def.INSTANCE));
        assertSame(TieStability.UNSTABLE, logical.getTrait(TieStability.Def.INSTANCE));
    }

    public void testBareScanIsExactInRowAddressOrder() {
        LanceTableScan scan = (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
        assertSame(LanceConvention.INSTANCE, scan.getConvention());
        assertSame(Accuracy.EXACT, accuracyOf(scan));
        assertSame(TieStability.STABLE_ROWADDR, tieStabilityOf(scan));
        assertTrue("the digest carries the traits: " + scan.getDigest(), scan.getDigest().contains("LANCE.exact.stable_rowaddr"));
    }

    public void testPushedPagesDeclareTheOrderOfTheirCollations() throws IOException {
        // A column ordered page, a cursor page, an unsorted scalar page
        // and a bare full text page, each folded into the scan.
        LanceTableScan keyed = (LanceTableScan) physical("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}]}");
        assertTrue(keyed.pushedTopK().isPresent());
        assertSame(Accuracy.EXACT, accuracyOf(keyed));
        assertSame(TieStability.STABLE_KEY, tieStabilityOf(keyed));

        LanceTableScan cursor = (LanceTableScan) physical("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}],\"search_after\":[7]}");
        assertNotNull(cursor.pushedTopK().orElseThrow().cursorSql());
        assertSame(TieStability.STABLE_KEY, tieStabilityOf(cursor));

        LanceTableScan unsorted = (LanceTableScan) physical("{\"size\":5,\"query\":{\"term\":{\"rating\":5}}}");
        assertTrue(unsorted.pushedTopK().isPresent());
        assertSame("no sort over a scalar query is the scan's row address order", TieStability.STABLE_ROWADDR, tieStabilityOf(unsorted));

        LanceTableScan scored = (LanceTableScan) physical(
            "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"
        );
        assertTrue(scored.pushedFts().isPresent());
        assertTrue(scored.pushedTopK().isPresent());
        assertSame(Accuracy.EXACT, accuracyOf(scored));
        assertSame("equal scores have no reproducible order", TieStability.UNSTABLE, tieStabilityOf(scored));

        LanceTableScan explicitScore = (LanceTableScan) physical(
            "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\"]}"
        );
        assertSame(TieStability.UNSTABLE, tieStabilityOf(explicitScore));
    }

    public void testPushedFullTextWithoutAPageIsUnstable() throws IOException {
        // A size 0 full text request folds the clause alone: the rows
        // come in score order, so no reproducible order is promised.
        LanceTableScan count = (LanceTableScan) physical(
            "{\"size\":0,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"
        );
        assertTrue(count.pushedFts().isPresent());
        assertTrue(count.pushedTopK().isEmpty());
        assertSame(Accuracy.EXACT, accuracyOf(count));
        assertSame(TieStability.UNSTABLE, tieStabilityOf(count));
    }

    public void testPushedAggregateIsExactUnlessAMetricIsASketch() throws IOException {
        LanceTableScan sum = pushedAggregate("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}");
        assertSame(Accuracy.EXACT, accuracyOf(sum));
        assertSame("group rows have no order contract", TieStability.UNSTABLE, tieStabilityOf(sum));

        LanceTableScan terms = pushedAggregate(
            "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}}}"
        );
        assertSame(Accuracy.EXACT, accuracyOf(terms));

        LanceTableScan cardinality = pushedAggregate("{\"size\":0,\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"category\"}}}}");
        assertSame(Accuracy.APPROXIMATE, accuracyOf(cardinality));

        LanceTableScan percentiles = pushedAggregate("{\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"rating\"}}}}");
        assertSame(Accuracy.APPROXIMATE, accuracyOf(percentiles));

        LanceAggregate mixed = (LanceAggregate) translate(
            "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}},"
                + "\"r\":{\"percentile_ranks\":{\"field\":\"rating\",\"values\":[5]}}}}}}"
        );
        assertSame("one sketch among exact metrics makes the tree approximate", Accuracy.APPROXIMATE, mixed.accuracy());
    }

    public void testLuceneAggregateExecDeclaresTheSameAccuracyAsThePushedScan() throws IOException {
        for (String body : List.of(
            "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}",
            "{\"size\":0,\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"category\"}}}}",
            "{\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"rating\"}}}}"
        )) {
            LuceneAggregateExec exec = luceneAggregate(body);
            assertSame(body, accuracyOf(pushedAggregate(body)), accuracyOf(exec));
            assertSame(body, exec.aggregate().accuracy(), accuracyOf(exec));
            assertSame(body, TieStability.UNSTABLE, tieStabilityOf(exec));
            assertSame(LuceneConvention.INSTANCE, exec.getConvention());
        }
    }

    public void testHeapTopKExecDeclaresTheOrderOfItsPage() throws IOException {
        HeapTopKExec keyed = heapTopK("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}]}");
        assertSame(Accuracy.EXACT, accuracyOf(keyed));
        assertSame(TieStability.STABLE_KEY, tieStabilityOf(keyed));

        HeapTopKExec tieBroken = heapTopK(
            "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\",{\"id\":\"asc\"}]}"
        );
        assertSame(
            "a stored column tie breaker after the score makes the page reproducible",
            TieStability.STABLE_KEY,
            tieStabilityOf(tieBroken)
        );

        HeapTopKExec columnOverFts = heapTopK(
            "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[{\"rating\":\"desc\"}]}"
        );
        assertSame(TieStability.STABLE_KEY, tieStabilityOf(columnOverFts));

        HeapTopKExec scored = heapTopK(
            "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\"]}"
        );
        assertSame(TieStability.UNSTABLE, tieStabilityOf(scored));

        HeapTopKExec bareFts = heapTopK("{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}");
        assertSame(TieStability.UNSTABLE, tieStabilityOf(bareFts));

        HeapTopKExec knn = heapTopK("{\"size\":3,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[0.1,0.2],\"k\":3}}}");
        assertSame(TieStability.UNSTABLE, tieStabilityOf(knn));

        HeapTopKExec unsorted = heapTopK("{\"size\":5,\"query\":{\"term\":{\"rating\":5}}}");
        assertSame(TieStability.STABLE_ROWADDR, tieStabilityOf(unsorted));
    }

    public void testHandoffFanOutAndMergeInheritTheirInput() throws IOException {
        LanceTableScan cardinality = pushedAggregate("{\"size\":0,\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"category\"}}}}");
        LuceneHandoffExec handoff = new LuceneHandoffExec(
            cardinality.getCluster(),
            cardinality.getCluster().traitSetOf(LuceneConvention.INSTANCE),
            cardinality
        );
        assertSame(LuceneConvention.INSTANCE, handoff.getConvention());
        assertSame(Accuracy.APPROXIMATE, accuracyOf(handoff));
        assertSame(TieStability.UNSTABLE, tieStabilityOf(handoff));

        LanceTableScan keyed = (LanceTableScan) physical("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}]}");
        MergeExec merge = (MergeExec) SearchRequestToRel.withCoordinatorLayer(keyed, MergeExec.ReduceKind.HITS_TOP_K, 3);
        FanOutExec fanOut = (FanOutExec) merge.getInput();
        assertSame(Accuracy.EXACT, accuracyOf(fanOut));
        assertSame(TieStability.STABLE_KEY, tieStabilityOf(fanOut));
        assertSame("the merge keeps the per node page's tie order", Accuracy.EXACT, accuracyOf(merge));
        assertSame(TieStability.STABLE_KEY, tieStabilityOf(merge));

        MergeExec sketch = (MergeExec) SearchRequestToRel.withCoordinatorLayer(cardinality, MergeExec.ReduceKind.AGGREGATE_INTERNAL, 2);
        assertSame("reducing sketches stays approximate", Accuracy.APPROXIMATE, accuracyOf(sketch));

        // copy re reads the new input
        MergeExec copy = (MergeExec) sketch.copy(sketch.getTraitSet(), List.of(fanOut));
        assertSame(Accuracy.EXACT, accuracyOf(copy));
    }

    public void testShardPathFallbackIsExactInRowAddressOrder() {
        LanceTableScan scan = (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
        ShardPathFallbackExec exec = new ShardPathFallbackExec(
            scan.getCluster(),
            scan.getCluster().traitSetOf(ShardPathConvention.INSTANCE),
            scan,
            List.of(ShardPathReason.SUGGEST)
        );
        assertSame(Accuracy.EXACT, accuracyOf(exec));
        assertSame(TieStability.STABLE_ROWADDR, tieStabilityOf(exec));
        LanceShardPathShape shape = new LanceShardPathShape(
            scan.getCluster(),
            scan.getCluster().traitSetOf(Convention.NONE),
            scan,
            List.of(ShardPathReason.SUGGEST)
        );
        assertSame("the logical shape carries the defaults", Accuracy.APPROXIMATE, accuracyOf(shape));
    }

    public void testRequirementApplyAndSatisfy() {
        PlanRequirement none = PlanRequirement.NONE;
        assertTrue(none.isNone());
        PlanRequirement exact = none.withAccuracy(Accuracy.EXACT, "track_total_hits");
        assertFalse(exact.isNone());
        PlanRequirement both = exact.withTieStability(TieStability.STABLE_KEY, "search_after");
        RelTraitSet defaults = PlanTestFixtures.factory().newCluster().traitSetOf(LuceneConvention.INSTANCE);
        RelTraitSet applied = both.applyTo(defaults);
        assertSame(Accuracy.EXACT, applied.getTrait(Accuracy.Def.INSTANCE));
        assertSame(TieStability.STABLE_KEY, applied.getTrait(TieStability.Def.INSTANCE));
        assertSame(LuceneConvention.INSTANCE, applied.getTrait(ConventionTraitDef.INSTANCE));
        assertTrue(none.satisfiedBy(defaults));
        assertFalse(exact.satisfiedBy(defaults));
        assertTrue(both.satisfiedBy(applied));
        assertTrue(exact.satisfiedBy(applied));
        assertFalse(both.satisfiedBy(defaults.plus(Accuracy.EXACT).plus(TieStability.STABLE_ROWADDR)));
        assertFalse("a trait set without the defs promises nothing", exact.satisfiedBy(RelTraitSet.createEmpty()));
    }

    public void testPlannerMeetsAnExactDemandWithThePushedScan() throws IOException {
        RelNode logical = translate("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}");
        RelNode physical = PlanTestFixtures.factory()
            .plan(logical, CostInputs.local(), PlanRequirement.NONE.withAccuracy(Accuracy.EXACT, "track_total_hits"));
        assertTrue(physical instanceof LanceTableScan);
        assertSame(Accuracy.EXACT, accuracyOf(physical));
    }

    public void testPlannerRefusesAnExactDemandOverASketch() throws IOException {
        RelNode logical = translate("{\"size\":0,\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"category\"}}}}");
        LancePlannerFactory factory = PlanTestFixtures.factory();
        // Without the demand the tree plans (the aggregators win on cost).
        assertTrue(factory.plan(logical) instanceof LuceneAggregateExec);
        UnmetPlanRequirementException refused = expectThrows(
            UnmetPlanRequirementException.class,
            () -> factory.plan(logical, CostInputs.local(), PlanRequirement.NONE.withAccuracy(Accuracy.EXACT, "track_total_hits"))
        );
        assertSame(Accuracy.APPROXIMATE, refused.offeredAccuracy());
        assertTrue(refused.getMessage(), refused.getMessage().startsWith("plan_failed"));
        assertTrue(refused.getMessage(), refused.getMessage().contains("track_total_hits requires Accuracy [exact]"));
        assertTrue(refused.getMessage(), refused.getMessage().contains("offers Accuracy [approximate]"));
    }

    public void testPlannerRefusesAStableDemandOverAScoredPage() throws IOException {
        RelNode logical = translate("{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}");
        LancePlannerFactory factory = PlanTestFixtures.factory();
        UnmetPlanRequirementException refused = expectThrows(
            UnmetPlanRequirementException.class,
            () -> factory.plan(logical, CostInputs.local(), PlanRequirement.NONE.withTieStability(TieStability.STABLE_KEY, "search_after"))
        );
        assertSame(TieStability.UNSTABLE, refused.offeredTieStability());
        assertTrue(refused.getMessage(), refused.getMessage().contains("search_after requires TieStability [stable_key]"));
        assertTrue(refused.getMessage(), refused.getMessage().contains("offers TieStability [unstable]"));
    }

    public void testPlannerMeetsAStableDemandWithTheHeapPageWhenTheScanCannot() throws IOException {
        // Two collations with a cursor: the top-k rule does not fold, the
        // heap page over the fused clause is the only form and it is
        // STABLE_KEY, so the demand is met.
        RelNode logical = translate(
            "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                + "\"sort\":[{\"category\":\"asc\"},{\"id\":\"asc\"}],\"search_after\":[\"c0\",3]}"
        );
        RelNode physical = PlanTestFixtures.factory()
            .plan(logical, CostInputs.local(), PlanRequirement.NONE.withTieStability(TieStability.STABLE_KEY, "search_after"));
        assertTrue(physical.toString(), physical instanceof HeapTopKExec);
        assertSame(TieStability.STABLE_KEY, tieStabilityOf(physical));
    }
}

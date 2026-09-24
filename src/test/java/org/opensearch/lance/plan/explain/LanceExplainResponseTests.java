/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.lance.plan.rel.ShardPathReason;
import org.opensearch.lance.plan.traits.Accuracy;
import org.opensearch.lance.plan.traits.PlanRequirement;
import org.opensearch.lance.plan.traits.TieStability;
import org.opensearch.lance.plan.traits.TraitEnforcement;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The explain response: both routes survive the stream round trip
 * with every field, and the JSON carries the route, the reasons on the
 * shard path, the fragment plan, the unplanned element and the
 * predicted refinements on the fragment route, and the traits object on
 * both. The stream opens with the wire version and a reader refuses
 * another one.
 */
public class LanceExplainResponseTests extends OpenSearchTestCase {

    private static final LanceExplainResponse.Traits NO_DEMAND = new LanceExplainResponse.Traits(
        Accuracy.APPROXIMATE,
        null,
        Accuracy.EXACT,
        TieStability.STABLE_ROWADDR,
        "none"
    );

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, List.of(new LancePlugin())).getNamedWriteables()
    );

    private static LanceExplainResponse roundTrip(LanceExplainResponse response) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (
                StreamInput raw = out.bytes().streamInput();
                NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(raw, REGISTRY)
            ) {
                return new LanceExplainResponse(in);
            }
        }
    }

    private static Map<String, Object> json(LanceExplainResponse response) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), true, XContentType.JSON).v2();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Map<String, Object> parent, String key) {
        return (Map<String, Object>) parent.get(key);
    }

    public void testFragmentRouteWithAPushedPageRoundTripsAndRenders() throws IOException {
        float[] vector = new float[8];
        vector[0] = 1f;
        FragmentPlan plan = new FragmentPlan(
            FragmentPlan.Kind.PUSHED_SCAN,
            "rating >= 500",
            new LanceKnnQueryBuilder("embedding", vector, 3),
            new FragmentPlan.TopK(List.of(new FragmentPlan.ScanOrdering("rating", false, false)), 5, "(rating < 7 OR rating IS NULL)"),
            null
        );
        LanceExplainResponse response = LanceExplainResponse.fragment(
            "demo",
            "LanceHitShape\n  LanceTopK\n    LanceTableScan\n",
            "MergeExec(reduce=[HITS_TOP_K])\n  FanOutExec(fanOut=[3])\n    LanceTableScan\n",
            plan,
            null,
            List.of(FragmentPlanRefiner.Reason.SORT_FIELD_TYPE),
            NO_DEMAND
        );
        assertEquals(response, roundTrip(response));
        assertEquals(LanceExplainResponse.Route.FRAGMENT, response.route());
        assertTrue(response.reasons().isEmpty());

        Map<String, Object> json = json(response);
        assertEquals("demo", json.get("index"));
        assertEquals("fragment", json.get("route"));
        assertFalse("no reasons on the fragment route", json.containsKey("reasons"));
        assertTrue(json.get("physical").toString().startsWith("MergeExec("));
        assertFalse("nothing unplanned", json.containsKey("unplanned"));
        assertEquals(List.of("sort_field_type"), json.get("refinements_possible"));
        Map<String, Object> fragmentPlan = object(json, "fragment_plan");
        assertEquals("PUSHED_SCAN", fragmentPlan.get("kind"));
        assertEquals("rating >= 500", fragmentPlan.get("filter_sql"));
        assertEquals("lance_knn", fragmentPlan.get("lance_clause"));
        assertFalse(fragmentPlan.containsKey("aggregate"));
        Map<String, Object> topK = object(fragmentPlan, "top_k");
        assertEquals(5, topK.get("fetch"));
        assertEquals("(rating < 7 OR rating IS NULL)", topK.get("cursor_sql"));
        assertEquals(List.of(Map.of("column", "rating", "ascending", false, "nulls_first", false)), topK.get("orderings"));
    }

    public void testFragmentRouteWithAPushedAggregateRendersTheSummary() throws IOException {
        FragmentPlan plan = new FragmentPlan(
            FragmentPlan.Kind.PUSHED_SCAN,
            null,
            null,
            null,
            new FragmentPlan.Aggregate(new byte[] { 1, 2, 3 }, 1, List.of(new FragmentPlan.MetricSlot("s", MetricSpec.Kind.SUM)))
        );
        LanceExplainResponse response = LanceExplainResponse.fragment("demo", "logical", "physical", plan, null, List.of(), NO_DEMAND);
        assertEquals(response, roundTrip(response));
        Map<String, Object> json = json(response);
        assertEquals(List.of(), json.get("refinements_possible"));
        Map<String, Object> fragmentPlan = object(json, "fragment_plan");
        assertEquals("PUSHED_SCAN", fragmentPlan.get("kind"));
        assertFalse(fragmentPlan.containsKey("filter_sql"));
        assertFalse(fragmentPlan.containsKey("lance_clause"));
        assertFalse(fragmentPlan.containsKey("top_k"));
        Map<String, Object> aggregate = object(fragmentPlan, "aggregate");
        assertEquals(1, aggregate.get("group_count"));
        assertEquals(3, aggregate.get("substrait_bytes"));
        assertEquals(List.of(Map.of("name", "s", "kind", "SUM")), aggregate.get("metrics"));
    }

    public void testFragmentRouteWithALucenePlanCarriesTheUnplannedElement() throws IOException {
        FragmentPlan plan = FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, "rating = 5");
        LanceExplainResponse response = LanceExplainResponse.fragment(
            "demo",
            "logical",
            "physical",
            plan,
            "sort type [_geo_distance]",
            // Given out of order: the response sorts the reasons.
            List.of(FragmentPlanRefiner.Reason.SORT_FIELD_TYPE, FragmentPlanRefiner.Reason.SECURITY_WRAPPER),
            NO_DEMAND
        );
        assertEquals(response, roundTrip(response));
        Map<String, Object> json = json(response);
        assertEquals("sort type [_geo_distance]", json.get("unplanned"));
        assertEquals(List.of("security_wrapper", "sort_field_type"), json.get("refinements_possible"));
        assertEquals(
            List.of(FragmentPlanRefiner.Reason.SECURITY_WRAPPER, FragmentPlanRefiner.Reason.SORT_FIELD_TYPE),
            response.refinementsPossible()
        );
        assertEquals("LUCENE_TOPK", object(json, "fragment_plan").get("kind"));
    }

    public void testShardPathRouteCarriesTheReasonsAndNoFragmentPlan() throws IOException {
        LanceExplainResponse response = LanceExplainResponse.shardPath(
            "demo",
            List.of(ShardPathReason.SUGGEST, ShardPathReason.HIGHLIGHT),
            "LanceShardPathShape(reasons=[[SUGGEST, HIGHLIGHT]])\n  LanceTableScan\n",
            "ShardPathFallbackExec(reasons=[[SUGGEST, HIGHLIGHT]])\n  LanceTableScan\n",
            NO_DEMAND
        );
        assertEquals(response, roundTrip(response));
        assertNull(response.fragmentPlan());
        assertNull(response.unplanned());
        Map<String, Object> json = json(response);
        assertEquals("shard_path", json.get("route"));
        assertEquals(List.of("SUGGEST", "HIGHLIGHT"), json.get("reasons"));
        assertFalse(json.containsKey("fragment_plan"));
        assertFalse(json.containsKey("unplanned"));
        assertFalse("the data node refines nothing on the shard path", json.containsKey("refinements_possible"));
        assertTrue(json.get("physical").toString().startsWith("ShardPathFallbackExec("));
        Map<String, Object> traits = object(json, "traits");
        assertEquals(Map.of("accuracy", "APPROXIMATE", "tie_stability", "NONE"), traits.get("requested"));
        assertEquals(Map.of("accuracy", "EXACT", "tie_stability", "STABLE_ROWADDR"), traits.get("declared"));
        assertEquals("none", traits.get("enforcer"));
    }

    public void testShardPathRouteNeedsAReason() {
        expectThrows(IllegalArgumentException.class, () -> LanceExplainResponse.shardPath("demo", List.of(), "l", "p", NO_DEMAND));
    }

    public void testTraitsWithADemandRoundTripAndRender() throws IOException {
        PlanRequirement requirement = PlanRequirement.NONE.withAccuracy(Accuracy.EXACT, "track_total_hits")
            .withTieStability(TieStability.STABLE_KEY, "search_after");
        LanceExplainResponse.Traits traits = new LanceExplainResponse.Traits(
            Accuracy.EXACT,
            TieStability.STABLE_KEY,
            Accuracy.EXACT,
            TieStability.STABLE_KEY,
            TraitEnforcement.satisfied(requirement).describe()
        );
        LanceExplainResponse response = LanceExplainResponse.fragment(
            "demo",
            "logical",
            "physical",
            FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, null),
            null,
            List.of(),
            traits
        );
        LanceExplainResponse read = roundTrip(response);
        assertEquals(response, read);
        assertEquals(traits, read.traits());
        assertSame(TieStability.STABLE_KEY, read.traits().requestedTieStability());
        Map<String, Object> json = object(json(response), "traits");
        assertEquals(Map.of("accuracy", "EXACT", "tie_stability", "STABLE_KEY"), json.get("requested"));
        assertEquals(Map.of("accuracy", "EXACT", "tie_stability", "STABLE_KEY"), json.get("declared"));
        assertEquals("none", json.get("enforcer"));
    }

    public void testPlanFailedCarriesTheRefusalAndNoFragmentPlan() throws IOException {
        PlanRequirement requirement = PlanRequirement.NONE.withAccuracy(Accuracy.EXACT, "track_total_hits");
        LanceExplainResponse.Traits traits = new LanceExplainResponse.Traits(
            Accuracy.EXACT,
            null,
            Accuracy.APPROXIMATE,
            TieStability.UNSTABLE,
            new TraitEnforcement(requirement, Accuracy.APPROXIMATE, TieStability.UNSTABLE, false).describe()
        );
        LanceExplainResponse response = LanceExplainResponse.planFailed(
            "demo",
            "LanceAggregate\n  LanceTableScan\n",
            "MergeExec\n  FanOutExec\n    LuceneAggregateExec\n      LanceTableScan\n",
            "plan_failed: no plan of this request meets its trait requirement; track_total_hits requires Accuracy [exact]",
            traits
        );
        assertEquals(response, roundTrip(response));
        assertEquals(LanceExplainResponse.Route.FRAGMENT, response.route());
        assertNull(response.fragmentPlan());
        Map<String, Object> json = json(response);
        assertEquals("fragment", json.get("route"));
        assertFalse("nothing ships: " + json, json.containsKey("fragment_plan"));
        assertTrue(json.get("unplanned").toString().startsWith("plan_failed"));
        assertEquals(List.of(), json.get("refinements_possible"));
        Map<String, Object> traitsJson = object(json, "traits");
        assertEquals(Map.of("accuracy", "EXACT", "tie_stability", "NONE"), traitsJson.get("requested"));
        assertEquals(Map.of("accuracy", "APPROXIMATE", "tie_stability", "UNSTABLE"), traitsJson.get("declared"));
        assertEquals(
            "track_total_hits demanded Accuracy [EXACT], the cheapest plan offered [APPROXIMATE]; no plan declares the demand (plan_failed)",
            traitsJson.get("enforcer")
        );
    }

    public void testReaderRefusesAnotherWireVersion() throws IOException {
        LanceExplainResponse response = LanceExplainResponse.shardPath("demo", List.of(ShardPathReason.SUGGEST), "l", "p", NO_DEMAND);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(LanceExplainResponse.WIRE_VERSION + 1);
            try (BytesStreamOutput rest = new BytesStreamOutput()) {
                response.writeTo(rest);
                StreamInput written = rest.bytes().streamInput();
                assertEquals(LanceExplainResponse.WIRE_VERSION, written.readVInt());
                out.writeBytes(written.readAllBytes());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                IOException refused = expectThrows(IOException.class, () -> new LanceExplainResponse(in));
                assertEquals(
                    "LanceExplainResponse wire version [2] does not match this node's [1]: every node must run the same plugin version",
                    refused.getMessage()
                );
            }
        }
    }
}

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
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.WireVersion;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner;
import org.opensearch.lance.plan.rel.MetricSpec;
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
import java.util.Set;

/**
 * The explain response: both routes survive the stream round trip
 * with every field, and the JSON carries the route, the refusal message
 * alone on the unsupported route, and the fragment plan, the unplanned
 * element, the predicted refinements and the traits object on the
 * fragment route. The stream opens with the wire version; a version 1
 * stream is decoded with the retired route mapped to unsupported, and
 * a newer optional block is stepped over.
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
            NO_DEMAND,
            LanceExplainResponse.Cacheability.no("size > 0")
        );
        assertEquals(response, roundTrip(response));
        assertEquals(LanceExplainResponse.Route.FRAGMENT, response.route());

        Map<String, Object> json = json(response);
        assertEquals("demo", json.get("index"));
        assertEquals("fragment", json.get("route"));
        assertTrue(json.get("physical").toString().startsWith("MergeExec("));
        assertFalse("nothing unplanned", json.containsKey("unplanned"));
        assertEquals(false, json.get("cacheable"));
        assertEquals("size > 0", json.get("cacheable_reason"));
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
        LanceExplainResponse response = LanceExplainResponse.fragment(
            "demo",
            "logical",
            "physical",
            plan,
            null,
            List.of(),
            NO_DEMAND,
            LanceExplainResponse.Cacheability.YES
        );
        assertEquals(response, roundTrip(response));
        Map<String, Object> json = json(response);
        assertEquals(List.of(), json.get("refinements_possible"));
        assertEquals(true, json.get("cacheable"));
        assertFalse("a cacheable body carries no reason: " + json, json.containsKey("cacheable_reason"));
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
            NO_DEMAND,
            LanceExplainResponse.Cacheability.no("dls")
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

    public void testUnsupportedRouteCarriesTheRefusalAndNothingElse() throws IOException {
        String refusal = "search body carries a `highlight` clause which needs full-text APIs Lance does not surface.";
        LanceExplainResponse response = LanceExplainResponse.unsupported("demo", refusal);
        assertEquals(response, roundTrip(response));
        assertEquals(LanceExplainResponse.Route.UNSUPPORTED, response.route());
        assertNull(response.logical());
        assertNull(response.physical());
        assertNull(response.fragmentPlan());
        assertNull(response.traits());
        assertEquals(refusal, response.unplanned());
        assertTrue(response.refinementsPossible().isEmpty());
        Map<String, Object> json = json(response);
        assertEquals("demo", json.get("index"));
        assertEquals("unsupported", json.get("route"));
        assertEquals(refusal, json.get("unplanned"));
        assertEquals("nothing was planned: " + json, Set.of("index", "route", "unplanned"), json.keySet());
    }

    public void testUnsupportedRouteNeedsAMessage() {
        expectThrows(NullPointerException.class, () -> LanceExplainResponse.unsupported("demo", null));
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
            traits,
            LanceExplainResponse.Cacheability.YES
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
        assertNull("nothing runs, so nothing is cached", response.cacheability());
        Map<String, Object> json = json(response);
        assertEquals("fragment", json.get("route"));
        assertFalse("nothing ships: " + json, json.containsKey("fragment_plan"));
        assertFalse("nothing runs: " + json, json.containsKey("cacheable"));
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

    public void testStreamOpensWithTheWireVersionAndANewerOptionalBlockIsSteppedOver() throws IOException {
        LanceExplainResponse response = LanceExplainResponse.unsupported("demo", "no plan");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(LanceExplainResponse.WIRE_VERSION, in.readVInt());
            }
        }
        // The stream a version 3 node would write: today's fields and one
        // optional block this version does not know.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(LanceExplainResponse.WIRE_VERSION + 1);
            try (BytesStreamOutput rest = new BytesStreamOutput()) {
                response.writeTo(rest);
                StreamInput written = rest.bytes().streamInput();
                assertEquals(LanceExplainResponse.WIRE_VERSION, written.readVInt());
                out.writeBytes(written.readAllBytes());
            }
            WireVersion.writeBlock(out, false, o -> o.writeString("a field of version 3"));
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(response, new LanceExplainResponse(in));
                assertEquals("the reader consumed the block", -1, in.read());
            }
        }
    }

    public void testMixedPluginVersionAVersion2AnswerHasNoCacheability() throws IOException {
        // The stream a version 2 node writes: today's base fields and no
        // block; the cacheability falls back to absent.
        LanceExplainResponse today = LanceExplainResponse.fragment(
            "demo",
            "logical",
            "physical",
            FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, null),
            null,
            List.of(),
            NO_DEMAND,
            LanceExplainResponse.Cacheability.YES
        );
        try (BytesStreamOutput out = new BytesStreamOutput(); BytesStreamOutput written = new BytesStreamOutput()) {
            today.writeTo(written);
            int blockBytes;
            try (BytesStreamOutput block = new BytesStreamOutput()) {
                WireVersion.writeBlock(block, false, o -> o.writeOptionalWriteable(LanceExplainResponse.Cacheability.YES));
                blockBytes = block.bytes().length();
            }
            try (StreamInput in = written.bytes().streamInput()) {
                assertEquals(LanceExplainResponse.WIRE_VERSION, in.readVInt());
                byte[] rest = in.readAllBytes();
                out.writeVInt(2);
                out.writeBytes(rest, 0, rest.length - blockBytes);
            }
            LanceExplainResponse asVersion2 = read(out);
            assertNull(asVersion2.cacheability());
            assertEquals(today.fragmentPlan(), asVersion2.fragmentPlan());
            assertEquals(today.traits(), asVersion2.traits());
        }
    }

    public void testMixedPluginVersionAVersion2CoordinatorReadsTodaysAnswerWithoutTheCacheability() throws IOException {
        // Today's writer, read as a version 2 node does: the base fields
        // by hand in their version 2 layout, then the walk over the
        // blocks it does not know. That node has no cacheability field at
        // all, so the block is stepped over and the stream ends there.
        LanceExplainResponse today = LanceExplainResponse.fragment(
            "demo",
            "logical",
            "physical",
            FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, "rating = 5"),
            "collapse",
            List.of(FragmentPlanRefiner.Reason.SORT_FIELD_TYPE),
            NO_DEMAND,
            LanceExplainResponse.Cacheability.no("size > 0")
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            today.writeTo(out);
            try (
                StreamInput raw = out.bytes().streamInput();
                NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(raw, REGISTRY)
            ) {
                WireVersion.Reader reader = WireVersion.read(in, "LanceExplainResponse", 2, 2);
                assertEquals(LanceExplainResponse.WIRE_VERSION, reader.marker());
                assertEquals("demo", in.readString());
                assertEquals(LanceExplainResponse.Route.FRAGMENT, in.readEnum(LanceExplainResponse.Route.class));
                assertEquals("logical", in.readOptionalString());
                assertEquals("physical", in.readOptionalString());
                assertEquals(today.fragmentPlan(), in.readOptionalWriteable(FragmentPlan::new));
                assertEquals("collapse", in.readOptionalString());
                assertEquals(
                    List.of(FragmentPlanRefiner.Reason.SORT_FIELD_TYPE),
                    in.readList(input -> input.readEnum(FragmentPlanRefiner.Reason.class))
                );
                assertTrue(in.readBoolean());
                assertEquals(today.traits(), LanceExplainResponse.Traits.read(in));
                reader.finish();
                assertEquals("the version 2 reader stepped over the cacheability block", -1, in.read());
            }
        }
    }

    /**
     * The stream a version 1 node writes: the route enum had
     * {@code FRAGMENT} and {@code SHARD_PATH}, a reasons list followed
     * the route, the plan texts and the traits were always present.
     */
    private static void writeVersion1(
        BytesStreamOutput out,
        int routeOrdinal,
        int[] reasonOrdinals,
        String logical,
        String physical,
        Writeable plan,
        String unplanned,
        List<FragmentPlanRefiner.Reason> refinements,
        LanceExplainResponse.Traits traits
    ) throws IOException {
        out.writeVInt(1);
        out.writeString("demo");
        out.writeVInt(routeOrdinal);
        out.writeVInt(reasonOrdinals.length);
        for (int ordinal : reasonOrdinals) {
            out.writeVInt(ordinal);
        }
        out.writeString(logical);
        out.writeString(physical);
        out.writeOptionalWriteable(plan);
        out.writeOptionalString(unplanned);
        out.writeCollection(refinements, StreamOutput::writeEnum);
        traits.writeTo(out);
    }

    private static LanceExplainResponse read(BytesStreamOutput out) throws IOException {
        try (
            StreamInput raw = out.bytes().streamInput();
            NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(raw, REGISTRY)
        ) {
            LanceExplainResponse response = new LanceExplainResponse(in);
            assertEquals("the reader consumed the whole response", -1, in.read());
            return response;
        }
    }

    public void testMixedPluginVersionAVersion1FragmentAnswerReadsFieldByField() throws IOException {
        FragmentPlan plan = FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, "rating = 5");
        // A version 1 node ships a version 1 plan inside its answer.
        Writeable version1Plan = o -> {
            o.writeVInt(1);
            o.writeEnum(FragmentPlan.Kind.LUCENE_TOPK);
            o.writeOptionalString("rating = 5");
            o.writeOptionalNamedWriteable(null);
            o.writeOptionalWriteable(null);
            o.writeOptionalWriteable(null);
        };
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writeVersion1(
                out,
                0,
                new int[0],
                "logical",
                "physical",
                version1Plan,
                "collapse",
                List.of(FragmentPlanRefiner.Reason.SORT_FIELD_TYPE),
                NO_DEMAND
            );
            LanceExplainResponse response = read(out);
            assertEquals("demo", response.index());
            assertEquals(LanceExplainResponse.Route.FRAGMENT, response.route());
            assertEquals("logical", response.logical());
            assertEquals("physical", response.physical());
            assertEquals(plan, response.fragmentPlan());
            assertEquals("collapse", response.unplanned());
            assertEquals(List.of(FragmentPlanRefiner.Reason.SORT_FIELD_TYPE), response.refinementsPossible());
            assertEquals(NO_DEMAND, response.traits());
            assertNull("a version 1 answer has no cacheability", response.cacheability());
            assertFalse("nothing rendered for it: " + json(response), json(response).containsKey("cacheable"));
        }
    }

    public void testMixedPluginVersionAVersion1ShardPathAnswerBecomesUnsupported() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writeVersion1(out, 1, new int[] { 0, 2 }, "logical", "physical", null, null, List.of(), NO_DEMAND);
            LanceExplainResponse response = read(out);
            assertEquals(LanceExplainResponse.unsupported("demo", LanceExplainResponse.SHARD_PATH_RETIRED), response);
            Map<String, Object> json = json(response);
            assertEquals("unsupported", json.get("route"));
            assertEquals(LanceExplainResponse.SHARD_PATH_RETIRED, json.get("unplanned"));
            assertFalse("no plan texts on the unsupported route: " + json, json.containsKey("logical"));
            assertFalse("no traits on the unsupported route: " + json, json.containsKey("traits"));
        }
    }
}

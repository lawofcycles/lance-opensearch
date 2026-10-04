/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.query.ScanAdmission;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * The {@code profile.lance} object the coordinator renders: a node's
 * {@code query} block carries {@code admission_kind} when its executor
 * reported the kind the full text gate judged the request under, and
 * no such key when the gate did not run; several responses of one node
 * keep the later kind. The kind an executor derives for the profile
 * follows the split {@code ScanAdmission.admit} makes.
 */
public class LanceSearchProfileTests extends OpenSearchTestCase {

    private static final DiscoveryNode NODE_A = new DiscoveryNode("a", buildNewFakeTransportAddress(), Version.CURRENT);
    private static final DiscoveryNode NODE_B = new DiscoveryNode("b", buildNewFakeTransportAddress(), Version.CURRENT);

    private static SearchResponse emptyResponse() {
        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), Float.NaN);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, null, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 5L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> queryBlock(LanceSearchProfile profile, String nodeId) throws IOException {
        SearchResponse rendered = profile.attachTo(emptyResponse(), false);
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            rendered.toXContent(builder, ToXContent.EMPTY_PARAMS);
            Map<String, Object> json = XContentHelper.convertToMap(BytesReference.bytes(builder), true, XContentType.JSON).v2();
            Map<String, Object> lance = (Map<String, Object>) ((Map<String, Object>) json.get("profile")).get("lance");
            Map<String, Object> nodes = (Map<String, Object>) lance.get("nodes");
            return (Map<String, Object>) ((Map<String, Object>) nodes.get(nodeId)).get("query");
        }
    }

    public void testAdmissionKindRendersInTheQueryBlockWhenReported() throws IOException {
        LanceSearchProfile profile = new LanceSearchProfile();
        profile.record(NODE_A, new LanceFragmentQueryResponse.Profile(12L, 3L, 0L, 0L, 0L, 0L, 1L, "fts_flat"));
        profile.record(NODE_B, new LanceFragmentQueryResponse.Profile(7L, 1L, 0L, 0L, 0L, 0L, 0L));
        Map<String, Object> judged = queryBlock(profile, "a");
        assertEquals(12, judged.get("millis"));
        assertEquals(1, judged.get("fts_scans"));
        assertEquals("fts_flat", judged.get("admission_kind"));
        Map<String, Object> ungated = queryBlock(profile, "b");
        assertEquals(7, ungated.get("millis"));
        assertFalse("no gate, no key: " + ungated, ungated.containsKey("admission_kind"));
    }

    public void testSeveralResponsesOfOneNodeKeepTheLaterKind() throws IOException {
        LanceSearchProfile profile = new LanceSearchProfile();
        profile.record(NODE_A, new LanceFragmentQueryResponse.Profile(1L, 0L, 0L, 0L, 0L, 0L, 1L, "fts"));
        profile.record(NODE_A, new LanceFragmentQueryResponse.Profile(2L, 0L, 0L, 0L, 0L, 0L, 1L, "fts_flat"));
        // A fetch round response carries no kind and leaves the one the
        // query round reported.
        profile.record(NODE_A, new LanceFragmentQueryResponse.Profile(0L, 4L, 1L, 3L, 2L, 2L, 0L));
        Map<String, Object> query = queryBlock(profile, "a");
        assertEquals(3, query.get("millis"));
        assertEquals(2, query.get("fts_scans"));
        assertEquals("fts_flat", query.get("admission_kind"));
    }

    public void testExecutorKindFollowsTheGatesSplit() {
        ScanAdmission.Shape body = new ScanAdmission.Shape(true, false, 10L, 1, 0, Set.of("body"));
        assertEquals("fts", FragmentExecutorSupport.ftsAdmissionKind(body, Set.of("body", "title")));
        assertEquals("fts_flat", FragmentExecutorSupport.ftsAdmissionKind(body, Set.of("title")));
        assertEquals("fts_flat", FragmentExecutorSupport.ftsAdmissionKind(body, Set.of()));
        assertEquals("the gate takes an unknown set as no index", "fts_flat", FragmentExecutorSupport.ftsAdmissionKind(body, null));
        ScanAdmission.Shape both = new ScanAdmission.Shape(true, true, 0L, 1, 0, Set.of("body", "title"));
        assertEquals(
            "a split request reports the flat path, judged last",
            "fts_flat",
            FragmentExecutorSupport.ftsAdmissionKind(both, Set.of("body"))
        );
        assertEquals("fts", FragmentExecutorSupport.ftsAdmissionKind(both, Set.of("body", "title")));
        ScanAdmission.Shape unnamed = new ScanAdmission.Shape(true, true, 0L);
        assertEquals("a shape naming no column is judged as fts", "fts", FragmentExecutorSupport.ftsAdmissionKind(unnamed, Set.of()));
    }
}

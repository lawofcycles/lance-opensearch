/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.util.LinkedHashMap;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;

/**
 * The consensus check over the mappings a {@code node_local} build's
 * node legs re-derive from their clones: agreement is judged on the
 * parsed maps (key order and whitespace do not count), a disagreement
 * names the nodes involved and applies nothing, and no mapping at all
 * is not an error.
 */
public class LanceBuildIndexesMappingConsensusTests extends OpenSearchTestCase {

    public void testAgreementIgnoresKeyOrderAndWhitespace() {
        Map<String, String> byNode = new LinkedHashMap<>();
        byNode.put("node-a", "{\"properties\":{\"label\":{\"type\":\"lance_text\"},\"id\":{\"type\":\"integer\"}}}");
        byNode.put("node-b", "{ \"properties\": {\"id\": {\"type\": \"integer\"}, \"label\": {\"type\": \"lance_text\"}} }");
        TransportLanceBuildIndexesAction.MappingConsensus consensus = TransportLanceBuildIndexesAction.mappingConsensus(byNode);
        assertNull(consensus.error());
        assertEquals("the first node's literal JSON is the one applied", byNode.get("node-a"), consensus.mappingJson());
    }

    public void testDisagreementNamesTheNodesAndAppliesNothing() {
        Map<String, String> byNode = new LinkedHashMap<>();
        byNode.put("node-a", "{\"properties\":{\"label\":{\"type\":\"lance_text\"}}}");
        byNode.put("node-b", "{\"properties\":{\"label\":{\"type\":\"keyword\"}}}");
        TransportLanceBuildIndexesAction.MappingConsensus consensus = TransportLanceBuildIndexesAction.mappingConsensus(byNode);
        assertNull(consensus.mappingJson());
        assertNotNull(consensus.error());
        assertTrue(consensus.error(), consensus.error().contains("node-b"));
        assertTrue(consensus.error(), consensus.error().contains("node-a"));
    }

    public void testNoMappingsIsNotAnError() {
        TransportLanceBuildIndexesAction.MappingConsensus consensus = TransportLanceBuildIndexesAction.mappingConsensus(Map.of());
        assertNull(consensus.mappingJson());
        assertNull(consensus.error());
    }
}

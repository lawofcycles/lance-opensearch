/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.lance.LancePlugin;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * The {@link IndicesRequest.Replaceable} contract of the explain
 * request: what a security plugin reads through {@code indices()} and
 * writes back through {@code indices(String...)}, and that the written
 * name is what the transport action and the wire see while the search
 * body is kept.
 */
public class LanceExplainRequestTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, List.of(new LancePlugin())).getNamedWriteables()
    );

    public void testIndicesSetterReplacesTheIndexAndKeepsTheSource() throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termQuery("category", "books")).size(3);
        LanceExplainRequest request = new LanceExplainRequest("perf20m", source);
        assertTrue(request instanceof IndicesRequest.Replaceable);

        IndicesRequest returned = request.indices("perf20m-resolved");
        assertSame("the setter returns the request for chaining", request, returned);
        assertEquals("perf20m-resolved", request.index());
        assertArrayEquals(new String[] { "perf20m-resolved" }, request.indices());
        assertSame(source, request.source());

        LanceExplainRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (
                StreamInput raw = out.bytes().streamInput();
                NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(raw, REGISTRY)
            ) {
                restored = new LanceExplainRequest(in);
            }
        }
        assertEquals("perf20m-resolved", restored.index());
        assertArrayEquals(new String[] { "perf20m-resolved" }, restored.indices());
        assertEquals(source, restored.source());
    }

    public void testIndicesSetterRefusesAnythingButOneName() {
        LanceExplainRequest request = new LanceExplainRequest("perf20m", null);
        IllegalArgumentException two = expectThrows(IllegalArgumentException.class, () -> request.indices("a", "b"));
        assertTrue(two.getMessage(), two.getMessage().contains("exactly one index"));
        assertTrue(two.getMessage(), two.getMessage().contains("[a, b]"));
        expectThrows(IllegalArgumentException.class, () -> request.indices(new String[0]));
        expectThrows(IllegalArgumentException.class, () -> request.indices((String[]) null));
        expectThrows(IllegalArgumentException.class, () -> request.indices(""));
        expectThrows(IllegalArgumentException.class, () -> request.indices((String) null));
        assertEquals("a refused call leaves the index as it was", "perf20m", request.index());
    }

    public void testIndicesOptionsStayStrict() {
        IndicesOptions options = new LanceExplainRequest("perf20m", null).indicesOptions();
        assertFalse(options.expandWildcardsOpen());
        assertFalse(options.expandWildcardsClosed());
        assertFalse(options.ignoreUnavailable());
        assertFalse(options.allowAliasesToMultipleIndices());
    }
}

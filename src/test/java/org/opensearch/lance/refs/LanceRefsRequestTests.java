/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.refs;

import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The {@link IndicesRequest.Replaceable} contract of the refs request:
 * what a security plugin reads through {@code indices()} and writes
 * back through {@code indices(String...)}, and that the written name
 * is what the transport action and the wire see.
 */
public class LanceRefsRequestTests extends OpenSearchTestCase {

    public void testIndicesSetterReplacesTheIndexEverywhere() throws Exception {
        LanceRefsRequest request = new LanceRefsRequest("perf20m");
        assertTrue(request instanceof IndicesRequest.Replaceable);

        IndicesRequest returned = request.indices("perf20m-resolved");
        assertSame("the setter returns the request for chaining", request, returned);
        assertEquals("perf20m-resolved", request.index());
        assertArrayEquals(new String[] { "perf20m-resolved" }, request.indices());

        LanceRefsRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceRefsRequest(in);
            }
        }
        assertEquals("perf20m-resolved", restored.index());
        assertArrayEquals(new String[] { "perf20m-resolved" }, restored.indices());
    }

    public void testIndicesSetterRefusesAnythingButOneName() {
        LanceRefsRequest request = new LanceRefsRequest("perf20m");
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
        IndicesOptions options = new LanceRefsRequest("perf20m").indicesOptions();
        assertFalse(options.expandWildcardsOpen());
        assertFalse(options.expandWildcardsClosed());
        assertFalse(options.ignoreUnavailable());
        assertFalse(options.allowAliasesToMultipleIndices());
    }
}

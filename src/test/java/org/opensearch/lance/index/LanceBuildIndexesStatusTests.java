/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.io.IOException;
import java.util.List;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.lance.engine.LanceIndexBuilder.Failed;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The status rule of {@code POST /_lance/build_indexes/{index}}: skipped
 * columns are not failures, Lance's invalid-input rejections are the
 * caller's fault (400), anything else Lance throws is 500, and a mix is
 * 500 because that is the one the operator has to act on.
 */
public class LanceBuildIndexesStatusTests extends OpenSearchTestCase {

    public void testNoFailureIsOk() {
        assertEquals(RestStatus.OK, TransportLanceBuildIndexesAction.statusOf(List.of()));
    }

    public void testInvalidInputOnlyIsBadRequest() {
        Failed tokenizer = Failed.of("text", new IllegalArgumentException("unknown base tokenizer x"));
        assertTrue(tokenizer.invalidInput());
        assertEquals(RestStatus.BAD_REQUEST, TransportLanceBuildIndexesAction.statusOf(List.of(tokenizer)));
        assertEquals(RestStatus.BAD_REQUEST, TransportLanceBuildIndexesAction.statusOf(List.of(tokenizer, tokenizer)));
    }

    public void testIoFailureIsInternalServerError() {
        Failed io = Failed.of("rating", new IOException("LanceError(IO): Permission denied (os error 13)"));
        assertFalse(io.invalidInput());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, TransportLanceBuildIndexesAction.statusOf(List.of(io)));
    }

    public void testMixedFailuresStayInternalServerError() {
        Failed io = Failed.of("rating", new IOException("LanceError(IO): Permission denied (os error 13)"));
        Failed tokenizer = Failed.of("text", new IllegalArgumentException("unknown base tokenizer x"));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, TransportLanceBuildIndexesAction.statusOf(List.of(tokenizer, io)));
    }

    public void testWrappedReasonKeepsClassification() {
        // The optimize path prefixes the reason with the index names but
        // must not lose Lance's invalid-input classification.
        Failed wrapped = new Failed("body", "optimize of [body_fts] failed: Invalid user input: x", true);
        assertEquals(RestStatus.BAD_REQUEST, TransportLanceBuildIndexesAction.statusOf(List.of(wrapped)));
    }

    public void testFailedReasonFallsBackToExceptionStringWhenMessageIsMissing() {
        assertEquals("java.lang.IllegalStateException", Failed.of("id", new IllegalStateException()).reason());
        assertEquals("boom", Failed.of("id", new IOException("boom")).reason());
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.query;

import java.io.IOException;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.RemoteTransportException;

public class LanceInvalidInputTests extends OpenSearchTestCase {

    private static final String LANCE_MESSAGE =
        "Invalid user input: position is not found but required for phrase queries, try recreating the index with position";

    /** An IllegalArgumentException whose top frame is a Lance SDK native method, as the JNI layer produces. */
    private static IllegalArgumentException thrownByLance(String message) {
        IllegalArgumentException e = new IllegalArgumentException(message);
        e.setStackTrace(
            new StackTraceElement[] {
                new StackTraceElement("org.lance.ipc.LanceScanner", "scanBatches", null, -2),
                new StackTraceElement("org.opensearch.lance.query.LanceFtsQuery$LanceFtsWeight", "collectHits", "LanceFtsQuery.java", 1) }
        );
        return e;
    }

    public void testIsInvalidInputAcceptsLanceMessageOrLanceFrame() {
        assertTrue(LanceInvalidInput.isInvalidInput(new IllegalArgumentException(LANCE_MESSAGE)));
        assertTrue(LanceInvalidInput.isInvalidInput(thrownByLance("Dataset not found: /tmp/nope.lance")));
        assertTrue(LanceInvalidInput.isInvalidInput(thrownByLance(null)));
    }

    public void testIsInvalidInputRejectsIllegalArgumentsFromOtherCode() {
        // The plugin's own validation and Lucene's both throw
        // IllegalArgumentException; neither is the client's mistake.
        assertFalse(LanceInvalidInput.isInvalidInput(new IllegalArgumentException("columns must not be empty")));
        assertFalse(LanceInvalidInput.isInvalidInput(new IllegalArgumentException((String) null)));
        assertFalse(LanceInvalidInput.isInvalidInput(new IOException(LANCE_MESSAGE)));
        assertFalse(LanceInvalidInput.isInvalidInput(null));
    }

    public void testFindReturnsTheLanceExceptionBehindAnIoException() {
        IllegalArgumentException lance = new IllegalArgumentException(LANCE_MESSAGE);
        IOException wrapped = new IOException(lance);
        assertSame(lance, LanceInvalidInput.find(wrapped));
        assertSame(lance, LanceInvalidInput.find(new RuntimeException("outer", wrapped)));
        assertSame(lance, LanceInvalidInput.find(lance));
        // A non Lance IllegalArgumentException above the Lance one is
        // passed over, not reported.
        IllegalArgumentException lanceByFrame = thrownByLance("Commit conflict for version 3");
        assertSame(lanceByFrame, LanceInvalidInput.find(new IllegalArgumentException("plugin wrapper", lanceByFrame)));
    }

    public void testFindReturnsNullWhenTheChainHoldsNoLanceException() {
        assertNull(LanceInvalidInput.find(new IOException("read failed")));
        assertNull(LanceInvalidInput.find(new IOException(new RuntimeException("LanceError(IO): Permission denied"))));
        assertNull(LanceInvalidInput.find(new IOException(new IllegalArgumentException("scanLimit must not be negative, was -1"))));
        assertNull(LanceInvalidInput.find(null));
    }

    public void testFindStopsOnACyclicChain() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);
        assertNull(LanceInvalidInput.find(a));
    }

    public void testUnwrapMapsTheWrappedLanceExceptionToBadRequestAndLeavesOthersAlone() {
        IllegalArgumentException lance = new IllegalArgumentException(LANCE_MESSAGE);
        Exception reported = LanceInvalidInput.unwrap(new IOException(lance));
        assertSame(lance, reported);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(reported));
        assertEquals(LANCE_MESSAGE, reported.getMessage());

        IOException io = new IOException(new RuntimeException("LanceError(IO): Permission denied"));
        assertSame(io, LanceInvalidInput.unwrap(io));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(io));

        IOException pluginBug = new IOException(new IllegalArgumentException("scanLimit must not be negative, was -1"));
        assertSame(pluginBug, LanceInvalidInput.unwrap(pluginBug));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(pluginBug));
    }

    public void testUnwrapRejectsNull() {
        expectThrows(NullPointerException.class, () -> LanceInvalidInput.unwrap(null));
    }

    /**
     * The coordinator receives the executor's failure wrapped in a
     * {@link RemoteTransportException} after a wire round trip. The
     * REST layer unwraps that wrapper, so the status and the exception
     * name the client sees come from the transported exception.
     */
    public void testStatusSurvivesTheWireAndTheRemoteTransportWrapper() throws IOException {
        Exception reported = LanceInvalidInput.unwrap(new IOException(new IllegalArgumentException(LANCE_MESSAGE)));
        Exception transported;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(reported);
            try (StreamInput in = out.bytes().streamInput()) {
                transported = in.readException();
            }
        }
        RemoteTransportException remote = new RemoteTransportException("lance fragment query", transported);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(remote));
        Throwable unwrapped = ExceptionsHelper.unwrapCause(remote);
        assertEquals("illegal_argument_exception", OpenSearchException.getExceptionName(unwrapped));
        assertEquals(LANCE_MESSAGE, unwrapped.getMessage());
    }
}

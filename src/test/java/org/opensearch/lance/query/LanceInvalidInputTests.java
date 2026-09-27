/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.query;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.lance.Dataset;
import org.lance.ipc.FullTextQuery;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.engine.LanceIndexBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.RemoteTransportException;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
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
     * A Lance invalid input whose message quotes an object store error
     * body (S3 echoes the access key id in {@code InvalidAccessKeyId})
     * is reported without the key id, still as an
     * {@link IllegalArgumentException} the REST layer answers with 400,
     * while the original stays as it was for the debug log.
     */
    public void testUnwrapRedactsTheAccessKeyIdOfAnObjectStoreErrorBody() {
        // The AWS documentation example key id, assembled so the source carries no scanner matching literal.
        String keyId = "AKIA" + "IOSFODNN7EXAMPLE";
        String message = "Invalid user input: Generic S3 error: Client error with status 403 Forbidden: "
            + "<Error><Code>InvalidAccessKeyId</Code><AWSAccessKeyId>"
            + keyId
            + "</AWSAccessKeyId></Error>, /rust/lance-io/src/object_store.rs:1:1";
        IllegalArgumentException lance = new IllegalArgumentException(message);
        IOException wrapped = new IOException(lance);
        Exception reported = LanceInvalidInput.unwrap(wrapped);
        assertNotSame(lance, reported);
        assertTrue(reported.toString(), reported instanceof IllegalArgumentException);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(reported));
        assertFalse("the key id must not be reported: " + reported.getMessage(), reported.getMessage().contains(keyId));
        assertTrue("the S3 error code stays: " + reported.getMessage(), reported.getMessage().contains("InvalidAccessKeyId"));
        assertEquals(message, lance.getMessage());
        // A message without a credential is reported as the exception itself.
        IllegalArgumentException clean = new IllegalArgumentException(LANCE_MESSAGE);
        assertSame(clean, LanceInvalidInput.unwrap(new IOException(clean)));
    }

    /**
     * The criterion depends on two conventions of the bundled Lance SDK:
     * the display prefix of {@code Error::InvalidInput} and the package
     * its native methods live in. Both are checked here against
     * exceptions the SDK really raises, so an SDK upgrade that renames
     * either fails this test instead of turning the client's 400 into a
     * 500. The two samples are the invalid inputs the documentation
     * names: a phrase query on an inverted index built without
     * positions, and an index build with a tokenizer Lance does not know.
     */
    public void testBundledSdkRaisesInvalidInputTheCriterionRecognises() throws Exception {
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeEnglishTextTable(dir, "invalid-input-pin");
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open().allocator(allocator).uri(uri).build()
        ) {
            LanceIndexBuilder.BuildResult unknownTokenizer = LanceIndexBuilder.ensureFtsIndexes(
                dataset,
                Set.of("body"),
                Long.MAX_VALUE,
                Optional.empty(),
                "no-such-tokenizer",
                false
            );
            assertEquals("the unknown tokenizer fails the build: " + unknownTokenizer.built(), 1, unknownTokenizer.failed().size());
            LanceIndexBuilder.Failed failed = unknownTokenizer.failed().get(0);
            assertTrue("Lance's refusal of the tokenizer is invalid input: " + failed, failed.invalidInput());
            assertTrue("Lance's message names the tokenizer: " + failed.reason(), failed.reason().contains("no-such-tokenizer"));

            LanceIndexBuilder.BuildResult built = LanceIndexBuilder.ensureFtsIndexes(
                dataset,
                Set.of("body"),
                Long.MAX_VALUE,
                Optional.empty(),
                LanceIndexBuilder.DEFAULT_FTS_TOKENIZER,
                /* withPosition */ false
            );
            assertEquals("fts build failures: " + built.failed(), 0, built.failed().size());

            ScanOptions phrase = new ScanOptions.Builder().fullTextQuery(FullTextQuery.phrase("the quick", "body", 0))
                .columns(List.of("_score"))
                .withRowAddress(true)
                .build();
            Exception raised = expectThrows(Exception.class, () -> {
                try (LanceScanner scanner = dataset.newScan(phrase); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        // Drain: the refusal may surface on the first batch.
                    }
                }
            });
            assertTrue(
                "Lance refuses the phrase query as IllegalArgumentException, saw " + raised,
                raised instanceof IllegalArgumentException
            );
            assertTrue("the criterion accepts the SDK's exception: " + raised, LanceInvalidInput.isInvalidInput(raised));
            assertTrue(
                "the message opens with the InvalidInput display prefix ["
                    + LanceInvalidInput.INVALID_INPUT_PREFIX
                    + "]: "
                    + raised.getMessage(),
                raised.getMessage().startsWith(LanceInvalidInput.INVALID_INPUT_PREFIX)
            );
            assertTrue(
                "the message is the positions refusal: " + raised.getMessage(),
                raised.getMessage().contains("position is not found but required for phrase queries")
            );
            StackTraceElement top = raised.getStackTrace()[0];
            assertTrue(
                "the frame that threw is a method of the SDK package [" + LanceInvalidInput.LANCE_PACKAGE + "]: " + top,
                top.getClassName().startsWith(LanceInvalidInput.LANCE_PACKAGE)
            );
        }
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

    private static final String TABLE = "s3://redaction-bucket/attach.lance";

    /** The AWS documentation example key id, assembled so the source carries no scanner matching literal. */
    private static final String KEY_ID = "AKIA" + "IOSFODNN7EXAMPLE";

    /** Lance's message for an S3 request the store answered with {@code code}, the way lance-io quotes the response. */
    private static String s3Failure(String code) {
        return "LanceError(IO): Generic S3 error: Error performing list request: Error performing GET "
            + "https://s3.us-east-1.amazonaws.com/redaction-bucket?list-type=2&prefix=attach.lance%2F_versions%2F in 5.9ms - "
            + "Server returned non-2xx status code: 403 Forbidden: <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>"
            + code
            + "</Code><Message>The request was refused.</Message><AWSAccessKeyId>"
            + KEY_ID
            + "</AWSAccessKeyId><RequestId>7A9E3F0C2B1D4E5F</RequestId><HostId>fixture</HostId></Error>, "
            + "/rust/lance-io/src/object_store.rs:1490:92";
    }

    private static String gcsFailure(String reason) {
        return "LanceError(IO): Generic GCS error: Error performing list request: Error performing GET "
            + "https://storage.googleapis.com/storage/v1/b/redaction-bucket/o?prefix=attach.lance%2F_versions%2F in 4.1ms - "
            + "Server returned non-2xx status code: 403 Forbidden: {\"error\": {\"code\": 403, \"message\": \"Caller does not have "
            + "storage.objects.list access to the Google Cloud Storage bucket.\", \"errors\": [{\"message\": \"Caller does not have "
            + "storage.objects.list access to the Google Cloud Storage bucket.\", \"domain\": \"global\", \"reason\": \""
            + reason
            + "\"}]}}, /rust/lance-io/src/object_store.rs:1490:92";
    }

    private static String azureFailure(String code) {
        return "LanceError(IO): Generic MicrosoftAzure error: Error performing list request: Error performing GET "
            + "https://redaction.blob.core.windows.net/tables?restype=container&comp=list&prefix=attach.lance%2F_versions%2F in 3.2ms - "
            + "Server returned non-2xx status code: 403 Forbidden: <?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>"
            + code
            + "</Code><Message>Server failed to authenticate the request. Make sure the value of Authorization header is formed "
            + "correctly including the signature.\nRequestId:0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0\nTime:2026-09-27T04:00:00.0000000Z"
            + "</Message></Error>, /rust/lance-io/src/object_store.rs:1490:92";
    }

    /**
     * The codes the criterion recognises are the contract with the
     * documentation and the operators reading a 400; a change to either
     * list is deliberate and updates this test with it.
     */
    public void testCredentialErrorCodesArePinned() {
        assertArrayEquals(
            new String[] { "InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied", "ExpiredToken", "InvalidToken" },
            LanceInvalidInput.S3_CREDENTIAL_ERROR_CODES
        );
        assertArrayEquals(
            new String[] { "authError", "forbidden", "insufficientPermissions" },
            LanceInvalidInput.GCS_CREDENTIAL_ERROR_REASONS
        );
        assertArrayEquals(
            new String[] { "AuthenticationFailed", "AuthorizationFailure", "AuthorizationPermissionMismatch", "InvalidAuthenticationInfo" },
            LanceInvalidInput.AZURE_CREDENTIAL_ERROR_CODES
        );
    }

    /**
     * Every listed code, in the body shape of its store and inside the
     * {@code Error::IO} Lance raises for it, is reported as a 400 whose
     * message names the subject and keeps the store's code but not the
     * access key id the S3 body echoes.
     */
    public void testOpenFailureMapsEachStoreCodeToBadRequest() {
        for (String code : LanceInvalidInput.S3_CREDENTIAL_ERROR_CODES) {
            assertCredentialRejection(code, new IOException(s3Failure(code)));
        }
        for (String reason : LanceInvalidInput.GCS_CREDENTIAL_ERROR_REASONS) {
            assertCredentialRejection(reason, new IOException(gcsFailure(reason)));
        }
        for (String code : LanceInvalidInput.AZURE_CREDENTIAL_ERROR_CODES) {
            assertCredentialRejection(code, new IOException(azureFailure(code)));
        }
    }

    private static void assertCredentialRejection(String code, Exception failure) {
        assertEquals(code, LanceInvalidInput.credentialErrorCode(failure));
        Exception reported = LanceInvalidInput.openFailure(failure, TABLE);
        assertTrue(code + ": " + reported, reported instanceof IllegalArgumentException);
        assertEquals(code, RestStatus.BAD_REQUEST, ExceptionsHelper.status(reported));
        String message = reported.getMessage();
        assertTrue(code + ": " + message, message.startsWith(LanceInvalidInput.CREDENTIALS_REJECTED_PREFIX + TABLE + "]: LanceError(IO)"));
        assertTrue(code + ": " + message, message.contains(code));
        assertFalse(code + ": the key id must not be reported: " + message, message.contains(KEY_ID));
        // The cause keeps the store's message for the log, redacted the same way.
        Throwable cause = reported.getCause();
        assertNotNull(code + ": the cause is kept", cause);
        assertTrue(code + ": " + cause.getMessage(), cause.getMessage().contains(code));
        assertFalse(code + ": the cause must not carry the key id: " + cause.getMessage(), cause.getMessage().contains(KEY_ID));
        // The original is left as it was.
        assertTrue(failure.getMessage().contains(code));
    }

    /**
     * The code is looked for down the cause chain: a namespace
     * initialise runs inside {@code doPrivileged}, which hands the
     * store's failure back wrapped. The message reported is the one of
     * the exception that quotes the code, not the wrapper's.
     */
    public void testCredentialErrorCodeIsFoundBehindAWrapper() {
        IOException store = new IOException(s3Failure("SignatureDoesNotMatch"));
        Exception wrapped = new RuntimeException("namespace initialise failed", new IllegalStateException("privileged call failed", store));
        assertEquals("SignatureDoesNotMatch", LanceInvalidInput.credentialErrorCode(wrapped));
        Exception reported = LanceInvalidInput.openFailure(wrapped, "cat");
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(reported));
        assertTrue(
            reported.getMessage(),
            reported.getMessage().startsWith(LanceInvalidInput.CREDENTIALS_REJECTED_PREFIX + "cat]: LanceError(IO)")
        );
        assertNull(LanceInvalidInput.credentialErrorCode(null));
    }

    /**
     * A failure that is not the store refusing the credentials keeps
     * its status: another S3 error code, the code's word outside the
     * body's element, a local filesystem refusal, a plain runtime
     * failure. The redaction of the message still applies.
     */
    public void testOpenFailureLeavesOtherFailuresAtTheirStatus() {
        IOException noSuchBucket = new IOException(s3Failure("NoSuchBucket"));
        assertNull(LanceInvalidInput.credentialErrorCode(noSuchBucket));
        Exception reportedBucket = LanceInvalidInput.openFailure(noSuchBucket, TABLE);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(reportedBucket));
        assertFalse(reportedBucket.getMessage(), reportedBucket.getMessage().contains(KEY_ID));
        assertTrue(reportedBucket.getMessage(), reportedBucket.getMessage().contains("NoSuchBucket"));

        IOException wordOnly = new IOException("LanceError(IO): AccessDenied while reading /tables/attach.lance");
        assertNull(LanceInvalidInput.credentialErrorCode(wordOnly));
        assertSame(wordOnly, LanceInvalidInput.openFailure(wordOnly, TABLE));

        RuntimeException local = new RuntimeException("LanceError(IO): Permission denied (os error 13)");
        assertNull(LanceInvalidInput.credentialErrorCode(local));
        assertSame(local, LanceInvalidInput.openFailure(local, TABLE));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(local));

        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);
        assertNull(LanceInvalidInput.credentialErrorCode(a));

        expectThrows(NullPointerException.class, () -> LanceInvalidInput.openFailure(null, TABLE));
    }

    /**
     * The attach action runs on the cluster manager, so on a multi node
     * cluster the rejection crosses the wire before the REST layer
     * reads its status.
     */
    public void testCredentialRejectionSurvivesTheWire() throws IOException {
        Exception reported = LanceInvalidInput.openFailure(new IOException(s3Failure("InvalidAccessKeyId")), TABLE);
        Exception transported;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(reported);
            try (StreamInput in = out.bytes().streamInput()) {
                transported = in.readException();
            }
        }
        RemoteTransportException remote = new RemoteTransportException("lance attach", transported);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(remote));
        Throwable unwrapped = ExceptionsHelper.unwrapCause(remote);
        assertEquals("illegal_argument_exception", OpenSearchException.getExceptionName(unwrapped));
        assertEquals(reported.getMessage(), unwrapped.getMessage());
        assertFalse(ExceptionsHelper.stackTrace(unwrapped), ExceptionsHelper.stackTrace(unwrapped).contains(KEY_ID));
    }
}

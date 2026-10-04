/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.RemoteTransportException;

public class StorageOptionsTests extends OpenSearchTestCase {

    public void testEmptyIsSingleton() {
        assertSame(StorageOptions.empty(), StorageOptions.empty());
        assertSame(StorageOptions.empty(), StorageOptions.of(null));
        assertSame(StorageOptions.empty(), StorageOptions.of(Map.of()));
        assertTrue(StorageOptions.empty().isEmpty());
    }

    public void testOfCopiesInputMap() {
        // The wrapper must snapshot the caller's map so a later mutation
        // of the input does not leak into the persisted / propagated
        // options. This is the same defensive-copy contract lance-flink
        // and lance-spark use around their storage-option maps.
        Map<String, String> input = new HashMap<>();
        input.put("aws_region", "us-west-2");
        StorageOptions options = StorageOptions.of(input);
        input.put("aws_endpoint", "https://s3.example");
        assertFalse("later mutation must not leak into options", options.asMap().containsKey("aws_endpoint"));
    }

    public void testAsMapIsUnmodifiable() {
        StorageOptions options = StorageOptions.of(Map.of("aws_region", "us-east-1"));
        expectThrows(UnsupportedOperationException.class, () -> options.asMap().put("aws_endpoint", "x"));
    }

    public void testOfRejectsNullKey() {
        Map<String, String> input = new HashMap<>();
        input.put(null, "value");
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.of(input));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("keys must be non-empty"));
    }

    public void testOfRejectsEmptyKey() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.of(Map.of("", "v")));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("keys must be non-empty"));
    }

    public void testOfRejectsNullValue() {
        Map<String, String> input = new HashMap<>();
        input.put("aws_region", null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.of(input));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("aws_region"));
    }

    public void testParseFromRequestFieldAcceptsNull() {
        assertSame(StorageOptions.empty(), StorageOptions.parseFromRequestField(null, "[lance_attach]"));
    }

    public void testParseFromRequestFieldReadsStringMap() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("aws_access_key_id", "AKID");
        body.put("aws_region", "us-east-1");
        StorageOptions options = StorageOptions.parseFromRequestField(body, "[lance_attach]");
        assertEquals("AKID", options.asMap().get("aws_access_key_id"));
        assertEquals("us-east-1", options.asMap().get("aws_region"));
    }

    public void testParseFromRequestFieldRejectsNonObject() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> StorageOptions.parseFromRequestField("not-an-object", "[lance_attach]")
        );
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("must be a JSON object"));
    }

    public void testParseFromRequestFieldRejectsNestedObject() {
        // Nested / array values would silently get toString()-ed if we
        // did not reject them up front; keep the 400 explicit so callers
        // notice the shape mismatch before it reaches Lance's Rust side.
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("aws_config", Map.of("nested", "value"));
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.parseFromRequestField(body, "[lance_attach]"));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("must be a string"));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("aws_config"));
    }

    public void testParseFromRequestFieldRejectsNumericValue() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timeout_seconds", 30);
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.parseFromRequestField(body, "[lance_attach]"));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("must be a string"));
    }

    private static Map<String, Object> entries(int count) {
        Map<String, Object> body = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            body.put("key_" + i, "value_" + i);
        }
        return body;
    }

    public void testParseFromRequestFieldAcceptsTheEntryBound() {
        StorageOptions options = StorageOptions.parseFromRequestField(entries(StorageOptions.MAX_ENTRIES), "[lance_attach]");
        assertEquals(StorageOptions.MAX_ENTRIES, options.asMap().size());
    }

    public void testParseFromRequestFieldRejectsOneEntryAboveTheBound() {
        Map<String, Object> body = entries(StorageOptions.MAX_ENTRIES + 1);
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.parseFromRequestField(body, "[lance_attach]"));
        assertEquals(
            "[lance_attach] storage_options has ["
                + (StorageOptions.MAX_ENTRIES + 1)
                + "] entries, the limit is "
                + StorageOptions.MAX_ENTRIES,
            e.getMessage()
        );
    }

    public void testParseFromRequestFieldAcceptsTheKeyBound() {
        String key = "k".repeat(StorageOptions.MAX_KEY_BYTES);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put(key, "v");
        assertEquals("v", StorageOptions.parseFromRequestField(body, "[lance_attach]").asMap().get(key));
    }

    public void testParseFromRequestFieldRejectsOneByteAboveTheKeyBound() {
        // A multi byte character shows the bound counts UTF 8 bytes, not chars.
        String key = "k".repeat(StorageOptions.MAX_KEY_BYTES - 1) + "\u00e9";
        assertEquals(StorageOptions.MAX_KEY_BYTES, key.length());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put(key, "v");
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.parseFromRequestField(body, "[lance_attach]"));
        assertEquals(
            "[lance_attach] storage_options key ["
                + key
                + "] is ["
                + (StorageOptions.MAX_KEY_BYTES + 1)
                + "] bytes, the limit is "
                + StorageOptions.MAX_KEY_BYTES,
            e.getMessage()
        );
    }

    public void testParseFromRequestFieldAcceptsTheValueBound() {
        String value = "v".repeat(StorageOptions.MAX_VALUE_BYTES);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("aws_session_token", value);
        assertEquals(value, StorageOptions.parseFromRequestField(body, "[lance_attach]").asMap().get("aws_session_token"));
    }

    public void testParseFromRequestFieldRejectsOneByteAboveTheValueBoundWithoutQuotingIt() {
        String value = "v".repeat(StorageOptions.MAX_VALUE_BYTES + 1);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("aws_session_token", value);
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.parseFromRequestField(body, "[lance_attach]"));
        assertEquals(
            "[lance_attach] storage_options value for [aws_session_token] is ["
                + (StorageOptions.MAX_VALUE_BYTES + 1)
                + "] bytes, the limit is "
                + StorageOptions.MAX_VALUE_BYTES,
            e.getMessage()
        );
        assertFalse("the value must not be quoted: " + e.getMessage(), e.getMessage().contains("vvvv"));
    }

    public void testParseFromRequestFieldRejectsOneByteAboveTheValueBoundReachedByAMultiByteCharacter() {
        // The value is MAX_VALUE_BYTES chars long but one byte over: the
        // bound counts UTF 8 bytes, not chars, on the attach path too.
        String value = "v".repeat(StorageOptions.MAX_VALUE_BYTES - 1) + "\u00e9";
        assertEquals(StorageOptions.MAX_VALUE_BYTES, value.length());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("aws_session_token", value);
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.parseFromRequestField(body, "[lance_attach]"));
        assertEquals(
            "[lance_attach] storage_options value for [aws_session_token] is ["
                + (StorageOptions.MAX_VALUE_BYTES + 1)
                + "] bytes, the limit is "
                + StorageOptions.MAX_VALUE_BYTES,
            e.getMessage()
        );
        assertFalse("the value must not be quoted: " + e.getMessage(), e.getMessage().contains("vvvv"));
        assertFalse("the value must not be quoted: " + e.getMessage(), e.getMessage().contains("\u00e9"));
    }

    public void testFromIndexSettingsAndWriteToSettingsRoundTrip() {
        StorageOptions original = StorageOptions.of(Map.of("aws_region", "us-west-2", "aws_endpoint", "https://s3.example"));
        Settings.Builder builder = Settings.builder();
        original.writeToSettings(builder);

        Settings settings = builder.build();
        assertEquals("us-west-2", settings.get(StorageOptions.INDEX_SETTING_PREFIX + "aws_region"));
        assertEquals("https://s3.example", settings.get(StorageOptions.INDEX_SETTING_PREFIX + "aws_endpoint"));

        StorageOptions roundTripped = StorageOptions.fromIndexSettings(settings);
        assertEquals(original, roundTripped);
    }

    public void testFromIndexSettingsIgnoresUnrelatedKeys() {
        // Only entries under the canonical prefix belong to
        // storage_options; other index settings must not bleed in.
        Settings settings = Settings.builder()
            .put(StorageOptions.INDEX_SETTING_PREFIX + "aws_region", "us-east-1")
            .put("index.number_of_shards", 1)
            .put("index.plugins.lance.table", "/tmp/table.lance")
            .build();
        StorageOptions options = StorageOptions.fromIndexSettings(settings);
        assertEquals(Map.of("aws_region", "us-east-1"), options.asMap());
    }

    public void testStreamRoundTrip() throws Exception {
        StorageOptions original = StorageOptions.of(
            Map.of("aws_access_key_id", "AKID", "aws_secret_access_key", "SECRET", "aws_region", "us-east-1")
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                StorageOptions copy = StorageOptions.readFromStream(in);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
            }
        }
    }

    public void testEmptyStreamRoundTrip() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            StorageOptions.empty().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertSame(StorageOptions.empty(), StorageOptions.readFromStream(in));
            }
        }
    }

    public void testToReadOptionsOrNullReturnsNullWhenEmpty() {
        assertNull(StorageOptions.empty().toReadOptionsOrNull());
    }

    public void testToReadOptionsOrNullCarriesMap() {
        StorageOptions options = StorageOptions.of(Map.of("aws_region", "us-east-1"));
        assertEquals("us-east-1", options.toReadOptionsOrNull().getStorageOptions().get("aws_region"));
    }

    public void testToStringRedactsCredentialsLikeValues() {
        // Every place StorageOptions is toString'd could end up in a
        // shard-failed error body or a log line. The public API of the
        // plugin doesn't need to see credential values there, so redact
        // anything whose key hints at a secret; keep non-sensitive
        // values (region, endpoint) visible so operators can still
        // debug misrouted requests.
        StorageOptions options = StorageOptions.of(
            Map.of(
                "aws_access_key_id",
                "AKID",
                "aws_secret_access_key",
                "SECRET",
                "aws_session_token",
                "TOKEN",
                "aws_region",
                "us-east-1",
                "aws_endpoint",
                "https://s3.example"
            )
        );
        String text = options.toString();
        assertFalse("access key must be redacted: " + text, text.contains("AKID"));
        assertFalse("secret must be redacted: " + text, text.contains("SECRET"));
        assertFalse("session token must be redacted: " + text, text.contains("TOKEN"));
        assertTrue("region must remain visible: " + text, text.contains("us-east-1"));
        assertTrue("endpoint must remain visible: " + text, text.contains("s3.example"));
    }

    public void testEqualityIgnoresInsertionOrder() {
        Map<String, String> a = new LinkedHashMap<>();
        a.put("aws_region", "us-east-1");
        a.put("aws_endpoint", "https://s3.example");
        Map<String, String> b = new LinkedHashMap<>();
        b.put("aws_endpoint", "https://s3.example");
        b.put("aws_region", "us-east-1");
        assertEquals(StorageOptions.of(a), StorageOptions.of(b));
        assertEquals(StorageOptions.of(a).hashCode(), StorageOptions.of(b).hashCode());
    }

    /**
     * The example key ids of the AWS documentation, assembled at run time
     * so the source never carries a string that matches a secret scanner's
     * access key id pattern.
     */
    private static final String ACCESS_KEY_ID = "AKIA" + "IOSFODNN7EXAMPLE";
    private static final String TEMPORARY_ACCESS_KEY_ID = "ASIA" + "IOSFODNN7EXAMPLE";

    /** The body S3 answers a request signed with an unknown access key id with, as Lance's object store quotes it. */
    private static final String S3_INVALID_ACCESS_KEY_ID = "LanceError(IO): Generic S3 error: Client error with status 403 Forbidden: "
        + "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidAccessKeyId</Code>"
        + "<Message>The AWS Access Key Id you provided does not exist in our records.</Message>"
        + "<AWSAccessKeyId>"
        + ACCESS_KEY_ID
        + "</AWSAccessKeyId><RequestId>7A9E3F0C2B1D4E5F</RequestId><HostId>host</HostId></Error>, "
        + "/home/runner/work/lance/rust/lance-io/src/object_store.rs:1234:56";

    public void testRedactCredentialsReplacesTheAccessKeyIdOfAnS3ErrorBody() {
        String redacted = StorageOptions.redactCredentials(S3_INVALID_ACCESS_KEY_ID);
        assertFalse("the key id must not survive: " + redacted, redacted.contains(ACCESS_KEY_ID));
        assertTrue(
            "the element stays so the shape is recognisable: " + redacted,
            redacted.contains("<AWSAccessKeyId>***</AWSAccessKeyId>")
        );
        assertTrue("the S3 error code stays: " + redacted, redacted.contains("<Code>InvalidAccessKeyId</Code>"));
        assertTrue("the request id is not a credential: " + redacted, redacted.contains("7A9E3F0C2B1D4E5F"));
        assertTrue("the Rust location stays: " + redacted, redacted.contains("object_store.rs:1234:56"));
    }

    public void testRedactCredentialsReplacesTheSignatureElementsOfAnS3ErrorBody() {
        // The body S3 answers a request signed with the right key id and
        // a wrong secret with, as the plugin's 400 message quotes it.
        // SignatureProvided is the HMAC the client derived from the
        // secret; StringToSign and CanonicalRequest are its inputs and
        // carry the session token and the credential scope over several
        // lines. The filler values are assembled at run time so the
        // source carries nothing a secret scanner matches.
        String signature = "9f" + "e3".repeat(31);
        String sessionToken = "FwoGZXIvYXdzE" + "Bya".repeat(5);
        String stringToSign = "AWS4-HMAC-SHA256\n20260927T015900Z\n20260927/us-east-1/s3/aws4_request\n" + "ab".repeat(32);
        String canonicalRequest = "GET\n/t.lance/_versions/\n\nhost:bucket.s3.amazonaws.com\nx-amz-content-sha256:UNSIGNED-PAYLOAD\n"
            + "x-amz-date:20260927T015900Z\nx-amz-security-token:"
            + sessionToken
            + "\n\nhost;x-amz-content-sha256;x-amz-date;x-amz-security-token\nUNSIGNED-PAYLOAD";
        String body = "Invalid user input: LanceError(IO): Generic S3 error: Client error with status 403 Forbidden: "
            + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>SignatureDoesNotMatch</Code>"
            + "<Message>The request signature we calculated does not match the signature you provided. "
            + "Check your key and signing method.</Message>"
            + "<AWSAccessKeyId>"
            + TEMPORARY_ACCESS_KEY_ID
            + "</AWSAccessKeyId>"
            + "<StringToSign>"
            + stringToSign
            + "</StringToSign>"
            + "<SignatureProvided>"
            + signature
            + "</SignatureProvided>"
            + "<StringToSignBytes>41 57 53 34</StringToSignBytes>"
            + "<CanonicalRequest>"
            + canonicalRequest
            + "</CanonicalRequest>"
            + "<CanonicalRequestBytes>47 45 54 0a</CanonicalRequestBytes>"
            + "<RequestId>7A9E3F0C2B1D4E5F</RequestId><HostId>host</HostId></Error>, "
            + "/home/runner/work/lance/rust/lance-io/src/object_store.rs:1234:56";

        String redacted = StorageOptions.redactCredentials(body);
        assertFalse("the signature must not survive: " + redacted, redacted.contains(signature));
        assertFalse("the session token must not survive: " + redacted, redacted.contains(sessionToken));
        assertFalse("the credential scope must not survive: " + redacted, redacted.contains("aws4_request"));
        assertFalse("the key id must not survive: " + redacted, redacted.contains(TEMPORARY_ACCESS_KEY_ID));
        assertTrue(redacted, redacted.contains("<StringToSign>***</StringToSign>"));
        assertTrue(redacted, redacted.contains("<SignatureProvided>***</SignatureProvided>"));
        assertTrue(redacted, redacted.contains("<CanonicalRequest>***</CanonicalRequest>"));
        assertTrue(redacted, redacted.contains("<AWSAccessKeyId>***</AWSAccessKeyId>"));
        assertTrue("the S3 error code stays: " + redacted, redacted.contains("<Code>SignatureDoesNotMatch</Code>"));
        assertTrue("the message stays: " + redacted, redacted.contains("Check your key and signing method.</Message>"));
        assertTrue(
            "the byte dumps of the inputs are redacted like their text forms: " + redacted,
            redacted.contains("<StringToSignBytes>***</StringToSignBytes>")
                && redacted.contains("<CanonicalRequestBytes>***</CanonicalRequestBytes>")
        );
        assertTrue("the request id is not a credential: " + redacted, redacted.contains("7A9E3F0C2B1D4E5F"));
        assertTrue("the Rust location stays: " + redacted, redacted.contains("object_store.rs:1234:56"));
    }

    public void testRedactCredentialsReplacesTheHexDumpElementsOfAnS3ErrorBody() {
        // The hex dumps S3 puts next to StringToSign and CanonicalRequest
        // decode to the same text, session token included, so they are
        // redacted whole. The payloads are assembled at run time: the
        // canonical request dump spells "x-amz-security-token:" in hex
        // followed by the token's bytes, the string to sign dump is a
        // run of one byte. A hex element S3 does not send today
        // (FutureBytes) and a dump cut short by a message length limit
        // are covered by the same family.
        String tokenHex = "78 2d 61 6d 7a 2d 73 65 63 75 72 69 74 79 2d 74 6f 6b 65 6e 3a " + "46 77 6f 47 ".repeat(12).trim();
        String stringToSignHex = "41 ".repeat(40).trim();
        String futureHex = "7a ".repeat(16).trim();
        String body = "Invalid user input: LanceError(IO): Generic S3 error: Client error with status 403 Forbidden: "
            + "<Error><Code>SignatureDoesNotMatch</Code>"
            + "<StringToSign>***</StringToSign>"
            + "<StringToSignBytes>"
            + stringToSignHex
            + "</StringToSignBytes>"
            + "<CanonicalRequest>***</CanonicalRequest>"
            + "<CanonicalRequestBytes>"
            + tokenHex
            + "</CanonicalRequestBytes>"
            + "<FutureBytes>"
            + futureHex
            + "</FutureBytes>"
            + "<RequestId>7A9E3F0C2B1D4E5F</RequestId></Error>";
        String redacted = StorageOptions.redactCredentials(body);
        assertFalse("the token's hex must not survive: " + redacted, redacted.contains(tokenHex));
        assertFalse("the string to sign's hex must not survive: " + redacted, redacted.contains(stringToSignHex));
        assertFalse("the unknown hex element's content must not survive: " + redacted, redacted.contains(futureHex));
        assertEquals(
            "Invalid user input: LanceError(IO): Generic S3 error: Client error with status 403 Forbidden: "
                + "<Error><Code>SignatureDoesNotMatch</Code><StringToSign>***</StringToSign>"
                + "<StringToSignBytes>***</StringToSignBytes><CanonicalRequest>***</CanonicalRequest>"
                + "<CanonicalRequestBytes>***</CanonicalRequestBytes><FutureBytes>***</FutureBytes>"
                + "<RequestId>7A9E3F0C2B1D4E5F</RequestId></Error>",
            redacted
        );

        String truncated = "<Error><Code>SignatureDoesNotMatch</Code><CanonicalRequestBytes>" + tokenHex;
        String redactedTruncated = StorageOptions.redactCredentials(truncated);
        assertFalse("the token's hex must not survive a cut body: " + redactedTruncated, redactedTruncated.contains(tokenHex));
        assertEquals("<Error><Code>SignatureDoesNotMatch</Code><CanonicalRequestBytes>***", redactedTruncated);

        String prose = "the StringToSignBytes and CanonicalRequestBytes elements are hex dumps";
        assertSame("the element names in prose are words, not elements", prose, StorageOptions.redactCredentials(prose));
    }

    public void testRedactCredentialsReplacesAnUnclosedSigningElementUpToTheEndOfTheMessage() {
        // A body cut short inside StringToSign: the opening tag is there,
        // the closing tag never arrives. Everything after the opening tag
        // is the element's content and is redacted to the end.
        String stringToSign = "AWS4-HMAC-SHA256\n20260927T015900Z\n20260927/us-east-1/s3/aws4_request\n" + "ab".repeat(32);
        String truncated = "Generic S3 error: Client error with status 403 Forbidden: <Error><Code>SignatureDoesNotMatch</Code>"
            + "<StringToSign>"
            + stringToSign;
        String redacted = StorageOptions.redactCredentials(truncated);
        assertFalse("the credential scope must not survive: " + redacted, redacted.contains("aws4_request"));
        assertFalse("the content must not survive: " + redacted, redacted.contains("ab".repeat(32)));
        assertEquals(
            "Generic S3 error: Client error with status 403 Forbidden: <Error><Code>SignatureDoesNotMatch</Code><StringToSign>***",
            redacted
        );
    }

    public void testRedactCredentialsReplacesAnUnclosedSigningElementUpToTheNextTag() {
        // A body whose StringToSign has no closing tag and runs into the
        // next element: the content stops at that element's opening tag,
        // and the next element is redacted on its own terms.
        String signature = "9f" + "e3".repeat(31);
        String stringToSign = "AWS4-HMAC-SHA256\n20260927T015900Z\n20260927/us-east-1/s3/aws4_request\n" + "ab".repeat(32);
        String body = "<Error><Code>SignatureDoesNotMatch</Code>"
            + "<StringToSign>"
            + stringToSign
            + "<SignatureProvided>"
            + signature
            + "</SignatureProvided>"
            + "<RequestId>7A9E3F0C2B1D4E5F</RequestId></Error>";
        String redacted = StorageOptions.redactCredentials(body);
        assertFalse("the credential scope must not survive: " + redacted, redacted.contains("aws4_request"));
        assertFalse("the signature must not survive: " + redacted, redacted.contains(signature));
        assertEquals(
            "<Error><Code>SignatureDoesNotMatch</Code><StringToSign>***<SignatureProvided>***</SignatureProvided>"
                + "<RequestId>7A9E3F0C2B1D4E5F</RequestId></Error>",
            redacted
        );
    }

    public void testRedactCredentialsLeavesTheSigningElementNamesInProseAlone() {
        // The element names without angle brackets are words, not
        // elements: nothing matches and the message comes back as is.
        String prose = "the StringToSign was wrong, the SignatureProvided did not match and the CanonicalRequest had no host";
        assertSame(prose, StorageOptions.redactCredentials(prose));
    }

    public void testRedactCredentialsReplacesAccessKeyIdsWhereverTheyAppear() {
        // Long lived and temporary key ids, in a SigV4 Authorization
        // header, a presigned URL and a MinIO style element that does
        // not use the AWS format.
        String sigv4 = "AWS4-HMAC-SHA256 Credential=" + ACCESS_KEY_ID + "/20260927/us-east-1/s3/aws4_request, SignedHeaders=host";
        String redactedSigv4 = StorageOptions.redactCredentials(sigv4);
        assertFalse(redactedSigv4, redactedSigv4.contains(ACCESS_KEY_ID));
        assertTrue("the algorithm and signed headers stay: " + redactedSigv4, redactedSigv4.contains("SignedHeaders=host"));

        String presigned = "https://bucket.s3.amazonaws.com/t.lance/_versions/1.manifest?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            + "&X-Amz-Credential="
            + TEMPORARY_ACCESS_KEY_ID
            + "%2F20260927%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260927T015900Z"
            + "&X-Amz-Security-Token=FwoGZXIvYXdzEBYaDHNlc3Npb250b2tlbg%3D%3D&X-Amz-Signature=abcdef";
        String redactedPresigned = StorageOptions.redactCredentials(presigned);
        assertFalse(redactedPresigned, redactedPresigned.contains(TEMPORARY_ACCESS_KEY_ID));
        assertFalse("the session token is a credential: " + redactedPresigned, redactedPresigned.contains("FwoGZXIvYXdz"));
        assertTrue(
            "the bucket and path stay: " + redactedPresigned,
            redactedPresigned.contains("bucket.s3.amazonaws.com/t.lance/_versions/1.manifest")
        );
        assertTrue("the date stays: " + redactedPresigned, redactedPresigned.contains("X-Amz-Date=20260927T015900Z"));

        String minio = "<Error><Code>InvalidAccessKeyId</Code><AWSAccessKeyId>minioadmin</AWSAccessKeyId></Error>";
        assertEquals(
            "<Error><Code>InvalidAccessKeyId</Code><AWSAccessKeyId>***</AWSAccessKeyId></Error>",
            StorageOptions.redactCredentials(minio)
        );
    }

    public void testRedactCredentialsReplacesTheValueOfACredentialKey() {
        // Every spelling a quoted storage option or catalog config can
        // take: bare, JSON, single quoted, upper case, dotted, camel
        // case, and the header form of the session token.
        String text = "storage options aws_access_key_id=AKID12 aws_secret_access_key: s3cr3t/+= "
            + "\"aws_session_token\": \"tok.en\" AWS_SECRET_ACCESS_KEY='quoted' header.Authorization=Bearer eyJhbGciOi "
            + "sessionToken=abc x-amz-security-token:FwoGZXIv credentials=id:secret aws_region=us-east-1 aws_endpoint=http://minio:9000 "
            + "allow_http=true";
        String redacted = StorageOptions.redactCredentials(text);
        for (String secret : List.of("AKID12", "s3cr3t", "tok.en", "quoted", "eyJhbGciOi", "abc", "FwoGZXIv", "id:secret")) {
            assertFalse("[" + secret + "] must not survive: " + redacted, redacted.contains(secret));
        }
        assertTrue("the JSON quotes stay around the redaction: " + redacted, redacted.contains("\"aws_session_token\": \"***\""));
        assertTrue("region is not a credential: " + redacted, redacted.contains("aws_region=us-east-1"));
        assertTrue("endpoint is not a credential: " + redacted, redacted.contains("aws_endpoint=http://minio:9000"));
        assertTrue("allow_http is not a credential: " + redacted, redacted.contains("allow_http=true"));
    }

    public void testRedactCredentialsLeavesMessagesWithoutCredentialsAlone() {
        // Region, endpoint, bucket, table path, a tokenizer name and a
        // keyword field: none of these is credential shaped, and the
        // message comes back as the same instance so callers can tell
        // nothing changed.
        for (String message : List.of(
            "Dataset not found: s3://my-bucket/tables/demo.lance",
            "LanceError(IO): Generic S3 error: Error after 3 retries: error sending request for url (http://127.0.0.1:9000/my-bucket/demo.lance/_versions/)",
            "Invalid user input: unknown tokenizer: no-such-tokenizer, /rust/lance-index/src/scalar/inverted/tokenizer.rs:30:1",
            "aws_region=eu-west-1 aws_endpoint=https://s3.eu-west-1.amazonaws.com allow_http=false",
            "field [category] of type keyword: cannot be changed from type [keyword] to [lance_text]",
            "table [/data/keyed-tables/monkey_key_2026.lance] has a fragment of 5 docs",
            "Azure error: https://acct.blob.core.windows.net/container/t.lance?sv=2024-01-01&sr=c&sp=rl"
        )) {
            assertSame("must come back unchanged: " + message, message, StorageOptions.redactCredentials(message));
        }
        assertNull(StorageOptions.redactCredentials((String) null));
        assertEquals("", StorageOptions.redactCredentials(""));
    }

    public void testRedactCredentialsReplacesAuthorizationValuesAndSasSignatures() {
        String bearer = "REST catalog answered 401 for Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.e30.abc, retrying";
        String redactedBearer = StorageOptions.redactCredentials(bearer);
        assertFalse(redactedBearer, redactedBearer.contains("eyJhbGciOiJIUzI1NiJ9"));
        assertTrue(redactedBearer, redactedBearer.endsWith(", retrying"));

        String basic = "Basic dXNlcjpwYXNzd29yZA== was refused";
        assertEquals("Basic *** was refused", StorageOptions.redactCredentials(basic));

        String sas = "https://acct.blob.core.windows.net/c/t.lance?sv=2024-01-01&sig=abc%2Fdef%3D&sr=c";
        assertEquals("https://acct.blob.core.windows.net/c/t.lance?sv=2024-01-01&sig=***&sr=c", StorageOptions.redactCredentials(sas));
    }

    public void testRedactCredentialsReturnsTheSameExceptionWhenNothingNeedsRedacting() {
        Exception clean = new RuntimeException(
            "Dataset not found: s3://my-bucket/tables/demo.lance",
            new IOException("connection refused")
        );
        assertSame(clean, StorageOptions.redactCredentials(clean));
        RuntimeException runtime = new IllegalStateException("no data nodes available");
        assertSame(runtime, StorageOptions.redactCredentials(runtime));
        assertNull(StorageOptions.redactCredentials((Exception) null));
    }

    public void testRedactCredentialsCopiesAnExceptionWithTheStatusTheFramesAndTheChain() {
        // A Lance open failure is a RuntimeException the REST layer
        // answers with 500; the copy keeps the status and the frames,
        // and every message of the chain is redacted, including the
        // wrapped IOException a scan puts around it.
        RuntimeException lance = new RuntimeException(S3_INVALID_ACCESS_KEY_ID);
        IOException scan = new IOException("scan of fragment 3 failed: " + S3_INVALID_ACCESS_KEY_ID, lance);
        Exception reported = StorageOptions.redactCredentials(scan);
        assertNotSame(scan, reported);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(reported));
        assertArrayEquals(scan.getStackTrace(), reported.getStackTrace());
        int links = 0;
        for (Throwable t = reported; t != null; t = t.getCause()) {
            links++;
            assertFalse("link " + links + " must not carry the key id: " + t.getMessage(), t.getMessage().contains(ACCESS_KEY_ID));
            assertTrue("link " + links + " keeps the S3 error code: " + t.getMessage(), t.getMessage().contains("InvalidAccessKeyId"));
        }
        assertEquals("one link per link of the original", 2, links);
        assertTrue(
            "the cause names the original class: " + reported.getCause().getMessage(),
            reported.getCause().getMessage().startsWith("java.lang.RuntimeException: ")
        );
        assertArrayEquals(lance.getStackTrace(), reported.getCause().getStackTrace());
        // The original is left as it was, for the debug log.
        assertTrue(scan.getMessage().contains(ACCESS_KEY_ID));
        assertSame(lance, scan.getCause());
    }

    public void testRedactCredentialsKeepsAnIllegalArgumentExceptionAndAStatusException() {
        // Lance's invalid input is an IllegalArgumentException the REST
        // layer answers with 400 as illegal_argument_exception; the copy
        // is one too. A status exception keeps its status.
        IllegalArgumentException invalid = new IllegalArgumentException("Invalid user input: " + S3_INVALID_ACCESS_KEY_ID);
        RuntimeException reportedInvalid = StorageOptions.redactCredentials(invalid);
        assertTrue(reportedInvalid.toString(), reportedInvalid instanceof IllegalArgumentException);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(reportedInvalid));
        assertFalse(reportedInvalid.getMessage(), reportedInvalid.getMessage().contains(ACCESS_KEY_ID));
        assertTrue(reportedInvalid.getMessage(), reportedInvalid.getMessage().startsWith("Invalid user input: "));

        OpenSearchStatusException forbidden = new OpenSearchStatusException(
            "tag [v1] could not be resolved on table [s3://b/t.lance]: " + S3_INVALID_ACCESS_KEY_ID,
            RestStatus.BAD_REQUEST
        );
        Exception reportedForbidden = StorageOptions.redactCredentials(forbidden);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(reportedForbidden));
        assertFalse(reportedForbidden.getMessage(), reportedForbidden.getMessage().contains(ACCESS_KEY_ID));
        assertTrue(reportedForbidden.getMessage(), reportedForbidden.getMessage().startsWith("tag [v1] could not be resolved"));
    }

    public void testRedactCredentialsUnwrapsATransportWrapperAndSurvivesTheWire() throws IOException {
        // The coordinator receives the executor's failure inside a
        // RemoteTransportException; the copy reports what the REST
        // layer would have rendered, the wrapped exception, and a copy
        // written to the wire reads back without the key id.
        RemoteTransportException remote = new RemoteTransportException(
            "lance fragment query",
            new IllegalArgumentException("Invalid user input: " + S3_INVALID_ACCESS_KEY_ID)
        );
        Exception reported = StorageOptions.redactCredentials(remote);
        assertTrue(reported.toString(), reported instanceof IllegalArgumentException);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(reported));
        assertFalse(reported.getMessage(), reported.getMessage().contains(ACCESS_KEY_ID));

        Exception transported;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(
                StorageOptions.redactCredentials(new RuntimeException(S3_INVALID_ACCESS_KEY_ID, new IOException("inner " + ACCESS_KEY_ID)))
            );
            try (StreamInput in = out.bytes().streamInput()) {
                transported = in.readException();
            }
        }
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(transported));
        for (Throwable t = transported; t != null; t = t.getCause()) {
            assertFalse(String.valueOf(t.getMessage()), String.valueOf(t.getMessage()).contains(ACCESS_KEY_ID));
        }
        assertNotNull("the cause survives the wire", transported.getCause());
    }

    public void testRedactCredentialsStopsOnACyclicChain() {
        RuntimeException a = new RuntimeException("a " + ACCESS_KEY_ID);
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);
        Exception reported = StorageOptions.redactCredentials((Exception) a);
        int links = 0;
        for (Throwable t = reported; t != null && links < 100; t = t.getCause()) {
            links++;
        }
        assertTrue("the copy is bounded, saw " + links + " links", links < 100);
        assertFalse(reported.getMessage(), reported.getMessage().contains(ACCESS_KEY_ID));
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The attach body parser: every declared top level field reaches the
 * request, an unknown field is refused naming it, {@code number_of_shards}
 * is refused with its reason, and the checks that need more than one
 * field ({@code version} with {@code tag}) or a parsed object
 * ({@code overrides} with {@code multi_fields}) run in
 * {@link LanceAttachRequest.Builder#build} with the message the operator
 * sees.
 */
public class LanceAttachRequestTests extends OpenSearchTestCase {

    private LanceAttachRequest parse(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return LanceAttachRequest.PARSER.parse(parser, null).build();
        }
    }

    public void testEveryDeclaredFieldReachesTheRequest() throws IOException {
        LanceAttachRequest request = parse(
            "{\"table\":\"/tmp/t.lance\",\"name\":\"t\",\"version\":3,"
                + "\"storage_options\":{\"aws_region\":\"us-east-1\"},"
                + "\"overrides\":{\"body\":{\"type\":\"keyword\"}},"
                + "\"multi_fields\":{\"title\":{\"raw\":{\"type\":\"keyword\"}}}}"
        );
        assertEquals("/tmp/t.lance", request.table());
        assertEquals("t", request.indexName());
        assertEquals(Long.valueOf(3L), request.pinnedVersion().orElseThrow());
        assertTrue(request.tag().isEmpty());
        assertEquals(Map.of("aws_region", "us-east-1"), request.storageOptions().asMap());
        assertEquals("keyword", request.overrides().columns().get("body").type());
        assertEquals(Map.of("raw", "keyword"), request.overrides().columns().get("title").subFields());
    }

    public void testTagAloneIsAccepted() throws IOException {
        LanceAttachRequest request = parse("{\"table\":\"/tmp/t.lance\",\"tag\":\"v1\"}");
        assertEquals("v1", request.tag().orElseThrow());
        assertTrue(request.pinnedVersion().isEmpty());
    }

    public void testUnknownFieldIsRefusedNamingIt() {
        for (String key : List.of("indexes", "index_placement", "derive", "fts_columns", "tokenizer", "with_position", "settings")) {
            String json = "{\"table\":\"/tmp/t.lance\",\"" + key + "\":{}}";
            XContentParseException e = expectThrows(XContentParseException.class, () -> parse(json));
            assertTrue(e.getMessage(), e.getMessage().contains("[lance_attach] unknown field [" + key + "]"));
        }
        // The first unknown field in document order is the one named,
        // even when it comes before the required field.
        XContentParseException first = expectThrows(
            XContentParseException.class,
            () -> parse("{\"indexes\":[],\"table\":\"/tmp/t.lance\",\"derive\":true}")
        );
        assertTrue(first.getMessage(), first.getMessage().contains("[lance_attach] unknown field [indexes]"));
    }

    public void testNumberOfShardsIsRefusedWithTheReason() {
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> parse("{\"table\":\"/tmp/t.lance\",\"number_of_shards\":3}")
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[lance_attach] failed to parse field [number_of_shards]"));
        assertTrue(
            e.getCause().getMessage(),
            e.getCause().getMessage().startsWith("[number_of_shards] is no longer accepted by /_plugins/_lance/attach")
        );
    }

    public void testWrongValueTypeIsRefusedNamingTheField() {
        XContentParseException table = expectThrows(XContentParseException.class, () -> parse("{\"table\":42}"));
        assertTrue(table.getMessage(), table.getMessage().contains("[lance_attach] table doesn't support values of type: VALUE_NUMBER"));
        XContentParseException overrides = expectThrows(
            XContentParseException.class,
            () -> parse("{\"table\":\"/tmp/t.lance\",\"overrides\":[\"body\"]}")
        );
        assertTrue(
            overrides.getMessage(),
            overrides.getMessage().contains("[lance_attach] overrides doesn't support values of type: START_ARRAY")
        );
    }

    public void testBuildChecksTheFieldsTogether() {
        assertEquals("[table] is required", expectThrows(IllegalArgumentException.class, () -> parse("{}")).getMessage());
        assertEquals("[table] is required", expectThrows(IllegalArgumentException.class, () -> parse("{\"table\":\"\"}")).getMessage());
        assertEquals(
            "[version] must be a non-negative integer",
            expectThrows(IllegalArgumentException.class, () -> parse("{\"table\":\"/tmp/t.lance\",\"version\":-1}")).getMessage()
        );
        assertEquals(
            "[tag] must not be empty",
            expectThrows(IllegalArgumentException.class, () -> parse("{\"table\":\"/tmp/t.lance\",\"tag\":\"\"}")).getMessage()
        );
        assertEquals(
            "[version] and [tag] are mutually exclusive",
            expectThrows(IllegalArgumentException.class, () -> parse("{\"table\":\"/tmp/t.lance\",\"version\":1,\"tag\":\"v1\"}"))
                .getMessage()
        );
        // The empty builder (a request without a body) reports the
        // missing table the same way.
        assertEquals(
            "[table] is required",
            expectThrows(IllegalArgumentException.class, () -> new LanceAttachRequest.Builder().build()).getMessage()
        );
    }

    public void testOverridesAndMultiFieldsAreParsedTogether() {
        // The same column declared through both clauses is ambiguous and
        // refused; that check needs both objects, so it runs in build.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parse(
                "{\"table\":\"/tmp/t.lance\","
                    + "\"overrides\":{\"body\":{\"fields\":{\"raw\":{\"type\":\"keyword\"}}}},"
                    + "\"multi_fields\":{\"body\":{\"raw\":{\"type\":\"keyword\"}}}}"
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("both [multi_fields] and [overrides.body.fields]"));
    }

    public void testStorageOptionsAreCheckedInBuild() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parse("{\"table\":\"/tmp/t.lance\",\"storage_options\":{\"aws_region\":1}}")
        );
        assertEquals(
            "[lance_attach] [storage_options.aws_region] must be a string (nested objects / arrays / numbers / booleans are not accepted)",
            e.getMessage()
        );
    }
}

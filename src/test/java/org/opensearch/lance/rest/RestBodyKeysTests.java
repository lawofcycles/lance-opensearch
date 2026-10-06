/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

/**
 * The namespace bodies reject a top level key outside their
 * accepted list before anything else is read: the first unknown key is an
 * {@link IllegalArgumentException} naming it and the accepted keys, and a
 * body made only of accepted keys never trips the check. The accepted
 * lists are pinned here so adding a key to a parser without adding it to
 * the list fails a unit test instead of a request.
 */
public class RestBodyKeysTests extends OpenSearchTestCase {

    public void testRejectUnknownNamesTheFirstUnknownKeyAndTheAcceptedList() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("a", 1);
        body.put("zzz", 2);
        body.put("yyy", 3);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestBodyKeys.rejectUnknown("lance_x", body, List.of("a", "b"))
        );
        assertEquals("[lance_x] unknown key [zzz]; accepted keys are a, b", e.getMessage());

        RestBodyKeys.rejectUnknown("lance_x", Map.of("a", 1, "b", 2), List.of("a", "b"));
        RestBodyKeys.rejectUnknown("lance_x", Map.of(), List.of("a", "b"));
    }

    public void testNamespaceAcceptedKeysArePinned() {
        assertEquals(List.of("path", "type", "name", "config", "storage_options", "overrides"), RestNamespaceAction.REGISTER_KEYS);
        assertEquals(List.of("path", "name"), RestNamespaceAction.IDENTIFIER_KEYS);
    }

    public void testNamespaceRegisterRejectsAnUnknownTopLevelKey() {
        RestNamespaceAction action = new RestNamespaceAction();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(post("/_plugins/_lance/namespace", "{\"path\":\"/tmp\",\"poll_cadence\":\"1s\"}"), null)
        );
        assertEquals(
            "[lance_namespace] unknown key [poll_cadence]; accepted keys are path, type, name, config, storage_options, overrides",
            e.getMessage()
        );
    }

    public void testNamespaceRegisterReadsEveryAcceptedKey() {
        RestNamespaceAction action = new RestNamespaceAction();
        Map<String, String> validValue = Map.of(
            "path",
            "\"/tmp\"",
            "type",
            "\"directory\"",
            "name",
            "\"n\"",
            "config",
            "{\"k\":\"v\"}",
            "storage_options",
            "{\"aws_region\":\"us-east-1\"}",
            "overrides",
            "{\"body\":{\"type\":\"keyword\"}}"
        );
        assertEquals(RestNamespaceAction.REGISTER_KEYS.size(), validValue.size());
        for (String key : RestNamespaceAction.REGISTER_KEYS) {
            String json = "path".equals(key)
                ? "{\"path\":" + validValue.get(key) + "}"
                : "{\"path\":\"/tmp\",\"" + key + "\":" + validValue.get(key) + "}";
            assertNotNull(key, action.prepareRequest(post("/_plugins/_lance/namespace", json), null));
        }
    }

    public void testNamespaceIdentifierBodiesRejectRegisterOnlyKeys() {
        // /tables and DELETE take only an identifier; the register body's
        // other keys are unknown there and say so.
        RestNamespaceAction action = new RestNamespaceAction();
        String json = "{\"name\":\"n\",\"type\":\"directory\"}";
        IllegalArgumentException tables = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(post("/_plugins/_lance/namespace/tables", json), null)
        );
        assertEquals("[lance_namespace] unknown key [type]; accepted keys are path, name", tables.getMessage());
        IllegalArgumentException delete = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request(RestRequest.Method.DELETE, "/_plugins/_lance/namespace", json), null)
        );
        assertEquals("[lance_namespace] unknown key [type]; accepted keys are path, name", delete.getMessage());

        for (String key : RestNamespaceAction.IDENTIFIER_KEYS) {
            String accepted = "{\"" + key + "\":\"n\"}";
            assertNotNull(key, action.prepareRequest(post("/_plugins/_lance/namespace/tables", accepted), null));
            assertNotNull(key, action.prepareRequest(request(RestRequest.Method.DELETE, "/_plugins/_lance/namespace", accepted), null));
        }
    }

    private static RestRequest post(String path, String json) {
        return request(RestRequest.Method.POST, path, json);
    }

    private static RestRequest request(RestRequest.Method method, String path, String json) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(method)
            .withPath(path)
            .withContent(new BytesArray(json), XContentType.JSON)
            .build();
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.MappingDerivation;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * POST /_plugins/_lance/attach {"table": "/path/to/table.lance"}
 *
 * Attaches a Lance table as a read only index: derives everything from the
 * table and creates a real engine backed index. The mapping follows the
 * derivation defaults of {@link MappingDerivation} (a string column
 * carrying an FTS index maps to text, integers map to numeric doc values
 * fields), the shard count is always one, and the primary key is detected
 * from Lance field metadata. Optional overrides: "name" (index name,
 * defaults to the table directory name), "version" (pin a manifest
 * version) or "tag" (follow a Lance tag; not together with "version").
 *
 * <p>Attach always creates a single-shard index because the fragment path
 * (see {@code LanceDispatchActionFilter}) is the only search implementation
 * left, and it fans out to fragments regardless of shard count. Requests
 * carrying {@code number_of_shards} are rejected with 400 naming the
 * reason, before the key check; any other top level key outside
 * {@link #ACCEPTED_KEYS} is a 400 naming the key and the list.
 *
 * <p>The handler parses the body and hands a {@link LanceAttachRequest} to
 * {@link LanceAttachAction}; opening the table, deriving the mapping, and
 * creating the index happen in the transport action so a security plugin
 * evaluates the caller before any of that starts.
 */
public class RestAttachAction extends BaseRestHandler {

    /**
     * The top level keys {@link #prepareRequest} reads, in the order it
     * reads them. Any other key is a 400 naming it and this list.
     */
    static final List<String> ACCEPTED_KEYS = List.of("table", "name", "version", "tag", "storage_options", "overrides", "multi_fields");

    @Override
    public String getName() {
        return "lance_attach";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_plugins/_lance/attach"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        Map<String, Object> body = request.hasContent()
            ? XContentHelper.convertToMap(request.content(), true, request.getMediaType()).v2()
            : Map.of();
        if (body.containsKey("number_of_shards")) {
            // Lance-backed indices are single-shard and fan-out happens
            // per fragment, so a shard count has nothing to control.
            // Answered before the key check so the operator reads why,
            // not just that the key is unknown.
            return channel -> channel.sendResponse(
                new BytesRestResponse(
                    RestStatus.BAD_REQUEST,
                    "[number_of_shards] is no longer accepted by /_plugins/_lance/attach; the fragment path fans out at the fragment "
                        + "level regardless of shard count, and Lance-backed indices are always single-shard"
                )
            );
        }
        // Thrown, not caught: the REST controller turns it into a 400
        // illegal_argument_exception, the shape core uses for an unknown
        // field in a request body. The body is read in document order so
        // the key named is the first unknown one the caller wrote.
        RestBodyKeys.rejectUnknown(getName(), body, ACCEPTED_KEYS);

        String table;
        String explicitName;
        Long pinnedVersion;
        String tag;
        StorageOptions storageOptions;
        LanceOverrides overrides;
        try {
            table = readOptionalString(body, "table");
            if (table == null || table.isEmpty()) {
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "[table] is required"));
            }
            explicitName = readOptionalString(body, "name");
            pinnedVersion = readOptionalLong(body, "version");
            if (pinnedVersion != null && pinnedVersion < 0) {
                return channel -> channel.sendResponse(
                    new BytesRestResponse(RestStatus.BAD_REQUEST, "[version] must be a non-negative integer")
                );
            }
            tag = readOptionalString(body, "tag");
            if (tag != null && tag.isEmpty()) {
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "[tag] must not be empty"));
            }
            if (pinnedVersion != null && tag != null) {
                // `version` is a fixed pin, `tag` follows wherever the tag
                // points. The engine can honour only one of them per index.
                return channel -> channel.sendResponse(
                    new BytesRestResponse(RestStatus.BAD_REQUEST, "[version] and [tag] are mutually exclusive")
                );
            }
            storageOptions = StorageOptions.parseFromRequestField(body.get("storage_options"), "[lance_attach]");
            // `overrides` is the forward-looking clause; the legacy
            // `multi_fields` clause folds into `overrides.[col].fields`
            // at parse time so everything downstream sees one shape.
            overrides = LanceOverrides.parseAttachClauses(body.get("overrides"), body.get("multi_fields"));
        } catch (IllegalArgumentException e) {
            String message = e.getMessage();
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, message));
        }

        LanceAttachRequest attach = new LanceAttachRequest(table, explicitName, pinnedVersion, tag, storageOptions, overrides);
        return channel -> client.execute(LanceAttachAction.INSTANCE, attach, new RestToXContentListener<>(channel));
    }

    private static String readOptionalString(Map<String, Object> body, String key) {
        Object v = body.get(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof String)) {
            throw new IllegalArgumentException("[" + key + "] must be a string, got " + v.getClass().getSimpleName());
        }
        return (String) v;
    }

    private static Long readOptionalLong(Map<String, Object> body, String key) {
        Object v = body.get(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof Number)) {
            throw new IllegalArgumentException("[" + key + "] must be a number, got " + v.getClass().getSimpleName());
        }
        return ((Number) v).longValue();
    }
}

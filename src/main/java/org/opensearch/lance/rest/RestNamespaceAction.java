/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.namespace.LanceNamespaceListAction;
import org.opensearch.lance.namespace.LanceNamespaceListRequest;
import org.opensearch.lance.namespace.LanceNamespaceMetadata;
import org.opensearch.lance.namespace.LanceNamespaceUpdateAction;
import org.opensearch.lance.namespace.LanceNamespaceUpdateRequest;
import org.opensearch.lance.namespace.LanceNamespaceUpdateResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST surface for namespace registration and listing.
 *
 * <ul>
 *   <li>{@code POST /_lance/namespace} registers a catalog through
 *       {@link LanceNamespaceUpdateAction}. The body takes {@code type}
 *       ({@code directory} when absent; any value of
 *       {@link LanceNamespaceMetadata.Entry#ACCEPTED_TYPES}), a
 *       registration {@code name} (required for every type except
 *       {@code directory}, where it defaults to the path),
 *       {@code path} (directory only), and a {@code config} object of
 *       string values passed to the implementation's initialize.</li>
 *   <li>{@code DELETE /_lance/namespace} unregisters by {@code name}
 *       (or {@code path} for directory registrations) through the same
 *       action.</li>
 *   <li>{@code GET /_lance/namespace} lists the registrations and
 *       {@code POST /_lance/namespace/tables} lists the tables of one
 *       registration, both through {@link LanceNamespaceListAction}.</li>
 * </ul>
 *
 * <p>The handler only parses the body and hands the request to the
 * transport action. Allowlist and path existence checks live in the
 * transport action so they run after a security plugin has evaluated
 * the caller's privileges, and nothing about the path (whether it is
 * registered, whether it exists) is revealed to a caller who lacks them.
 */
public class RestNamespaceAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_namespace";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, "/_lance/namespace"),
            new Route(RestRequest.Method.GET, "/_lance/namespace"),
            new Route(RestRequest.Method.DELETE, "/_lance/namespace"),
            new Route(RestRequest.Method.POST, "/_lance/namespace/tables")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (request.method() == RestRequest.Method.GET) {
            return channel -> client.execute(
                LanceNamespaceListAction.INSTANCE,
                LanceNamespaceListRequest.namespaces(),
                new RestStatusToXContentListener<>(channel)
            );
        }
        Map<String, Object> body = request.hasContent()
            ? XContentHelper.convertToMap(request.content(), false, request.getMediaType()).v2()
            : Map.of();
        String path;
        try {
            path = optionalString(body, "path");
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }
        // POST /_lance/namespace/tables is a read-only listing endpoint.
        // POST is used (rather than GET with a query parameter) because
        // registered paths can contain slashes, scheme prefixes
        // (s3://bucket/root), and other characters that make URL-encoded
        // path segments fragile. Body-with-identifier matches the shape
        // of the register / unregister calls right below.
        if (request.path().endsWith("/tables")) {
            String identifier;
            try {
                identifier = requireIdentifier(body, path);
            } catch (IllegalArgumentException e) {
                return badRequest(e.getMessage());
            }
            return channel -> client.execute(
                LanceNamespaceListAction.INSTANCE,
                LanceNamespaceListRequest.tables(identifier),
                new RestStatusToXContentListener<>(channel)
            );
        }
        if (request.method() == RestRequest.Method.DELETE) {
            // DELETE only stops the polling of that registration.
            // Already-surfaced indexes stay; the operator can delete them
            // via DELETE /{index} if they want the tables to disappear.
            // This matches the "the namespace registration is separate
            // from the OpenSearch index lifecycle" contract in the RFC.
            String identifier;
            try {
                identifier = requireIdentifier(body, path);
            } catch (IllegalArgumentException e) {
                return badRequest(e.getMessage());
            }
            return channel -> client.execute(
                LanceNamespaceUpdateAction.INSTANCE,
                LanceNamespaceUpdateRequest.unregister(identifier),
                new RestBuilderListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(LanceNamespaceUpdateResponse response, XContentBuilder b) throws Exception {
                        boolean removed = response.changed();
                        b.startObject().field("unregistered", removed).field("name", identifier).endObject();
                        return new BytesRestResponse(removed ? RestStatus.OK : RestStatus.NOT_FOUND, b);
                    }
                }
            );
        }
        String type;
        String name;
        Map<String, String> config;
        StorageOptions storageOptions;
        String overridesJson;
        try {
            type = optionalString(body, "type");
            if (type == null) {
                type = LanceNamespaceMetadata.Entry.TYPE_DIRECTORY;
            } else if (!LanceNamespaceMetadata.Entry.ACCEPTED_TYPES.contains(type)) {
                return badRequest(
                    "unknown namespace type [" + type + "]; accepted values are " + LanceNamespaceMetadata.Entry.ACCEPTED_TYPES
                );
            }
            name = optionalString(body, "name");
            config = parseConfig(body.get("config"));
            storageOptions = StorageOptions.parseFromRequestField(body.get("storage_options"), "[lance_namespace]");
            overridesJson = LanceOverrides.parseAttachClauses(body.get("overrides"), null, body.get("indexes")).toJson();
            if (LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(type)) {
                if (path == null) {
                    return badRequest("[path] is required");
                }
                if (name == null) {
                    name = path;
                }
            } else {
                if (path != null) {
                    return badRequest("[path] is only accepted for type [directory]; a [" + type + "] namespace is rooted by its config");
                }
                if (name == null) {
                    return badRequest("[name] is required for type [" + type + "]");
                }
                if (LanceNamespaceMetadata.Entry.TYPE_REST.equals(type) && isBlank(config.get("uri"))) {
                    return badRequest("[config.uri] is required for type [rest]");
                }
                boolean icebergProtocol = LanceNamespaceMetadata.Entry.TYPE_ICEBERG.equals(type)
                    || LanceNamespaceMetadata.Entry.TYPE_POLARIS.equals(type);
                if (icebergProtocol || LanceNamespaceMetadata.Entry.TYPE_UNITY.equals(type)) {
                    if (isBlank(config.get("endpoint"))) {
                        return badRequest("[config.endpoint] is required for type [" + type + "]");
                    }
                }
                if (icebergProtocol && isBlank(config.get("warehouse"))) {
                    // Without a warehouse the client rejects every listing
                    // (the warehouse / catalog is the first level of each
                    // table id), so the registration could never surface
                    // a table. Refuse it up front with the reason.
                    return badRequest(
                        "[config.warehouse] is required for type ["
                            + type
                            + "]; it names the warehouse (catalog) whose namespaces are polled"
                    );
                }
                if (LanceNamespaceMetadata.Entry.TYPE_UNITY.equals(type) && isBlank(config.get("catalog"))) {
                    return badRequest("[config.catalog] is required for type [unity]");
                }
            }
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }
        final String registeredName = name;
        final String registeredType = type;
        return channel -> client.execute(
            LanceNamespaceUpdateAction.INSTANCE,
            LanceNamespaceUpdateRequest.register(registeredName, registeredType, path, storageOptions, config, overridesJson),
            new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(LanceNamespaceUpdateResponse response, XContentBuilder b) throws Exception {
                    b.startObject()
                        .field("registered", registeredName)
                        .field("type", registeredType)
                        .field("note", "tables surface as indexes within the poll cadence")
                        .endObject();
                    return new BytesRestResponse(RestStatus.OK, b);
                }
            }
        );
    }

    private static RestChannelConsumer badRequest(String message) {
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, message));
    }

    /** Non-empty string field, or {@code null} when absent. Throws on wrong type or empty value. */
    private static String optionalString(Map<String, Object> body, String field) {
        Object raw = body.get(field);
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof String value)) {
            throw new IllegalArgumentException("[" + field + "] must be a string, got " + raw.getClass().getSimpleName());
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("[" + field + "] must not be empty");
        }
        return value;
    }

    /** The registration identifier for DELETE and /tables: {@code name} when given, {@code path} otherwise. */
    private static String requireIdentifier(Map<String, Object> body, String path) {
        String name = optionalString(body, "name");
        if (name != null) {
            return name;
        }
        if (path != null) {
            return path;
        }
        throw new IllegalArgumentException("[name] (or [path] for a directory registration) is required");
    }

    /**
     * Parse the {@code config} object into a string-to-string map.
     * Keys the plugin does not know pass through to the catalog
     * implementation untouched; only the shape is validated here.
     */
    private static Map<String, String> parseConfig(Object raw) {
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Map<?, ?> rawMap)) {
            throw new IllegalArgumentException("[config] must be a JSON object of string values");
        }
        Map<String, String> parsed = new LinkedHashMap<>(rawMap.size());
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            if (!(entry.getKey() instanceof String key) || key.isEmpty()) {
                throw new IllegalArgumentException("[config] keys must be non-empty strings");
            }
            if (!(entry.getValue() instanceof String value)) {
                throw new IllegalArgumentException(
                    "[config." + key + "] must be a string (nested objects / arrays / numbers / booleans are not accepted)"
                );
            }
            parsed.put(key, value);
        }
        return parsed;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isEmpty();
    }
}

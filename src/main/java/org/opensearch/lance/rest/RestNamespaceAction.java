/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.lance.namespace.LanceNamespaceListAction;
import org.opensearch.lance.namespace.LanceNamespaceListRequest;
import org.opensearch.lance.namespace.LanceNamespaceMetadata;
import org.opensearch.lance.namespace.LanceNamespacePollAction;
import org.opensearch.lance.namespace.LanceNamespacePollRequest;
import org.opensearch.lance.namespace.LanceNamespaceUpdateAction;
import org.opensearch.lance.namespace.LanceNamespaceUpdateRequest;
import org.opensearch.lance.namespace.LanceNamespaceUpdateRequest.Identifier;
import org.opensearch.lance.namespace.LanceNamespaceUpdateRequest.Register;
import org.opensearch.lance.namespace.LanceNamespaceUpdateResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST surface for namespace registration and listing.
 *
 * <ul>
 *   <li>{@code POST /_plugins/_lance/namespace} registers a catalog through
 *       {@link LanceNamespaceUpdateAction}. The body takes {@code type}
 *       ({@code directory} when absent; any value of
 *       {@link LanceNamespaceMetadata.Entry#ACCEPTED_TYPES}), a
 *       registration {@code name} (required for every type except
 *       {@code directory}, where it defaults to the path),
 *       {@code path} (directory only), and a {@code config} object of
 *       string values passed to the implementation's initialize.</li>
 *   <li>{@code DELETE /_plugins/_lance/namespace} unregisters by {@code name}
 *       (or {@code path} for directory registrations) through the same
 *       action.</li>
 *   <li>{@code GET /_plugins/_lance/namespace} lists the registrations and
 *       {@code POST /_plugins/_lance/namespace/tables} lists the tables of one
 *       registration, both through {@link LanceNamespaceListAction}.</li>
 *   <li>{@code POST /_plugins/_lance/namespace/_poll} runs one catalog listing
 *       cycle now on the cluster manager through
 *       {@link LanceNamespacePollAction}; {@code ?name=} (a registration
 *       name, or a directory registration's path) limits it to one
 *       registration.</li>
 * </ul>
 *
 * <p>The handler only parses the body and hands the request to the
 * transport action. The register body is read by
 * {@link LanceNamespaceUpdateRequest#REGISTER_PARSER} and the identifier
 * bodies of DELETE and {@code /tables} by
 * {@link LanceNamespaceUpdateRequest#IDENTIFIER_PARSER}; a field outside
 * the declared set answers 400 naming it. Allowlist and path existence
 * checks live in the transport action so they run after a security
 * plugin has evaluated the caller's privileges, and nothing about the
 * path (whether it is registered, whether it exists) is revealed to a
 * caller who lacks them.
 */
public class RestNamespaceAction extends BaseRestHandler {

    @Override
    public String getName() {
        return LanceNamespaceUpdateRequest.PARSER_NAME;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, "/_plugins/_lance/namespace"),
            new Route(RestRequest.Method.GET, "/_plugins/_lance/namespace"),
            new Route(RestRequest.Method.DELETE, "/_plugins/_lance/namespace"),
            new Route(RestRequest.Method.POST, "/_plugins/_lance/namespace/tables"),
            new Route(RestRequest.Method.POST, "/_plugins/_lance/namespace/_poll")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method() == RestRequest.Method.GET) {
            return channel -> client.execute(
                LanceNamespaceListAction.INSTANCE,
                LanceNamespaceListRequest.namespaces(),
                new RestStatusToXContentListener<>(channel)
            );
        }
        if (request.path().endsWith("/_poll")) {
            // The trigger takes its optional registration from the query
            // string: it has no body of its own, and the answer is the
            // cycle's report.
            String name = request.param("name");
            return channel -> client.execute(
                LanceNamespacePollAction.INSTANCE,
                new LanceNamespacePollRequest(name == null || name.isEmpty() ? null : name),
                new RestToXContentListener<>(channel)
            );
        }
        // Parse failures are thrown, not caught: the REST controller turns
        // an IllegalArgumentException (the parser's XContentParseException
        // is one) into a 400 whose reason is the message.
        if (request.path().endsWith("/tables")) {
            // POST /_plugins/_lance/namespace/tables is a read-only listing endpoint.
            // POST is used (rather than GET with a query parameter) because
            // registered paths can contain slashes, scheme prefixes
            // (s3://bucket/root), and other characters that make URL-encoded
            // path segments fragile. Body-with-identifier matches the shape
            // of the register / unregister calls right below.
            String identifier = parse(request, LanceNamespaceUpdateRequest.IDENTIFIER_PARSER, Identifier::new).resolve();
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
            // The namespace registration and the lifecycle of the
            // OpenSearch indexes it surfaced are separate.
            String identifier = parse(request, LanceNamespaceUpdateRequest.IDENTIFIER_PARSER, Identifier::new).resolve();
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
        LanceNamespaceUpdateRequest register = parse(request, LanceNamespaceUpdateRequest.REGISTER_PARSER, Register::new).build();
        return channel -> client.execute(LanceNamespaceUpdateAction.INSTANCE, register, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(LanceNamespaceUpdateResponse response, XContentBuilder b) throws Exception {
                b.startObject()
                    .field("registered", register.name())
                    .field("type", register.type())
                    .field("note", "tables surface as indexes within the poll cadence")
                    .endObject();
                return new BytesRestResponse(RestStatus.OK, b);
            }
        });
    }

    /** The request body read by {@code parser}; a request without a body reads as the empty value {@code empty} supplies. */
    private static <T> T parse(RestRequest request, ObjectParser<T, Void> parser, Supplier<T> empty) throws IOException {
        if (!request.hasContent()) {
            return empty.get();
        }
        try (XContentParser content = request.contentParser()) {
            return parser.parse(content, null);
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.io.IOException;
import java.util.List;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.MappingDerivation;
import org.opensearch.rest.BaseRestHandler;
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
 * <p>The body is read by {@link LanceAttachRequest#PARSER}, which names
 * the accepted top level fields; an unknown field, a field of the wrong
 * value type and {@code number_of_shards} (always one shard, the fragment
 * fan out does not depend on it) are refused with 400 by the exception
 * the parser throws. The handler hands the {@link LanceAttachRequest} to
 * {@link LanceAttachAction}; opening the table, deriving the mapping, and
 * creating the index happen in the transport action so a security plugin
 * evaluates the caller before any of that starts.
 */
public class RestAttachAction extends BaseRestHandler {

    @Override
    public String getName() {
        return LanceAttachRequest.PARSER_NAME;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_plugins/_lance/attach"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Thrown, not caught: the REST controller turns an
        // IllegalArgumentException (the parser's XContentParseException
        // is one) into a 400 whose reason is the message.
        LanceAttachRequest.Builder body;
        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
                body = LanceAttachRequest.PARSER.parse(parser, null);
            }
        } else {
            body = new LanceAttachRequest.Builder();
        }
        LanceAttachRequest attach = body.build();
        return channel -> client.execute(LanceAttachAction.INSTANCE, attach, new RestToXContentListener<>(channel));
    }
}

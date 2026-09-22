/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.index.LanceBuildIndexesAction;
import org.opensearch.lance.index.LanceBuildIndexesRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * POST /_lance/build_indexes/{index} [{"columns": [...], "fts_columns": [...], "fragment_ids": [...], "optimize": bool, "retrain": bool, "tokenizer": "...", "with_position": bool, "indexes": {...}}]
 *
 * Manual index build endpoint. The handler parses the body and hands a
 * {@link LanceBuildIndexesRequest} to {@link LanceBuildIndexesAction};
 * resolving the table, running the Lance builders, and refreshing the
 * index happen in the transport action so a security plugin evaluates
 * the caller's index-level privilege before the plugin opens the table.
 * {@code fts_columns} names Utf8 columns that receive a new FTS index;
 * {@code tokenizer} is forwarded to Lance as that index's
 * {@code base_tokenizer} without an allowlist, so validation of the name
 * is Lance's; {@code with_position} (default false) makes Lance store
 * token positions in that index, which {@code lance_match_phrase} needs.
 * {@code indexes} takes the same per-column index type preferences as
 * the attach body's {@code indexes} clause and applies them to this
 * build only, without touching the persisted preference. The response status (200 / 400 / 500) is the one the
 * transport action put on the response; the body always lists
 * {@code built}, {@code skipped} and {@code failed} per index kind.
 */
public class RestBuildIndexesAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_build_indexes";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_lance/build_indexes/{index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String indexName = request.param("index");
        Map<String, Object> body = request.hasContent()
            ? XContentHelper.convertToMap(request.content(), false, request.getMediaType()).v2()
            : Map.of();
        @SuppressWarnings("unchecked")
        List<String> columnsFilterRaw = (List<String>) body.get("columns");
        @SuppressWarnings("unchecked")
        List<Number> fragmentIdsRaw = (List<Number>) body.get("fragment_ids");
        boolean optimize = Boolean.TRUE.equals(body.get("optimize"));
        boolean retrain = Boolean.TRUE.equals(body.get("retrain"));
        Object ftsColumnsRaw = body.get("fts_columns");
        Object tokenizerRaw = body.get("tokenizer");
        Object withPositionRaw = body.get("with_position");
        Object indexesRaw = body.get("indexes");

        if (optimize && fragmentIdsRaw != null) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(
                    RestStatus.BAD_REQUEST,
                    "fragment_ids is not supported with optimize=true (Lance OptimizeOptions covers every out-of-index fragment automatically)"
                )
            );
        }
        if (retrain && !optimize) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(RestStatus.BAD_REQUEST, "retrain is only valid with optimize=true")
            );
        }
        if (ftsColumnsRaw != null && !isStringList(ftsColumnsRaw)) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(RestStatus.BAD_REQUEST, "fts_columns must be an array of Utf8 column names, saw " + ftsColumnsRaw)
            );
        }
        if (tokenizerRaw != null && !(tokenizerRaw instanceof String)) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(
                    RestStatus.BAD_REQUEST,
                    "tokenizer must be a string naming a Lance base_tokenizer, saw " + tokenizerRaw
                )
            );
        }
        if (withPositionRaw != null && !(withPositionRaw instanceof Boolean)) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(RestStatus.BAD_REQUEST, "with_position must be a boolean, saw " + withPositionRaw)
            );
        }
        // The one-shot `indexes` object goes through the same structural
        // validation as the attach clause; the transport action validates
        // the named columns against the table's schema.
        String indexesJson = null;
        if (indexesRaw != null) {
            try {
                indexesJson = LanceOverrides.indexPreferencesToJson(LanceOverrides.parseIndexesClause(indexesRaw));
            } catch (IllegalArgumentException e) {
                String message = e.getMessage();
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, message));
            }
        }

        List<Integer> fragmentIds = null;
        if (fragmentIdsRaw != null) {
            fragmentIds = new ArrayList<>(fragmentIdsRaw.size());
            for (Number n : fragmentIdsRaw) {
                fragmentIds.add(n.intValue());
            }
        }
        @SuppressWarnings("unchecked")
        List<String> ftsColumns = (List<String>) ftsColumnsRaw;
        LanceBuildIndexesRequest build = new LanceBuildIndexesRequest(
            indexName,
            columnsFilterRaw,
            ftsColumns,
            fragmentIds,
            optimize,
            retrain,
            (String) tokenizerRaw,
            Boolean.TRUE.equals(withPositionRaw),
            indexesJson
        );
        return channel -> client.execute(LanceBuildIndexesAction.INSTANCE, build, new RestStatusToXContentListener<>(channel));
    }

    private static boolean isStringList(Object value) {
        if (!(value instanceof List<?> list)) {
            return false;
        }
        for (Object element : list) {
            if (!(element instanceof String)) {
                return false;
            }
        }
        return true;
    }
}

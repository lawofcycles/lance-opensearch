/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.lance.Dataset;
import org.lance.index.OptimizeOptions;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/**
 * POST /_lance/build_indexes/{index} [{"columns": [...], "fragment_ids": [...], "optimize": bool, "retrain": bool}]
 *
 * Manual build endpoint (design-notes.md decision 33, recovery path C).
 *
 * Two modes:
 * <ul>
 *   <li>{@code optimize=false} (default) — Runs the FTS, scalar and vector
 *       builders for the target columns discovered by
 *       {@link RestAttachAction}#derive. Columns already carrying an index
 *       are skipped. Supports {@code fragment_ids} to produce a partial
 *       initial index over the requested fragments only.</li>
 *   <li>{@code optimize=true} — Runs {@link Dataset#optimizeIndices} for the
 *       filtered indexes. Lance incrementally merges fragments not yet
 *       covered. {@code fragment_ids} is rejected because
 *       {@link OptimizeOptions} does not accept per-fragment scoping.
 *       {@code retrain=true} rebuilds the index (vector codebook and
 *       centroids relearn) rather than merging.</li>
 * </ul>
 *
 * Skips the {@code lance.builder.max_rows} check so operators can force a
 * build on tables the automatic path passed over. After the build it refreshes
 * the index so the reader picks up the new Lance version.
 *
 * <p>Threading: cluster state lookup and the Lance JNI builder both run off
 * the transport thread. The state request goes through the async {@code
 * client.admin().cluster().state(request, listener)} path; the JNI call and
 * refresh happen on the {@link ThreadPool.Names#GENERIC} pool because
 * {@link Dataset#open} and {@code optimizeIndices} block on native I/O.
 */
public class RestBuildIndexesAction extends BaseRestHandler {

    private final ThreadPool threadPool;

    public RestBuildIndexesAction(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

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

        return channel -> {
            // Async cluster state lookup: transport threads must not block on
            // .actionGet(). We only need the metadata for one index, so scope
            // the request narrowly.
            ClusterStateRequest stateRequest = new ClusterStateRequest();
            stateRequest.clear().metadata(true).indices(indexName);
            client.admin().cluster().state(stateRequest, new ActionListener<ClusterStateResponse>() {
                @Override
                public void onResponse(ClusterStateResponse response) {
                    IndexMetadata metadata = response.getState().metadata().index(indexName);
                    if (metadata == null) {
                        sendError(channel, RestStatus.NOT_FOUND, "index not found: " + indexName);
                        return;
                    }
                    String tableUri = metadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
                    if (tableUri == null) {
                        sendError(channel, RestStatus.BAD_REQUEST, "index " + indexName + " is not a Lance index");
                        return;
                    }
                    // Dispatch the JNI work to the generic pool. Dataset.open
                    // and optimizeIndices block on native I/O and would trip
                    // the transport-thread assertion otherwise.
                    threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                        runBuildAndRefresh(client, channel, indexName, tableUri, columnsFilterRaw, fragmentIdsRaw, optimize, retrain);
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    sendError(channel, e);
                }
            });
        };
    }

    private static void runBuildAndRefresh(
        NodeClient client,
        RestChannel channel,
        String indexName,
        String tableUri,
        List<String> columnsFilterRaw,
        List<Number> fragmentIdsRaw,
        boolean optimize,
        boolean retrain
    ) {
        List<String> ftsBuilt;
        List<String> scalarBuilt;
        List<String> vectorBuilt;
        try (Dataset dataset = Dataset.open().allocator(LanceRegistry.allocator()).uri(tableUri).build()) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, null);
            Set<String> columnsFilter = columnsFilterRaw != null ? new LinkedHashSet<>(columnsFilterRaw) : null;
            if (columnsFilter != null) {
                // Reject unknown columns up front so callers don't get a 200
                // response with `built: []` and no explanation for a typo
                // (see issue #33). "Known" here means the column is
                // FTS-eligible, scalar-eligible, or vector-eligible per
                // derive; other columns are stored-only and cannot carry an
                // index.
                Set<String> known = new LinkedHashSet<>();
                known.addAll(derivation.ftsColumns());
                known.addAll(derivation.scalarColumns());
                known.addAll(derivation.vectorColumns());
                for (String c : columnsFilter) {
                    if (!known.contains(c)) {
                        sendError(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "column [" + c + "] is not indexable by build_indexes; known columns are " + known
                        );
                        return;
                    }
                }
            }
            Set<String> ftsTarget = filter(derivation.ftsColumns(), columnsFilter);
            Set<String> scalarTarget = filter(derivation.scalarColumns(), columnsFilter);
            Set<String> vectorTarget = filter(derivation.vectorColumns(), columnsFilter);
            if (optimize) {
                // Optimize path: hand the ACTUAL Lance index names (via
                // describeIndices) to OptimizeIndices instead of assuming the
                // `<col>_fts` / `<col>_btree` / `<col>_vec` convention. Lance
                // silently ignores unknown names, so the old convention path
                // returned 200 with `built: [...]` even when nothing was
                // touched (see issue #33).
                ftsBuilt = LanceIndexBuilder.optimizeExistingFtsIndexes(dataset, ftsTarget, retrain);
                scalarBuilt = LanceIndexBuilder.optimizeExistingScalarIndexes(dataset, scalarTarget, retrain);
                vectorBuilt = LanceIndexBuilder.optimizeExistingVectorIndexes(dataset, vectorTarget, retrain);
            } else {
                Optional<List<Integer>> fragmentIds = toIntList(fragmentIdsRaw);
                ftsBuilt = LanceIndexBuilder.ensureFtsIndexes(dataset, ftsTarget, Long.MAX_VALUE, fragmentIds);
                scalarBuilt = LanceIndexBuilder.ensureScalarIndexes(dataset, scalarTarget, Long.MAX_VALUE, fragmentIds);
                vectorBuilt = LanceIndexBuilder.ensureVectorIndexes(dataset, vectorTarget, Long.MAX_VALUE, fragmentIds);
            }
        } catch (Exception e) {
            sendError(channel, e);
            return;
        }

        final List<String> ftsBuiltFinal = ftsBuilt;
        final List<String> scalarBuiltFinal = scalarBuilt;
        final List<String> vectorBuiltFinal = vectorBuilt;
        final List<Integer> fragmentIdsResp = toIntList(fragmentIdsRaw).orElse(null);
        client.admin().indices().refresh(new RefreshRequest(indexName), new ActionListener<RefreshResponse>() {
            @Override
            public void onResponse(RefreshResponse response) {
                writeResponse(channel, indexName, ftsBuiltFinal, scalarBuiltFinal, vectorBuiltFinal, columnsFilterRaw, fragmentIdsResp);
            }

            @Override
            public void onFailure(Exception e) {
                sendError(channel, e);
            }
        });
    }

    private static Set<String> filter(Set<String> derivedColumns, Set<String> columnsFilter) {
        if (columnsFilter == null) {
            return derivedColumns;
        }
        Set<String> result = new LinkedHashSet<>();
        for (String c : derivedColumns) {
            if (columnsFilter.contains(c)) {
                result.add(c);
            }
        }
        return result;
    }

    private static Optional<List<Integer>> toIntList(List<Number> raw) {
        if (raw == null) {
            return Optional.empty();
        }
        List<Integer> converted = new ArrayList<>(raw.size());
        for (Number n : raw) {
            converted.add(n.intValue());
        }
        return Optional.of(converted);
    }

    private static void writeResponse(
        RestChannel channel,
        String indexName,
        List<String> ftsBuilt,
        List<String> scalarBuilt,
        List<String> vectorBuilt,
        List<String> columnsFilter,
        List<Integer> fragmentIds
    ) {
        try (XContentBuilder b = channel.newBuilder()) {
            b.startObject();
            b.field("index", indexName);
            b.startObject("built");
            b.field("fts", ftsBuilt);
            b.field("scalar", scalarBuilt);
            b.field("vector", vectorBuilt);
            b.endObject();
            if (columnsFilter != null) {
                b.field("columns_filter", columnsFilter);
            }
            if (fragmentIds != null) {
                b.field("fragment_ids", fragmentIds);
            }
            b.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, b));
        } catch (Exception e) {
            sendError(channel, e);
        }
    }

    private static void sendError(RestChannel channel, RestStatus status, String message) {
        try {
            channel.sendResponse(new BytesRestResponse(status, message));
        } catch (Exception inner) {
            // channel already closed
        }
    }

    private static void sendError(RestChannel channel, Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (Exception inner) {
            // channel already closed
        }
    }
}

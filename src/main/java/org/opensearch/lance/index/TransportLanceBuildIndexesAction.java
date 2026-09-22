/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.lance.LanceInternalHeaders;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceIndexBuilder;
import org.opensearch.lance.engine.LanceLocalClones;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Serves {@link LanceBuildIndexesAction}: resolves the Lance table
 * behind the index from cluster state, runs the FTS / scalar / vector
 * builders (or Lance's optimize path) on it, then refreshes the index so
 * the reader picks up the new Lance version. Running this behind a
 * transport action places it after the {@link ActionFilters} chain, so a
 * security plugin rejects a caller without
 * {@code indices:admin/lance/build_indexes} on the index before the
 * plugin opens the table.
 *
 * <p>Two modes:
 * <ul>
 *   <li>{@code optimize=false} (default) runs the builders for the target
 *       columns discovered by {@link RestAttachAction#derive}. Columns
 *       already carrying an index are skipped. {@code fragment_ids}
 *       produces a partial initial index over the requested fragments
 *       only. {@code fts_columns} names Utf8 columns that get an FTS
 *       index instead of the BTree a keyword column would otherwise
 *       receive; {@code tokenizer} names the Lance {@code base_tokenizer}
 *       for those indexes (default {@code simple}), and Lance's rejection
 *       of the name comes back as 400 with the column under
 *       {@code failed.fts}; {@code with_position} (default false) stores
 *       token positions in them, which {@code lance_match_phrase}
 *       needs.</li>
 *   <li>{@code optimize=true} runs {@link Dataset#optimizeIndices} for the
 *       filtered indexes. Lance incrementally merges fragments not yet
 *       covered. {@code retrain=true} rebuilds the index (vector codebook
 *       and centroids relearn) rather than merging.</li>
 * </ul>
 *
 * <p>Skips the {@code lance.builder.max_rows} check so operators can force
 * a build on tables the automatic path passed over.
 *
 * <p>Outcome reporting: the response carries, per index kind, the names
 * that were built, the columns that were skipped with a reason, and the
 * columns whose Lance call threw with Lance's message. The HTTP status
 * comes from {@link #statusOf}: 200 when nothing failed, 400 when every
 * failure is Lance's invalid-input rejection, 500 otherwise. The body is
 * the same in all three cases so a partial success stays readable.
 *
 * <p>Threading: {@link Dataset#open} and the builders block on native I/O,
 * so {@link #doExecute} hands the operation to the generic pool. The
 * cluster state read that resolves the table is a local lookup and runs
 * before the hand-off.
 */
public final class TransportLanceBuildIndexesAction extends HandledTransportAction<LanceBuildIndexesRequest, LanceBuildIndexesResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportLanceBuildIndexesAction.class);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportLanceBuildIndexesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client
    ) {
        super(LanceBuildIndexesAction.NAME, transportService, actionFilters, LanceBuildIndexesRequest::new, ThreadPool.Names.GENERIC);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, LanceBuildIndexesRequest request, ActionListener<LanceBuildIndexesResponse> listener) {
        String indexName = request.index();
        IndexMetadata metadata = clusterService.state().metadata().index(indexName);
        if (metadata == null) {
            listener.onFailure(new IndexNotFoundException(indexName));
            return;
        }
        String tableUri = metadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
        if (tableUri == null) {
            listener.onFailure(new IllegalArgumentException("index " + indexName + " is not a Lance index"));
            return;
        }
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(metadata.getSettings());
        // The stored overrides steer the derivation below: a column the
        // operator overrode to keyword must not become an FTS build
        // target even though it is Utf8.
        LanceOverrides overrides = LanceOverrides.of(metadata.getSettings());
        if (LanceLocalClones.isNodeLocal(metadata.getSettings())) {
            threadPool.executor(ThreadPool.Names.GENERIC)
                .execute(ActionRunnable.wrap(listener, l -> buildNodeLocal(request, tableUri, storageOptions, l)));
            return;
        }
        threadPool.executor(ThreadPool.Names.GENERIC)
            .execute(ActionRunnable.wrap(listener, l -> buildAndRefresh(request, tableUri, storageOptions, overrides, l)));
    }

    /**
     * The {@code node_local} build: resolve the source's current manifest
     * version once, fan the request out to every data node (each builds
     * into its own shallow clone), merge the per-node outcomes, and
     * refresh the index so every shard reader picks the new clone
     * versions up. The response carries the merged {@code built} /
     * {@code skipped} / {@code failed} lists plus a {@code nodes} block
     * with each node's own outcome under its node id; a node whose leg
     * failed outright appears there with an {@code error} instead.
     */
    private void buildNodeLocal(
        LanceBuildIndexesRequest request,
        String tableUri,
        StorageOptions storageOptions,
        ActionListener<LanceBuildIndexesResponse> listener
    ) throws Exception {
        String indexName = request.index();
        long sourceVersion;
        try (Dataset source = LanceRegistry.openDataset(tableUri, storageOptions)) {
            sourceVersion = source.version();
        }
        String[] dataNodeIds = clusterService.state().nodes().getDataNodes().keySet().toArray(new String[0]);
        LanceBuildIndexesNodesRequest nodesRequest = new LanceBuildIndexesNodesRequest(request, sourceVersion, dataNodeIds);
        client.execute(LanceBuildIndexesNodesAction.INSTANCE, nodesRequest, ActionListener.wrap(nodesResponse -> {
            LanceBuildIndexesResponse response = mergeNodeResponses(indexName, request, nodesResponse);
            Map<String, String> mappingJsonByNode = new LinkedHashMap<>();
            for (LanceBuildIndexesNodeResponse node : nodesResponse.getNodes()) {
                if (node.mappingJson() != null) {
                    mappingJsonByNode.put(node.getNode().getId(), node.mappingJson());
                }
            }
            MappingConsensus consensus = mappingConsensus(mappingJsonByNode);
            if (consensus.error() != null) {
                // Every clone must derive the same mapping because they
                // were all cloned from the same source version and built
                // with the same request; a disagreement means the nodes
                // are not serving the same schema, and applying either
                // side would hide that. Fail loudly instead.
                listener.onFailure(new IllegalStateException(consensus.error()));
                return;
            }
            applyMappingAndRefresh(indexName, consensus.mappingJson(), response, listener);
        }, listener::onFailure));
    }

    /** The one mapping every node leg agrees on, or the error to fail the build with. */
    record MappingConsensus(String mappingJson, String error) {}

    /**
     * Compare the mapping JSON each node leg re-derived from its clone.
     * The comparison normalises through a parsed map so key order and
     * whitespace differences do not count as disagreement. Returns the
     * agreed mapping (or none when no node sent one), or an error naming
     * the node whose mapping is the reference and the node ids that
     * disagree with it.
     */
    static MappingConsensus mappingConsensus(Map<String, String> mappingJsonByNode) {
        String referenceNode = null;
        String referenceJson = null;
        Map<String, Object> referenceMap = null;
        List<String> disagreeing = new ArrayList<>();
        for (Map.Entry<String, String> entry : mappingJsonByNode.entrySet()) {
            Map<String, Object> parsed = XContentHelper.convertToMap(new BytesArray(entry.getValue()), false, XContentType.JSON).v2();
            if (referenceMap == null) {
                referenceNode = entry.getKey();
                referenceJson = entry.getValue();
                referenceMap = parsed;
            } else if (!referenceMap.equals(parsed)) {
                disagreeing.add(entry.getKey());
            }
        }
        if (referenceJson == null) {
            return new MappingConsensus(null, null);
        }
        if (!disagreeing.isEmpty()) {
            return new MappingConsensus(
                null,
                "node_local build derived different mappings across the data nodes: nodes "
                    + disagreeing
                    + " disagree with node ["
                    + referenceNode
                    + "]; no mapping was applied. Re-run POST /_lance/build_indexes once the nodes serve the same clone version"
            );
        }
        return new MappingConsensus(referenceJson, null);
    }

    /**
     * Bring the index's mapping in line with what the clones now carry,
     * then refresh and answer. A first FTS build flips the column from
     * keyword to lance_text, which PutMapping refuses as a type change;
     * in that case the index is rebuilt the way the namespace poll
     * rebuilds it on the same flip (delete, then re-create with the same
     * Lance settings and the new mapping). The clone directories are
     * keyed by index name and survive the rebuild, so the structures the
     * nodes just built stay in place.
     */
    private void applyMappingAndRefresh(
        String indexName,
        String mappingJson,
        LanceBuildIndexesResponse response,
        ActionListener<LanceBuildIndexesResponse> listener
    ) {
        Runnable refreshAndRespond = () -> client.admin()
            .indices()
            .refresh(
                new RefreshRequest(indexName),
                ActionListener.wrap((RefreshResponse r) -> listener.onResponse(response), listener::onFailure)
            );
        if (mappingJson == null) {
            refreshAndRespond.run();
            return;
        }
        // The mapping update (and, on the type-change path below, the
        // delete half of the rebuild) is plugin housekeeping that follows
        // from the build the caller was already authorised for, so it
        // runs under a stashed context with the plugin's internal header,
        // the way the re-create call does. A role holding only
        // indices:admin/lance/build_indexes and indices:admin/refresh
        // must not need mapping or delete privileges for the first FTS
        // build that flips a keyword column.
        ThreadContext threadContext = client.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(LanceInternalHeaders.LANCE_INTERNAL_CREATE_INDEX, "true");
            client.admin()
                .indices()
                .preparePutMapping(indexName)
                .setSource(mappingJson, MediaTypeRegistry.JSON)
                .execute(ActionListener.wrap(ack -> refreshAndRespond.run(), e -> {
                    if (isTypeChangeRefusal(e)) {
                        rebuildIndexWithMapping(indexName, mappingJson, refreshAndRespond, listener);
                    } else {
                        LOGGER.warn("mapping re-derivation after node_local build failed for {}: {}", indexName, e.getMessage());
                        refreshAndRespond.run();
                    }
                }));
        }
    }

    /**
     * Whether {@code e} is PutMapping refusing a field type change
     * (keyword to lance_text after a first FTS build). The refusal
     * arrives wrapped in a RemoteTransportException when the mapping
     * update ran on the cluster manager, so the cause chain is walked
     * rather than only the top message.
     */
    private static boolean isTypeChangeRefusal(Exception e) {
        Throwable cursor = e;
        while (cursor != null) {
            String message = cursor.getMessage();
            if (message != null && message.contains("cannot be changed from type")) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private void rebuildIndexWithMapping(
        String indexName,
        String mappingJson,
        Runnable refreshAndRespond,
        ActionListener<LanceBuildIndexesResponse> listener
    ) {
        IndexMetadata metadata = clusterService.state().metadata().index(indexName);
        if (metadata == null) {
            listener.onFailure(new IndexNotFoundException(indexName));
            return;
        }
        Settings old = metadata.getSettings();
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        for (String key : old.keySet()) {
            if (key.startsWith("index.lance.")) {
                settings.copy(key, old);
            }
        }
        LOGGER.warn(
            "node_local build on {} flipped a column between keyword and lance_text; rebuilding the OpenSearch index "
                + "(the Lance source and the node-local clones are untouched)",
            indexName
        );
        ThreadContext threadContext = client.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(LanceInternalHeaders.LANCE_INTERNAL_CREATE_INDEX, "true");
            client.admin().indices().delete(new DeleteIndexRequest(indexName), ActionListener.wrap(deleted -> {
                ThreadContext createContext = client.threadPool().getThreadContext();
                CreateIndexRequest create = new CreateIndexRequest(indexName).settings(settings.build()).mapping(mappingJson);
                try (ThreadContext.StoredContext restored = createContext.stashContext()) {
                    createContext.putHeader(LanceInternalHeaders.LANCE_INTERNAL_CREATE_INDEX, "true");
                    client.admin().indices().create(create, ActionListener.wrap(created -> refreshAndRespond.run(), listener::onFailure));
                }
            }, listener::onFailure));
        }
    }

    static LanceBuildIndexesResponse mergeNodeResponses(
        String indexName,
        LanceBuildIndexesRequest request,
        LanceBuildIndexesNodesResponse nodesResponse
    ) {
        Map<String, LanceBuildIndexesResponse.NodeResult> nodes = new LinkedHashMap<>();
        LinkedHashSet<String> ftsBuilt = new LinkedHashSet<>();
        LinkedHashSet<String> scalarBuilt = new LinkedHashSet<>();
        LinkedHashSet<String> vectorBuilt = new LinkedHashSet<>();
        LinkedHashSet<LanceBuildIndexesResponse.ColumnResult> ftsSkipped = new LinkedHashSet<>();
        LinkedHashSet<LanceBuildIndexesResponse.ColumnResult> scalarSkipped = new LinkedHashSet<>();
        LinkedHashSet<LanceBuildIndexesResponse.ColumnResult> vectorSkipped = new LinkedHashSet<>();
        LinkedHashSet<LanceBuildIndexesResponse.ColumnResult> ftsFailed = new LinkedHashSet<>();
        LinkedHashSet<LanceBuildIndexesResponse.ColumnResult> scalarFailed = new LinkedHashSet<>();
        LinkedHashSet<LanceBuildIndexesResponse.ColumnResult> vectorFailed = new LinkedHashSet<>();
        boolean sawBadRequest = false;
        boolean sawServerError = false;
        for (LanceBuildIndexesNodeResponse node : nodesResponse.getNodes()) {
            nodes.put(node.getNode().getId(), new LanceBuildIndexesResponse.NodeResult(node.fts(), node.scalar(), node.vector(), null));
            ftsBuilt.addAll(node.fts().built());
            scalarBuilt.addAll(node.scalar().built());
            vectorBuilt.addAll(node.vector().built());
            ftsSkipped.addAll(node.fts().skipped());
            scalarSkipped.addAll(node.scalar().skipped());
            vectorSkipped.addAll(node.vector().skipped());
            ftsFailed.addAll(node.fts().failed());
            scalarFailed.addAll(node.scalar().failed());
            vectorFailed.addAll(node.vector().failed());
            if (node.status() == RestStatus.BAD_REQUEST) {
                sawBadRequest = true;
            } else if (node.status() != RestStatus.OK) {
                sawServerError = true;
            }
        }
        for (FailedNodeException failure : nodesResponse.failures()) {
            nodes.put(failure.nodeId(), new LanceBuildIndexesResponse.NodeResult(null, null, null, failure.getMessage()));
            sawServerError = true;
        }
        RestStatus status = sawServerError ? RestStatus.INTERNAL_SERVER_ERROR : sawBadRequest ? RestStatus.BAD_REQUEST : RestStatus.OK;
        return new LanceBuildIndexesResponse(
            indexName,
            new LanceBuildIndexesResponse.KindResult(new ArrayList<>(ftsBuilt), new ArrayList<>(ftsSkipped), new ArrayList<>(ftsFailed)),
            new LanceBuildIndexesResponse.KindResult(
                new ArrayList<>(scalarBuilt),
                new ArrayList<>(scalarSkipped),
                new ArrayList<>(scalarFailed)
            ),
            new LanceBuildIndexesResponse.KindResult(
                new ArrayList<>(vectorBuilt),
                new ArrayList<>(vectorSkipped),
                new ArrayList<>(vectorFailed)
            ),
            request.columns(),
            request.fragmentIds(),
            status,
            nodes
        );
    }

    /** The three per-kind results of one build pass over one dataset. */
    record BuildOutcome(LanceIndexBuilder.BuildResult fts, LanceIndexBuilder.BuildResult scalar, LanceIndexBuilder.BuildResult vector) {

        List<LanceIndexBuilder.Failed> failures() {
            List<LanceIndexBuilder.Failed> failures = new ArrayList<>();
            failures.addAll(fts.failed());
            failures.addAll(scalar.failed());
            failures.addAll(vector.failed());
            return failures;
        }
    }

    /**
     * Run the builders (or Lance's optimize path) for {@code request}
     * against {@code dataset}. Shared by the in-table path (one commit
     * into the source) and the node-local path (each data node commits
     * into its own clone). The stored overrides steer the derivation: a
     * column the operator overrode to keyword must not become an FTS
     * build target even though it is Utf8.
     */
    static BuildOutcome runBuilders(Dataset dataset, LanceBuildIndexesRequest request, LanceOverrides overrides) throws Exception {
        RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, overrides, true);
        Set<String> columnsFilter = request.columns() != null ? new LinkedHashSet<>(request.columns()) : null;
        if (columnsFilter != null) {
            // Reject unknown columns up front so callers don't get a 200
            // response with `built: []` and no explanation for a typo.
            // "Known" here means the column is FTS-eligible,
            // scalar-eligible, or vector-eligible per derive; other
            // columns are stored-only and cannot carry an index.
            Set<String> known = new LinkedHashSet<>();
            known.addAll(derivation.ftsColumns());
            known.addAll(derivation.scalarColumns());
            known.addAll(derivation.vectorColumns());
            for (String c : columnsFilter) {
                if (!known.contains(c)) {
                    throw new IllegalArgumentException("column [" + c + "] is not indexable by build_indexes; known columns are " + known);
                }
            }
        }
        Set<String> ftsTarget = new LinkedHashSet<>(filter(derivation.ftsColumns(), columnsFilter));
        Set<String> scalarTarget = new LinkedHashSet<>(filter(derivation.scalarColumns(), columnsFilter));
        Set<String> vectorTarget = filter(derivation.vectorColumns(), columnsFilter);
        if (request.ftsColumns() != null) {
            // derive() classifies a Utf8 column by the indexes it
            // already carries: with an FTS index it is lance_text and
            // sits in ftsColumns, without one it is keyword and sits in
            // scalarColumns. A first FTS build therefore needs the
            // caller to name the column; move it from the scalar
            // target to the FTS target so it gets an inverted index
            // rather than a BTree.
            Set<String> utf8 = utf8Columns(dataset);
            for (String c : request.ftsColumns()) {
                if (!utf8.contains(c)) {
                    throw new IllegalArgumentException(
                        "fts_columns entry [" + c + "] is not a Utf8 column of the table; Utf8 columns are " + utf8
                    );
                }
                ftsTarget.add(c);
                scalarTarget.remove(c);
            }
        }
        if (request.optimize()) {
            // Optimize path: hand the actual Lance index names (via
            // describeIndices) to OptimizeIndices instead of assuming the
            // `<col>_fts` / `<col>_btree` / `<col>_vec` convention. Lance
            // silently ignores unknown names, so guessing would return
            // 200 with `built: [...]` even when nothing was touched.
            return new BuildOutcome(
                LanceIndexBuilder.optimizeExistingFtsIndexes(dataset, ftsTarget, request.retrain()),
                LanceIndexBuilder.optimizeExistingScalarIndexes(dataset, scalarTarget, request.retrain()),
                LanceIndexBuilder.optimizeExistingVectorIndexes(dataset, vectorTarget, request.retrain())
            );
        }
        Optional<List<Integer>> fragmentIds = Optional.ofNullable(request.fragmentIds());
        String tokenizer = request.tokenizer() != null ? request.tokenizer() : LanceIndexBuilder.DEFAULT_FTS_TOKENIZER;
        return new BuildOutcome(
            LanceIndexBuilder.ensureFtsIndexes(dataset, ftsTarget, Long.MAX_VALUE, fragmentIds, tokenizer, request.withPosition()),
            LanceIndexBuilder.ensureScalarIndexes(dataset, scalarTarget, Long.MAX_VALUE, fragmentIds),
            LanceIndexBuilder.ensureVectorIndexes(dataset, vectorTarget, Long.MAX_VALUE, fragmentIds)
        );
    }

    private void buildAndRefresh(
        LanceBuildIndexesRequest request,
        String tableUri,
        StorageOptions storageOptions,
        LanceOverrides overrides,
        ActionListener<LanceBuildIndexesResponse> listener
    ) throws Exception {
        String indexName = request.index();
        BuildOutcome outcome;
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, storageOptions)) {
            outcome = runBuilders(dataset, request, overrides);
        }

        List<LanceIndexBuilder.Failed> failures = outcome.failures();
        LanceBuildIndexesResponse response = new LanceBuildIndexesResponse(
            indexName,
            toKindResult(outcome.fts()),
            toKindResult(outcome.scalar()),
            toKindResult(outcome.vector()),
            request.columns(),
            request.fragmentIds(),
            statusOf(failures)
        );
        client.admin()
            .indices()
            .refresh(
                new RefreshRequest(indexName),
                ActionListener.wrap((RefreshResponse r) -> listener.onResponse(response), listener::onFailure)
            );
    }

    /**
     * The one place that turns per-column failures into an HTTP status.
     * No failure is 200 (skipped columns are not failures). Any failure is
     * 500, unless every failure is Lance's invalid-input rejection
     * (unknown tokenizer, malformed index params), which is the caller's
     * mistake and answers 400. A mix stays 500 because the I/O error is
     * the one the operator has to act on.
     */
    static RestStatus statusOf(List<LanceIndexBuilder.Failed> failures) {
        if (failures.isEmpty()) {
            return RestStatus.OK;
        }
        for (LanceIndexBuilder.Failed failure : failures) {
            if (!failure.invalidInput()) {
                return RestStatus.INTERNAL_SERVER_ERROR;
            }
        }
        return RestStatus.BAD_REQUEST;
    }

    static LanceBuildIndexesResponse.KindResult toKindResult(LanceIndexBuilder.BuildResult result) {
        List<LanceBuildIndexesResponse.ColumnResult> skipped = new ArrayList<>(result.skipped().size());
        for (LanceIndexBuilder.Skipped s : result.skipped()) {
            skipped.add(new LanceBuildIndexesResponse.ColumnResult(s.column(), s.reason()));
        }
        List<LanceBuildIndexesResponse.ColumnResult> failed = new ArrayList<>(result.failed().size());
        for (LanceIndexBuilder.Failed f : result.failed()) {
            failed.add(new LanceBuildIndexesResponse.ColumnResult(f.column(), f.reason()));
        }
        return new LanceBuildIndexesResponse.KindResult(result.built(), skipped, failed);
    }

    private static Set<String> utf8Columns(Dataset dataset) {
        Set<String> utf8 = new LinkedHashSet<>();
        for (Field field : dataset.getSchema().getFields()) {
            if (field.getType() instanceof ArrowType.Utf8) {
                utf8.add(field.getName());
            }
        }
        return utf8;
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
}

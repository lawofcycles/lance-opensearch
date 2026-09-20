/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.lance.Dataset;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceIndexBuilder;
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
 *       {@code failed.fts}.</li>
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
        threadPool.executor(ThreadPool.Names.GENERIC)
            .execute(ActionRunnable.wrap(listener, l -> buildAndRefresh(request, tableUri, storageOptions, l)));
    }

    private void buildAndRefresh(
        LanceBuildIndexesRequest request,
        String tableUri,
        StorageOptions storageOptions,
        ActionListener<LanceBuildIndexesResponse> listener
    ) throws Exception {
        String indexName = request.index();
        LanceIndexBuilder.BuildResult fts;
        LanceIndexBuilder.BuildResult scalar;
        LanceIndexBuilder.BuildResult vector;
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, storageOptions)) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset);
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
                        throw new IllegalArgumentException(
                            "column [" + c + "] is not indexable by build_indexes; known columns are " + known
                        );
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
                fts = LanceIndexBuilder.optimizeExistingFtsIndexes(dataset, ftsTarget, request.retrain());
                scalar = LanceIndexBuilder.optimizeExistingScalarIndexes(dataset, scalarTarget, request.retrain());
                vector = LanceIndexBuilder.optimizeExistingVectorIndexes(dataset, vectorTarget, request.retrain());
            } else {
                Optional<List<Integer>> fragmentIds = Optional.ofNullable(request.fragmentIds());
                String tokenizer = request.tokenizer() != null ? request.tokenizer() : LanceIndexBuilder.DEFAULT_FTS_TOKENIZER;
                fts = LanceIndexBuilder.ensureFtsIndexes(dataset, ftsTarget, Long.MAX_VALUE, fragmentIds, tokenizer);
                scalar = LanceIndexBuilder.ensureScalarIndexes(dataset, scalarTarget, Long.MAX_VALUE, fragmentIds);
                vector = LanceIndexBuilder.ensureVectorIndexes(dataset, vectorTarget, Long.MAX_VALUE, fragmentIds);
            }
        }

        List<LanceIndexBuilder.Failed> failures = new ArrayList<>();
        failures.addAll(fts.failed());
        failures.addAll(scalar.failed());
        failures.addAll(vector.failed());
        LanceBuildIndexesResponse response = new LanceBuildIndexesResponse(
            indexName,
            toKindResult(fts),
            toKindResult(scalar),
            toKindResult(vector),
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

    private static LanceBuildIndexesResponse.KindResult toKindResult(LanceIndexBuilder.BuildResult result) {
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

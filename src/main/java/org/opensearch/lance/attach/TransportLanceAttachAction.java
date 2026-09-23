/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.schema.LanceField;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.lance.LanceInternalHeaders;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.dispatch.LanceCreateIndexActionFilter;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.namespace.AllowedTableRoots;
import org.opensearch.lance.namespace.LanceNamespaceService;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Serves {@link LanceAttachAction}: opens the table, derives the
 * mapping, creates the index, and registers it with the namespace
 * poller. Running this behind a transport action places it after the
 * {@link ActionFilters} chain, so a security plugin rejects a caller
 * without {@code cluster:admin/lance/attach} before the plugin touches
 * the table.
 *
 * <p>Routing: the action is cluster-manager scoped. Whichever node
 * receives {@code POST /_lance/attach} forwards the request to the
 * elected cluster manager, and {@link #clusterManagerOperation} runs
 * there. Attach reads and writes cluster state (index existence check,
 * create index, poll tracking), so the manager is where that work
 * belongs. It is also what keeps the internal create-index header
 * intact: the create call below runs through the node client on the
 * manager itself, so {@code indices:admin/create} executes locally and
 * is never forwarded over transport. A security plugin stashes the
 * thread context on every outbound transport request and copies only
 * its own headers, so a create that had to hop to the manager would
 * arrive without the header and {@link LanceCreateIndexActionFilter}
 * would reject it.
 *
 * <p>Threading: {@link Dataset#open} and the schema walk block on
 * native I/O, so {@link #executor()} names the generic pool. The base
 * class hands {@link #clusterManagerOperation} to that pool instead of
 * running it on the transport thread or the cluster state update
 * thread of the manager.
 *
 * <p>The index is created through the node client with the internal
 * create-index header stamped on a stashed thread context. The
 * caller's privilege has already been evaluated against the attach
 * action; the create runs as the plugin so a role does not also need
 * {@code indices:admin/create} on a name derived from the table path.
 *
 * <p>Existing-index handling: when the target index already exists the
 * action reads its settings and only reports {@code already_attached}
 * if the existing index is a Lance index pointing to the same table.
 * Any other clash (plain index reusing the name, Lance index for a
 * different table) is a 409 so the operator picks a different name
 * explicitly.
 */
public final class TransportLanceAttachAction extends TransportClusterManagerNodeAction<LanceAttachRequest, LanceAttachResponse> {

    private static final Logger LOG = LogManager.getLogger(TransportLanceAttachAction.class);

    private final Client client;
    private final LanceNamespaceService namespaceService;
    private final AllowedTableRoots allowedRoots;
    private final AnalysisRegistry analysisRegistry;

    @Inject
    public TransportLanceAttachAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        LanceNamespaceService namespaceService,
        AllowedTableRoots allowedRoots,
        AnalysisRegistry analysisRegistry
    ) {
        super(
            LanceAttachAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            LanceAttachRequest::new,
            indexNameExpressionResolver
        );
        this.client = client;
        this.namespaceService = namespaceService;
        this.allowedRoots = allowedRoots;
        this.analysisRegistry = analysisRegistry;
    }

    @Override
    protected String executor() {
        // Opening the table is blocking native I/O; keep it off the
        // transport and cluster state threads of the manager.
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected LanceAttachResponse read(StreamInput in) throws IOException {
        return new LanceAttachResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(LanceAttachRequest request, ClusterState state) {
        // Attach ends in a create-index metadata write. The index name
        // may still be derived from the table, so check the global
        // metadata-write block rather than a per-index one.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(LanceAttachRequest request, ClusterState state, ActionListener<LanceAttachResponse> listener)
        throws Exception {
        // The state handed in here is not used: nothing before the
        // create depends on cluster state (allowlist and derivation
        // read the request and the table), and the one read that does,
        // the existing-index check, happens after the create has
        // failed with ResourceAlreadyExistsException, which this state
        // predates. verifyExistingLanceIndex reads a fresher state then.
        attach(request, listener);
    }

    private void attach(LanceAttachRequest request, ActionListener<LanceAttachResponse> listener) throws Exception {
        String table = request.table();
        if (!allowedRoots.allows(table)) {
            throw new OpenSearchStatusException(
                "table [" + table + "] is not under any of the configured lance.allowed_table_roots",
                RestStatus.FORBIDDEN
            );
        }
        String indexName = request.indexName() != null ? request.indexName() : tableName(table);
        // Names the node that runs the attach, so a cluster log shows
        // where a forwarded request ended up.
        LOG.debug("lance.attach: attaching table [{}] as index [{}] on this node", table, indexName);
        // A tag is resolved to the version it points at right now so the
        // derivation below reads the tagged snapshot. The engine and the
        // namespace poll resolve it again on every open and poll, which is
        // what makes the index follow the tag when Lance moves it.
        Optional<Long> openVersion = request.pinnedVersion();
        if (request.tag().isPresent()) {
            String tag = request.tag().get();
            try (Dataset latest = LanceRegistry.openDataset(table, request.storageOptions())) {
                try {
                    openVersion = Optional.of(latest.tags().getVersion(tag));
                } catch (RuntimeException e) {
                    // The table itself opened, so the failure is about the
                    // tag (unknown name is the common case). Surface Lance's
                    // message as a 400 rather than a generic 500.
                    throw new OpenSearchStatusException(
                        "tag [" + tag + "] could not be resolved on table [" + table + "]: " + e.getMessage(),
                        RestStatus.BAD_REQUEST,
                        e
                    );
                }
            }
        }
        ensureAnalyzerDerivedColumns(request, openVersion);
        RestAttachAction.Derivation derivation;
        long[] fragmentDocs;
        try (Dataset dataset = LanceRegistry.openDataset(table, request.storageOptions(), openVersion)) {
            derivation = RestAttachAction.derive(dataset, request.overrides());
            List<Fragment> fragments = dataset.getFragments();
            fragmentDocs = new long[fragments.size()];
            for (int i = 0; i < fragmentDocs.length; i++) {
                fragmentDocs[i] = fragments.get(i).metadata().getPhysicalRows();
            }
            if (!derivation.nestedColumns().isEmpty()) {
                // The reader of a fragment with nested columns exposes
                // rows plus nested elements as docs, so the Lucene bound
                // arithmetic must count the elements too. One scan of the
                // nested columns reads each row's element count from the
                // list offsets.
                addNestedElementCounts(dataset, derivation.nestedColumns(), fragments, fragmentDocs);
            }
        }
        warnIfInvertedIndexExceedsShardShare(indexName, derivation);
        long maxDocs = clusterService.getClusterSettings().get(LancePlugin.MAX_DOCS_PER_READER_SETTING);
        boolean luceneBoundExceeded = checkLuceneBound(
            indexName,
            table,
            fragmentDocs,
            maxDocs,
            clusterService.state().nodes().getDataNodes().size()
        );
        createIndex(
            indexName,
            table,
            derivation,
            request.storageOptions(),
            request.pinnedVersion(),
            request.tag(),
            request.indexPlacement(),
            luceneBoundExceeded,
            listener
        );
    }

    /**
     * The write side of the {@code type: text_analyzer} override, run
     * before the mapping derivation so the derivation sees the derived
     * tokens columns: resolve every declared analyzer name (unknown is
     * a 400), then create, backfill and index the missing derived
     * columns through {@link LanceTextAnalyzerBackfill}. With
     * {@code derive: async} the backfill runs on the generic pool after
     * this method returns and the derivation maps the base column by
     * the default rules for now; the namespace poll re-derives the
     * mapping when the backfill commit advances the manifest, which is
     * when the column flips to the analyzer mode. A snapshot pinned by
     * {@code version} or {@code tag} cannot be written, so every
     * derived column must already exist there.
     */
    private void ensureAnalyzerDerivedColumns(LanceAttachRequest request, Optional<Long> openVersion) throws Exception {
        Map<String, LanceOverrides.Column> textAnalyzer = request.overrides().textAnalyzerColumns();
        if (textAnalyzer.isEmpty()) {
            return;
        }
        Map<String, Analyzer> resolved = new LinkedHashMap<>();
        for (Map.Entry<String, LanceOverrides.Column> entry : textAnalyzer.entrySet()) {
            String analyzerName = entry.getValue().analyzer();
            Analyzer analyzer;
            try {
                analyzer = analysisRegistry.getAnalyzer(analyzerName);
            } catch (Exception e) {
                throw new OpenSearchStatusException(
                    "[overrides." + entry.getKey() + ".analyzer=" + analyzerName + "] could not be built: " + e.getMessage(),
                    RestStatus.BAD_REQUEST,
                    e
                );
            }
            if (analyzer == null) {
                throw new OpenSearchStatusException(
                    "[overrides."
                        + entry.getKey()
                        + ".analyzer="
                        + analyzerName
                        + "] does not name a built-in OpenSearch analyzer; index-scoped custom analyzers are not resolvable at attach",
                    RestStatus.BAD_REQUEST
                );
            }
            resolved.put(entry.getKey(), analyzer);
        }
        if (openVersion.isPresent()) {
            // A pinned snapshot is readonly by design: the backfill
            // commit would land after the pin and never be visible. The
            // attach may still pin a version where a derived column
            // already exists (a re-attach of an already-derived table).
            try (Dataset pinned = LanceRegistry.openDataset(request.table(), request.storageOptions(), openVersion)) {
                Set<String> names = new HashSet<>();
                for (LanceField field : pinned.getLanceSchema().fields()) {
                    names.add(field.getName());
                }
                for (Map.Entry<String, LanceOverrides.Column> entry : textAnalyzer.entrySet()) {
                    String derived = LanceOverrides.derivedColumnName(entry.getKey(), entry.getValue());
                    if (!names.contains(derived)) {
                        throw new OpenSearchStatusException(
                            "[overrides."
                                + entry.getKey()
                                + ".type=text_analyzer] cannot backfill derived column ["
                                + derived
                                + "] on a snapshot pinned by [version] or [tag]; attach the latest version first, or pin a "
                                + "version that already carries the derived column",
                            RestStatus.BAD_REQUEST
                        );
                    }
                }
            }
            return;
        }
        if (request.asyncDerive()) {
            String table = request.table();
            threadPool.generic().execute(() -> {
                try (Dataset dataset = LanceRegistry.openDataset(table, request.storageOptions())) {
                    LanceTextAnalyzerBackfill.Ensured ensured = LanceTextAnalyzerBackfill.ensureDerivedColumns(
                        dataset,
                        textAnalyzer,
                        resolved::get,
                        LanceRegistry.allocator()
                    );
                    LOG.info(
                        "async text_analyzer backfill finished for table {}: created {}, already present {}",
                        table,
                        ensured.created(),
                        ensured.existing()
                    );
                } catch (Exception e) {
                    LOG.warn("async text_analyzer backfill failed for table {}; re-attach to retry", table, e);
                }
            });
            return;
        }
        try (Dataset dataset = LanceRegistry.openDataset(request.table(), request.storageOptions())) {
            try {
                LanceTextAnalyzerBackfill.ensureDerivedColumns(dataset, textAnalyzer, resolved::get, LanceRegistry.allocator());
            } catch (IllegalArgumentException e) {
                throw new OpenSearchStatusException(e.getMessage(), RestStatus.BAD_REQUEST, e);
            }
        }
    }

    /**
     * Add each fragment's nested element total to {@code fragmentDocs}
     * (indexed like {@code fragments}). One scan of the whole table
     * projecting the nested columns; the element count per row comes
     * from the list offsets, and rows a deletion file hides are skipped
     * by the scan, matching the reader's doc id layout.
     */
    static void addNestedElementCounts(Dataset dataset, Set<String> nestedColumns, List<Fragment> fragments, long[] fragmentDocs)
        throws Exception {
        Map<Integer, Integer> indexOfFragment = new HashMap<>(fragments.size() * 2);
        for (int i = 0; i < fragments.size(); i++) {
            indexOfFragment.put(fragments.get(i).getId(), i);
        }
        ScanOptions options = new ScanOptions.Builder().columns(new ArrayList<>(nestedColumns)).withRowAddress(true).build();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                for (String column : nestedColumns) {
                    ListVector list = (ListVector) root.getVector(column);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        if (list.isNull(i)) {
                            continue;
                        }
                        Integer index = indexOfFragment.get((int) (rowAddr.get(i) >>> 32));
                        if (index != null) {
                            fragmentDocs[index] += list.getElementEndIndex(i) - list.getElementStartIndex(i);
                        }
                    }
                }
            }
        }
    }

    /**
     * Whether the table has more docs than one Lucene reader may hold
     * ({@code maxDocs}); a fragment's docs are its physical rows plus,
     * for tables with nested columns, its nested elements. Such a table
     * attaches: the fragment path serves it in groups of fragments
     * within the bound and the shard reader holds the leading fragments
     * that fit, which one WARN says. A single fragment above the bound
     * cannot be read by any reader, so that table is refused with 400.
     */
    static boolean checkLuceneBound(String indexName, String table, long[] fragmentDocs, long maxDocs, int dataNodes) {
        long totalDocs = 0L;
        for (long docs : fragmentDocs) {
            if (docs > maxDocs) {
                throw new OpenSearchStatusException(
                    "table ["
                        + table
                        + "] has a fragment of "
                        + docs
                        + " docs (rows plus nested elements), above the bound of "
                        + maxDocs
                        + " docs per Lucene reader; no reader can hold it. Rewrite the table with smaller fragments",
                    RestStatus.BAD_REQUEST
                );
            }
            totalDocs += docs;
        }
        if (totalDocs <= maxDocs) {
            return false;
        }
        long readerDocs = 0L;
        int held = LanceDirectoryReader.leadingFragmentsWithinBound(fragmentDocs, maxDocs);
        for (int i = 0; i < held; i++) {
            readerDocs += fragmentDocs[i];
        }
        // The fragment path spreads the fragments round robin over the
        // data nodes and cuts each node's share into groups within the
        // bound; count the groups that would give right now.
        int groups = 0;
        int nodes = Math.max(1, dataNodes);
        for (int node = 0; node < nodes; node++) {
            List<Long> share = new ArrayList<>();
            for (int i = node; i < fragmentDocs.length; i += nodes) {
                share.add(fragmentDocs[i]);
            }
            long[] shareDocs = new long[share.size()];
            for (int i = 0; i < shareDocs.length; i++) {
                shareDocs[i] = share.get(i);
            }
            groups += LanceDirectoryReader.groupEnds(shareDocs, maxDocs).length;
        }
        LOG.warn(
            "lance.attach: table [{}] has {} docs, above the bound of {} docs per Lucene reader; the shard reader of [{}] holds {} of {} docs; "
                + "searches run on the fragment path in {} groups over {} data nodes",
            table,
            totalDocs,
            maxDocs,
            indexName,
            readerDocs,
            totalDocs,
            groups,
            nodes
        );
        return true;
    }

    private static void warnIfInvertedIndexExceedsShardShare(String indexName, RestAttachAction.Derivation derivation) {
        String warning = invertedIndexShardShareWarning(indexName, derivation, LanceRegistry.indexCacheSizing());
        if (warning != null) {
            LOG.warn(warning);
        }
    }

    /**
     * Lance keeps the whole document set of an inverted index as one
     * index cache entry and refuses an entry heavier than one cache
     * shard's share, so a table whose inverted index does not fit is
     * reloaded from storage on every full-text query. Returns the
     * warning to log when the table has a full-text column and its
     * estimated entry is heavier than the share, {@code null} when it
     * fits, has no full-text column, or no Session is installed. The
     * estimate is per full-text column, so it is compared once per
     * table.
     */
    static String invertedIndexShardShareWarning(
        String indexName,
        RestAttachAction.Derivation derivation,
        NativeMemoryLimit.IndexCacheSizing sizing
    ) {
        if (derivation.ftsColumns().isEmpty() || sizing == null) {
            return null;
        }
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(derivation.rows());
        if (estimate <= sizing.shardShareBytes()) {
            return null;
        }
        return "inverted index of ["
            + indexName
            + "] (~"
            + NativeMemoryLimit.humanReadable(estimate)
            + ") may not fit one index cache shard ("
            + NativeMemoryLimit.humanReadable(sizing.shardShareBytes())
            + "); raise lance.native_memory.limit or lower lance.cache.column_share";
    }

    private void createIndex(
        String indexName,
        String table,
        RestAttachAction.Derivation derivation,
        StorageOptions storageOptions,
        Optional<Long> pinnedVersion,
        Optional<String> tag,
        Optional<String> indexPlacement,
        boolean luceneBoundExceeded,
        ActionListener<LanceAttachResponse> listener
    ) {
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(LanceEngineFactory.TABLE_SETTING, table)
            .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, derivation.keyField())
            .put(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING, derivation.keyFieldType());
        if (!derivation.overridesJson().isEmpty()) {
            settings.put(LanceEngineFactory.OVERRIDES_SETTING, derivation.overridesJson());
        }
        pinnedVersion.ifPresent(v -> settings.put(LanceEngineFactory.VERSION_SETTING, v));
        tag.ifPresent(t -> settings.put(LanceEngineFactory.TAG_SETTING, t));
        indexPlacement.ifPresent(p -> settings.put(LanceEngineFactory.INDEX_PLACEMENT_SETTING, p));
        storageOptions.writeToSettings(settings);
        CreateIndexRequest create = new CreateIndexRequest(indexName).settings(settings.build()).mapping(derivation.mappingJson());

        // LanceCreateIndexActionFilter blocks user PUT /{index} that
        // tries to set index.lance.table. Stamp the internal header so
        // this plugin-issued call is recognised as legitimate. The
        // header lives in this node's ThreadContext only, which is
        // enough because this code runs on the elected cluster manager
        // and the create therefore executes locally. The stash also
        // drops the caller's identity for this one call, which is
        // intended: the caller was authorised against the attach
        // action, not against creating an index of this name.
        ThreadContext threadContext = client.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(LanceInternalHeaders.LANCE_INTERNAL_CREATE_INDEX, "true");
            client.admin().indices().create(create, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    // Register the attach-created index with the namespace
                    // poller unless the operator pinned a version. The
                    // poll runs on the elected cluster manager, which is
                    // the node this code runs on, so the registration
                    // lands in the memory the poll reads. Pinned
                    // indices stay on their manifest version by design
                    // (readonly snapshot for reproducibility), so the poll
                    // cycle does not need to touch them and would otherwise
                    // burn cycles probing for a manifest advance that must
                    // not change the reader. Tag-following indices are
                    // registered with their tag so the poll re-resolves it
                    // and refreshes when Lance moves the tag.
                    if (pinnedVersion.isEmpty()) {
                        namespaceService.registerAttachedIndex(indexName, table, derivation.version(), storageOptions, tag.orElse(null));
                    }
                    listener.onResponse(response(indexName, table, derivation, false, luceneBoundExceeded));
                }

                @Override
                public void onFailure(Exception e) {
                    if (!isAlreadyExists(e)) {
                        listener.onFailure(e);
                        return;
                    }
                    // The index already exists. Verify it is a Lance index
                    // for the same table before claiming success; otherwise
                    // attach would silently take credit for an unrelated
                    // index.
                    verifyExistingLanceIndex(indexName, table, derivation, storageOptions, luceneBoundExceeded, listener);
                }
            });
        }
    }

    private void verifyExistingLanceIndex(
        String indexName,
        String table,
        RestAttachAction.Derivation derivation,
        StorageOptions storageOptions,
        boolean luceneBoundExceeded,
        ActionListener<LanceAttachResponse> listener
    ) {
        // This runs on the elected cluster manager, whose applied state
        // already contains the index the create just collided with:
        // the manager publishes and applies each state update before it
        // executes the next task. The read is a direct clusterService
        // lookup rather than a cluster state request through the client
        // on purpose: the caller was already authorised under
        // cluster:admin/lance/attach, and this is plugin bookkeeping on
        // the manager, so the indices:monitor/state filter chain has
        // nothing to add.
        IndexMetadata md = clusterService.state().metadata().index(indexName);
        if (md == null) {
            // Race: the index disappeared between create and this read.
            // Treat as conflict rather than pretend attach succeeded.
            listener.onFailure(
                new OpenSearchStatusException("index " + indexName + " conflicts with a concurrent request", RestStatus.CONFLICT)
            );
            return;
        }
        String existing = md.getSettings().get(LanceEngineFactory.TABLE_SETTING);
        if (existing == null) {
            listener.onFailure(
                new OpenSearchStatusException(
                    "index " + indexName + " already exists and is not a Lance index; choose a different `name`",
                    RestStatus.CONFLICT
                )
            );
            return;
        }
        if (!existing.equals(table)) {
            listener.onFailure(
                new OpenSearchStatusException(
                    "index " + indexName + " already attached to a different table: " + existing,
                    RestStatus.CONFLICT
                )
            );
            return;
        }
        // Same table, so record the (index, table) pair with the
        // namespace poller in case this node has forgotten it
        // (cluster restart after attach, for example). The existing
        // index's own settings decide how it is registered, because
        // that is what its engine reads: a version pin keeps it out
        // of the poll cycle (readonly snapshot), and a stored tag is
        // what the poll has to re-resolve, even if this request named
        // a different tag or none.
        long existingVersion = md.getSettings().getAsLong(LanceEngineFactory.VERSION_SETTING, -1L);
        String existingTag = md.getSettings().get(LanceEngineFactory.TAG_SETTING, "");
        if (existingVersion < 0) {
            namespaceService.registerAttachedIndex(
                indexName,
                table,
                derivation.version(),
                storageOptions,
                existingTag.isEmpty() ? null : existingTag
            );
        }
        listener.onResponse(response(indexName, table, derivation, true, luceneBoundExceeded));
    }

    private static LanceAttachResponse response(
        String indexName,
        String table,
        RestAttachAction.Derivation derivation,
        boolean alreadyAttached,
        boolean luceneBoundExceeded
    ) {
        return new LanceAttachResponse(
            indexName,
            table,
            derivation.version(),
            derivation.rows(),
            derivation.fragments(),
            derivation.keyField(),
            derivation.mappingJson(),
            derivation.notes(),
            alreadyAttached,
            luceneBoundExceeded
        );
    }

    private static boolean isAlreadyExists(Throwable e) {
        Throwable cursor = e;
        while (cursor != null) {
            if (cursor instanceof ResourceAlreadyExistsException) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private static String tableName(String table) {
        String base = table.substring(table.lastIndexOf('/') + 1);
        return base.endsWith(".lance") ? base.substring(0, base.length() - 6) : base;
    }
}

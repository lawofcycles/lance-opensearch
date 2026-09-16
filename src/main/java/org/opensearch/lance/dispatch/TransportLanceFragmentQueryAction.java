/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.dispatch.LanceMetricAggregator.PartialState;
import org.opensearch.search.SearchHit;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Per-node handler for {@link LanceFragmentQueryAction}. Opens the
 * Lance dataset through the shared {@link LanceRegistry} and runs
 * the requested scan on the node this instance lives on. The
 * coordinator groups fragments by node so each handler only scans
 * its subset, keeping the total work proportional to the fragments
 * a node owns instead of the entire dataset.
 *
 * <p>Two Lance scans happen per invocation, at most:
 * <ul>
 *   <li>The <em>hits</em> scan honours {@link
 *       LanceFragmentQueryRequest#size()} and produces up to
 *       {@code size} {@link SearchHit} instances whose {@code _id}
 *       is synthesised from Lance's {@code _rowaddr} column. The
 *       filter is pushed into
 *       {@link ScanOptions.Builder#filter(String)} so the number of
 *       rows the scan iterates over stays bounded by the filter
 *       selectivity.</li>
 *   <li>When the request carries metric specs, an <em>aggregate</em>
 *       scan runs on the same fragment set under the same filter
 *       and returns per-metric {@link PartialState}. That call
 *       piggy-backs the {@code matched} row count so the coordinator
 *       can populate {@code hits.total.value} without a third
 *       scan.</li>
 * </ul>
 *
 * <p>For requests with no metric specs the handler falls back to
 * per-fragment {@link org.lance.Fragment#countRows()} sums (the
 * metadata fast path Lance offers without a filter) or a
 * {@link Dataset#countRows(String)} on the full dataset when a
 * filter is set — the latter over-counts across all fragments and
 * is corrected by the coordinator using the fragment subset it
 * assigned. Correcting that in the handler is Milestone 5-C4
 * work.
 */
public final class TransportLanceFragmentQueryAction extends HandledTransportAction<LanceFragmentQueryRequest, LanceFragmentQueryResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportLanceFragmentQueryAction.class);

    @Inject
    public TransportLanceFragmentQueryAction(TransportService transportService, ActionFilters actionFilters) {
        super(LanceFragmentQueryAction.NAME, transportService, actionFilters, LanceFragmentQueryRequest::new);
    }

    @Override
    protected void doExecute(Task task, LanceFragmentQueryRequest request, ActionListener<LanceFragmentQueryResponse> listener) {
        try {
            LanceFragmentQueryResponse response = execute(request);
            listener.onResponse(response);
        } catch (Exception e) {
            LOGGER.warn(
                "fragment query failed on this node for [{}] filter [{}] fragments [{}]",
                request.tableUri(),
                request.filterSql() == null ? "<match_all>" : request.filterSql(),
                request.fragmentIds().isEmpty() ? "<all>" : request.fragmentIds(),
                e
            );
            listener.onFailure(e);
        }
    }

    /**
     * Package-private helper that does the actual scan work. Split
     * out so unit tests can call it without going through the
     * transport layer.
     */
    LanceFragmentQueryResponse execute(LanceFragmentQueryRequest request) throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(request.tableUri(), request.storageOptions())) {
            int fragmentCount = request.fragmentIds().isEmpty() ? dataset.getFragments().size() : request.fragmentIds().size();

            List<SearchHit> hits = scanHits(dataset, request.size(), request.filterSql(), request.fragmentIdsOrNull());

            List<PartialState> partials = LanceMetricAggregator.aggregatePartials(
                dataset,
                request.fragmentIdsOrNull(),
                request.filterSql(),
                request.metrics()
            );

            long matched = computeMatched(dataset, request, partials);
            return new LanceFragmentQueryResponse(matched, fragmentCount, hits, partials);
        }
    }

    /**
     * Read up to {@code size} rows from the given fragment subset
     * and synthesise a {@link SearchHit} per row with an {@code _id}
     * of {@code "<fragmentId>-<offsetInFragment>"} and a JSON
     * {@code _source} rendered from the Arrow batch.
     */
    private List<SearchHit> scanHits(Dataset dataset, int size, String filterSql, List<Integer> fragmentIds) throws Exception {
        if (size <= 0) {
            return Collections.emptyList();
        }
        List<SearchHit> out = new ArrayList<>();
        ScanOptions.Builder builder = new ScanOptions.Builder().withRowAddress(true).limit((long) size);
        if (filterSql != null) {
            builder.filter(filterSql);
        }
        if (fragmentIds != null) {
            builder.fragmentIds(fragmentIds);
        }
        ScanOptions options = builder.build();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            int hitIndex = 0;
            while (out.size() < size && reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                int rowCount = root.getRowCount();
                for (int i = 0; i < rowCount && out.size() < size; i++) {
                    long addr = rowAddr.get(i);
                    int fragmentId = (int) (addr >>> 32);
                    int offset = (int) (addr & 0xFFFFFFFFL);
                    String idString = fragmentId + "-" + offset;
                    SearchHit hit = new SearchHit(hitIndex, idString, Collections.emptyMap(), Collections.emptyMap());
                    hit.score(1.0f);
                    byte[] source = LanceRowSourceRenderer.renderJson(root, i);
                    hit.sourceRef(new BytesArray(source));
                    out.add(hit);
                    hitIndex++;
                }
            }
        }
        return out;
    }

    /**
     * Determine the number of rows in this node's fragment subset
     * that satisfy the filter.
     *
     * <p>When at least one metric spec is present the aggregate scan
     * already visited every matching row, so we can reuse its
     * {@link PartialState#count()} — every supported metric type
     * increments {@code count} once per non-null row, so summing
     * counts would over-count. Any single spec's count is enough
     * because they all iterate the same rows.
     *
     * <p>Without metrics we take a shortcut appropriate to the
     * filter:
     * <ul>
     *   <li>No filter: sum {@link org.lance.Fragment#countRows()}
     *       across the assigned fragments (Lance metadata, no
     *       scan).</li>
     *   <li>Filter set, all fragments assigned: use
     *       {@link Dataset#countRows(String)}.</li>
     *   <li>Filter set, subset of fragments: run a bounded scan
     *       over the subset and count matching rows.</li>
     * </ul>
     */
    private long computeMatched(Dataset dataset, LanceFragmentQueryRequest request, List<PartialState> partials) throws Exception {
        if (!partials.isEmpty()) {
            return partials.get(0).count();
        }
        List<Integer> fragmentIds = request.fragmentIdsOrNull();
        String filterSql = request.filterSql();
        if (filterSql == null) {
            if (fragmentIds == null) {
                return dataset.countRows();
            }
            long total = 0L;
            List<org.lance.Fragment> allFragments = dataset.getFragments();
            for (org.lance.Fragment fragment : allFragments) {
                if (fragmentIds.contains(fragment.getId())) {
                    total += fragment.countRows();
                }
            }
            return total;
        }
        if (fragmentIds == null) {
            return dataset.countRows(filterSql);
        }
        // Filter + fragment subset: scan and count. Cheap for
        // typical query workloads because the filter narrows the
        // row set before the scan even starts.
        ScanOptions options = new ScanOptions.Builder().filter(filterSql).fragmentIds(fragmentIds).build();
        long count = 0L;
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                count += reader.getVectorSchemaRoot().getRowCount();
            }
        }
        return count;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.dispatch.LanceMetricAggregator.MetricSpec;

/**
 * Per-node dispatch request. The coordinator groups the target
 * dataset's fragment ids by data node and sends one of these to
 * each node. The receiving node opens the Lance table through the
 * shared registry with the same {@link StorageOptions} the
 * coordinator resolved, scans only the given fragments (or every
 * fragment when {@link #fragmentIds()} is empty as a shorthand for
 * "all"), and honours the filter and metric specs alongside the
 * hits {@code size} allowance.
 *
 * <p>{@link Writeable} so it survives the transport hop.
 */
public final class LanceFragmentQueryRequest extends ActionRequest {

    private final String tableUri;
    private final String indexName;
    private final StorageOptions storageOptions;
    private final String filterSql;
    private final int size;
    private final List<MetricSpec> metrics;
    private final List<Integer> fragmentIds;

    /**
     * @param tableUri absolute URI of the Lance table, resolved by
     *     the coordinator from the index metadata's
     *     {@code lance.table} setting
     * @param indexName concrete OpenSearch index name the search
     *     originally targeted. The receiving node uses this to look
     *     up the {@link org.opensearch.index.IndexService} for
     *     mapper / query-shard context construction on the
     *     direction-1 aggregator path.
     * @param storageOptions credentials / endpoint hints used to
     *     open the table via {@link org.opensearch.lance.LanceRegistry}
     * @param filterSql Lance SQL filter, or {@code null} for
     *     match_all
     * @param size max hits the node should return; the coordinator
     *     may over-fetch across nodes and truncate on merge
     * @param metrics metric specs to compute; empty list = no
     *     aggregations
     * @param fragmentIds fragment ids the node should scan; an
     *     empty list is shorthand for "every fragment on this
     *     dataset" and is used by single-node dispatch or by tests
     */
    public LanceFragmentQueryRequest(
        String tableUri,
        String indexName,
        StorageOptions storageOptions,
        String filterSql,
        int size,
        List<MetricSpec> metrics,
        List<Integer> fragmentIds
    ) {
        this.tableUri = tableUri;
        this.indexName = indexName;
        this.storageOptions = storageOptions;
        this.filterSql = filterSql;
        this.size = size;
        this.metrics = List.copyOf(metrics);
        this.fragmentIds = List.copyOf(fragmentIds);
    }

    public LanceFragmentQueryRequest(StreamInput in) throws IOException {
        super(in);
        this.tableUri = in.readString();
        this.indexName = in.readString();
        this.storageOptions = StorageOptions.readFromStream(in);
        this.filterSql = in.readOptionalString();
        this.size = in.readVInt();
        int metricCount = in.readVInt();
        List<MetricSpec> readMetrics = new ArrayList<>(metricCount);
        for (int i = 0; i < metricCount; i++) {
            readMetrics.add(new MetricSpec(in));
        }
        this.metrics = List.copyOf(readMetrics);
        int fragmentCount = in.readVInt();
        List<Integer> readFragments = new ArrayList<>(fragmentCount);
        for (int i = 0; i < fragmentCount; i++) {
            readFragments.add(in.readVInt());
        }
        this.fragmentIds = List.copyOf(readFragments);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(tableUri);
        out.writeString(indexName);
        storageOptions.writeTo(out);
        out.writeOptionalString(filterSql);
        out.writeVInt(size);
        out.writeVInt(metrics.size());
        for (MetricSpec spec : metrics) {
            spec.writeTo(out);
        }
        out.writeVInt(fragmentIds.size());
        for (Integer id : fragmentIds) {
            out.writeVInt(id);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        // Coordinator resolves and validates everything before
        // sending; a null table or negative size would be a bug
        // rather than user input.
        return null;
    }

    public String tableUri() {
        return tableUri;
    }

    public String indexName() {
        return indexName;
    }

    public StorageOptions storageOptions() {
        return storageOptions;
    }

    public String filterSql() {
        return filterSql;
    }

    public int size() {
        return size;
    }

    public List<MetricSpec> metrics() {
        return metrics;
    }

    /**
     * The fragment ids to scan, or an empty list to signal "all
     * fragments". Never {@code null}; the constructor copies the
     * input list into an immutable one so callers cannot mutate it
     * out from under a handler.
     */
    public List<Integer> fragmentIds() {
        return fragmentIds;
    }

    /**
     * Translate {@link #fragmentIds()} to the value
     * {@link LanceMetricAggregator#aggregatePartials(org.lance.Dataset, List, String, List)}
     * expects: {@code null} for "all fragments", non-null list
     * otherwise. Keeping the wire representation as a non-null empty
     * list makes the transport serialisation branch-free.
     */
    public List<Integer> fragmentIdsOrNull() {
        return fragmentIds.isEmpty() ? null : fragmentIds;
    }

    /** Convenience factory for single-node dispatch (all fragments). */
    public static LanceFragmentQueryRequest allFragments(
        String tableUri,
        String indexName,
        StorageOptions storageOptions,
        String filterSql,
        int size,
        List<MetricSpec> metrics
    ) {
        return new LanceFragmentQueryRequest(
            tableUri,
            indexName,
            storageOptions,
            filterSql,
            size,
            metrics,
            Collections.emptyList()
        );
    }
}

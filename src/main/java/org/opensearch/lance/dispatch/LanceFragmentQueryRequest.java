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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.StorageOptions;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.sort.SortBuilder;

/**
 * Per-node dispatch request. The coordinator groups the target
 * dataset's fragment ids by data node and sends one of these to
 * each node. The receiving node opens the Lance table through the
 * shared registry with the same {@link StorageOptions} the
 * coordinator resolved, scans only the given fragments (or every
 * fragment when {@link #fragmentIds()} is empty as a shorthand for
 * "all"), and honours the filter and aggregations alongside the
 * hits {@code size} allowance.
 *
 * <p>Since Direction 1 Stage 3 the request carries native OpenSearch
 * shapes on the wire:
 * <ul>
 *   <li>{@link #query()} — top-level query builder (nullable).
 *       Translated to a Lucene {@link org.apache.lucene.search.Query}
 *       on the receiving node via
 *       {@link org.opensearch.index.query.QueryShardContext#toQuery}.
 *       When {@code null} the executor uses
 *       {@link org.apache.lucene.search.MatchAllDocsQuery}.</li>
 *   <li>{@link #sorts()} — top-level sort builders (nullable, empty
 *       list also allowed). Passed to
 *       {@link org.apache.lucene.search.IndexSearcher#search(org.apache.lucene.search.Query, int, org.apache.lucene.search.Sort)}
 *       through {@link org.opensearch.search.sort.SortBuilder#buildSort}.</li>
 *   <li>{@link #aggregations()} — top-level aggregator specs
 *       ({@link AggregatorFactories.Builder}), NamedWriteable-compatible.</li>
 * </ul>
 *
 * <p>{@link #filterSql()} is preserved alongside {@link #query()} for
 * Lance metadata-only counting via {@code Dataset.countRows(String)}.
 * When the query is a pure filter shape (term / range / bool / ...)
 * the coordinator sets both fields; when the query needs scoring
 * (match / knn) only {@link #query()} is set. The per-node executor
 * uses {@link #query()} for hits and aggregations, and
 * {@link #filterSql()} for {@code hits.total.value} counting when
 * available (metadata path), falling back to
 * {@link org.apache.lucene.search.IndexSearcher#count} otherwise.
 */
public final class LanceFragmentQueryRequest extends ActionRequest {

    private final String tableUri;
    private final String indexName;
    private final StorageOptions storageOptions;
    private final String filterSql;
    private final QueryBuilder query;
    private final QueryBuilder postFilter;
    private final List<SortBuilder<?>> sorts;
    private final Object[] searchAfter;
    private final int size;
    private final AggregatorFactories.Builder aggregations;
    private final List<Integer> fragmentIds;

    public LanceFragmentQueryRequest(
        String tableUri,
        String indexName,
        StorageOptions storageOptions,
        String filterSql,
        QueryBuilder query,
        QueryBuilder postFilter,
        List<SortBuilder<?>> sorts,
        Object[] searchAfter,
        int size,
        AggregatorFactories.Builder aggregations,
        List<Integer> fragmentIds
    ) {
        this.tableUri = tableUri;
        this.indexName = indexName;
        this.storageOptions = storageOptions;
        this.filterSql = filterSql;
        this.query = query;
        this.postFilter = postFilter;
        this.sorts = sorts == null ? Collections.emptyList() : List.copyOf(sorts);
        this.searchAfter = searchAfter;
        this.size = size;
        this.aggregations = aggregations;
        this.fragmentIds = List.copyOf(fragmentIds);
    }

    public LanceFragmentQueryRequest(StreamInput in) throws IOException {
        super(in);
        this.tableUri = in.readString();
        this.indexName = in.readString();
        this.storageOptions = StorageOptions.readFromStream(in);
        this.filterSql = in.readOptionalString();
        this.query = in.readOptionalNamedWriteable(QueryBuilder.class);
        this.postFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        int sortCount = in.readVInt();
        if (sortCount == 0) {
            this.sorts = Collections.emptyList();
        } else {
            List<SortBuilder<?>> readSorts = new ArrayList<>(sortCount);
            for (int i = 0; i < sortCount; i++) {
                readSorts.add(in.readNamedWriteable(SortBuilder.class));
            }
            this.sorts = List.copyOf(readSorts);
        }
        // search_after values are the sort-field values of the last
        // hit from the previous page. Serialised through
        // writeGenericValue so any doc-value type (long, double,
        // string, boolean) round-trips without a discriminator.
        this.searchAfter = in.readBoolean() ? (Object[]) in.readGenericValue() : null;
        this.size = in.readVInt();
        this.aggregations = in.readBoolean() ? new AggregatorFactories.Builder(in) : null;
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
        out.writeOptionalNamedWriteable(query);
        out.writeOptionalNamedWriteable(postFilter);
        out.writeVInt(sorts.size());
        for (SortBuilder<?> sort : sorts) {
            out.writeNamedWriteable(sort);
        }
        if (searchAfter == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeGenericValue(searchAfter);
        }
        out.writeVInt(size);
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
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

    /**
     * Lance SQL filter for metadata-only counting via
     * {@code Dataset.countRows(String)}. May be {@code null} when
     * the query is not a pure filter shape (e.g. match / knn); in
     * that case the per-node executor falls back to
     * {@link org.apache.lucene.search.IndexSearcher#count} against
     * {@link #query()}.
     */
    public String filterSql() {
        return filterSql;
    }

    /**
     * Top-level query builder, translated to a Lucene Query on the
     * receiving node. May be {@code null} when the request is
     * match_all; the executor then uses
     * {@link org.apache.lucene.search.MatchAllDocsQuery}.
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Post-filter query builder — applied only to the hits after
     * aggregations have been computed over {@link #query()}. May be
     * {@code null} when the request has no post_filter clause.
     */
    public QueryBuilder postFilter() {
        return postFilter;
    }

    /**
     * Top-level sort clauses. Empty list means no sort (score
     * order or unordered).
     */
    public List<SortBuilder<?>> sorts() {
        return sorts;
    }

    /**
     * Cursor for {@code search_after} pagination — the sort-field
     * values of the last hit from the previous page. {@code null}
     * when the request is not paginated with {@code search_after}.
     * When non-null, sorts is guaranteed non-empty (the dispatch
     * filter rejects search_after without a matching sort).
     */
    public Object[] searchAfter() {
        return searchAfter;
    }

    public int size() {
        return size;
    }

    /**
     * Top-level aggregations to run on the per-fragment reader, or
     * {@code null} when the request has no aggregations.
     */
    public AggregatorFactories.Builder aggregations() {
        return aggregations;
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
     * Translate {@link #fragmentIds()} to the value the per-node
     * fragment executor expects: {@code null} for "all fragments",
     * non-null list otherwise. Keeping the wire representation as a
     * non-null empty list makes the transport serialisation
     * branch-free.
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
        QueryBuilder query,
        List<SortBuilder<?>> sorts,
        int size,
        AggregatorFactories.Builder aggregations
    ) {
        return new LanceFragmentQueryRequest(
            tableUri,
            indexName,
            storageOptions,
            filterSql,
            query,
            /* postFilter */ null,
            sorts,
            null,
            size,
            aggregations,
            Collections.emptyList()
        );
    }
}

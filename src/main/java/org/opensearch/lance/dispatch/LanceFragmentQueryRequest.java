/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.WireVersion;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.rescore.RescorerBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.tasks.Task;

/**
 * Per-node dispatch request. The coordinator groups the target
 * dataset's fragment ids by data node and sends one of these to
 * each node. The receiving node opens the Lance table through the
 * shared registry with the same {@link StorageOptions} and pinned
 * manifest version (see {@link #pinnedVersion()}) the coordinator
 * resolved, scans only the given fragments (or every
 * fragment when {@link #fragmentIds()} is empty as a shorthand for
 * "all"), and honours the filter and aggregations alongside the
 * hits {@code size} allowance.
 *
 * <p>The request carries native OpenSearch shapes on the wire:
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
 *   <li>{@link #minScore()}, {@link #terminateAfter()} — the collector
 *       knobs Lucene's {@code MinimumScoreCollector} and an early
 *       terminating collector apply on the executor.</li>
 *   <li>{@link #projection()} — the per hit projections ({@code _source},
 *       {@code stored_fields}, {@code docvalue_fields}, {@code fields},
 *       {@code explain}) the executor's fetch phase renders.</li>
 *   <li>{@link #rescores()}, {@link #collapse()} — the second pass over
 *       the collected page: the {@code rescore} list the executor runs
 *       over its first pass window, and the {@code collapse} whose
 *       collapsing collector replaces the top docs collector.</li>
 * </ul>
 *
 * <p>{@link #plan()} is the per node plan the coordinator's planner
 * chose for this request ({@link FragmentPlan}): the Lance SQL of the
 * scalar predicate, the full text or knn clause the executor builds the
 * Lance query from, and the page or aggregate the Lance scan computes
 * when the planner pushed one. The executor runs the plan after its
 * node local guards; the builders above stay on the wire because the
 * Lucene side (aggregator construction, collectors, the fetch phase)
 * needs them whatever the plan says. The wire format is internal to the
 * plugin and opens with {@link #WIRE_VERSION} (see {@link WireVersion}),
 * whose block framing lets an executor of the previous plugin version
 * read the request during a rolling upgrade; the plan inside carries a
 * marker of its own.
 */
public final class LanceFragmentQueryRequest extends ActionRequest {

    /** The wire format's version, the first field the request writes after its base class. */
    public static final int WIRE_VERSION = 1;

    private final String tableUri;
    private final String indexName;
    private final StorageOptions storageOptions;
    /**
     * Lance manifest version the coordinator enumerated the fragments
     * of this request from: the version {@code index.lance.version}
     * or the index's tag pins, or the latest version the coordinator
     * observed when it opened a table that follows the manifest. The
     * coordinator resolves it once per request and ships it on the
     * wire so every executor reads the same manifest the coordinator
     * assigned fragments from and keys its {@code LanceWarmCache}
     * snapshot on it without asking Lance for the latest version;
     * a pinned index thereby serves the same rows through
     * {@code _search} as through {@code _stats} and GET. {@code -1}
     * means no version was resolved and the executor falls back to
     * the table's latest version.
     */
    private final long pinnedVersion;
    private final FragmentPlan plan;
    private final QueryBuilder query;
    private final QueryBuilder postFilter;
    private final List<SortBuilder<?>> sorts;
    private final Object[] searchAfter;
    private final int size;
    private final AggregatorFactories.Builder aggregations;
    private final List<Integer> fragmentIds;
    /**
     * Whether the caller opted into per-hit score collection via
     * {@code track_scores}. Sort-only queries default to
     * {@code false} in Lucene, so the executor has to opt in
     * explicitly to keep {@code _score} populated when the user
     * asks for it alongside a {@code sort} clause.
     */
    private final boolean trackScores;
    /**
     * How far the executor has to count matches for
     * {@code hits.total}, in the encoding
     * {@link org.opensearch.search.builder.SearchSourceBuilder#trackTotalHitsUpTo()}
     * uses: a positive bound (the request's {@code track_total_hits}
     * integer, or {@link SearchContext#DEFAULT_TRACK_TOTAL_HITS_UP_TO}
     * when the request left it out),
     * {@link SearchContext#TRACK_TOTAL_HITS_ACCURATE} for
     * {@code track_total_hits: true} (also what {@code _count}
     * sends), or {@link SearchContext#TRACK_TOTAL_HITS_DISABLED} for
     * {@code track_total_hits: false}. The executor may stop counting
     * once it has seen more than the bound and report the count as a
     * lower bound through
     * {@link LanceFragmentQueryResponse#matchedIsLowerBound()}.
     */
    private final int trackTotalHitsUpTo;
    /**
     * The request's {@code min_score}, or {@code null} when it has none.
     * The executor wraps its hits and aggregation collectors in the
     * stock {@code MinimumScoreCollector}, so a document below the
     * threshold is neither returned, aggregated nor counted.
     */
    private final Float minScore;
    /**
     * The request's {@code terminate_after}, or 0 when it has none. The
     * executor stops collecting after that many documents and reports
     * {@link LanceFragmentQueryResponse#terminatedEarly()}.
     */
    private final int terminateAfter;
    /** The per hit projections of the body; {@link HitProjection#NONE} when it has none. */
    private final HitProjection projection;
    /**
     * The request's {@code rescore} list, empty when it has none. Each
     * entry is the stock {@link RescorerBuilder}; the executor builds its
     * {@link org.opensearch.search.rescore.RescoreContext} against the
     * node's mapping and re scores the top {@code window_size} hits of
     * its first pass with it, in list order.
     */
    private final List<RescorerBuilder<?>> rescores;
    /**
     * The request's {@code collapse}, or {@code null} when it has none.
     * The executor builds the {@link org.opensearch.search.collapse.CollapseContext}
     * against the node's mapping and collects one hit per distinct value
     * of the field; the coordinator reads the field name to keep one hit
     * per value across the executors and to expand {@code inner_hits}.
     */
    private final CollapseBuilder collapse;

    /**
     * A request without collector knobs, per hit projections, rescorers
     * or collapse: the full constructor with {@code minScore} null,
     * {@code terminateAfter} 0, {@link HitProjection#NONE}, no rescorers
     * and no collapse.
     */
    public LanceFragmentQueryRequest(
        String tableUri,
        String indexName,
        StorageOptions storageOptions,
        long pinnedVersion,
        FragmentPlan plan,
        QueryBuilder query,
        QueryBuilder postFilter,
        List<SortBuilder<?>> sorts,
        Object[] searchAfter,
        int size,
        AggregatorFactories.Builder aggregations,
        List<Integer> fragmentIds,
        boolean trackScores,
        int trackTotalHitsUpTo
    ) {
        this(
            tableUri,
            indexName,
            storageOptions,
            pinnedVersion,
            plan,
            query,
            postFilter,
            sorts,
            searchAfter,
            size,
            aggregations,
            fragmentIds,
            trackScores,
            trackTotalHitsUpTo,
            null,
            0,
            HitProjection.NONE,
            Collections.emptyList(),
            null
        );
    }

    /** A request without rescorers and without collapse. */
    public LanceFragmentQueryRequest(
        String tableUri,
        String indexName,
        StorageOptions storageOptions,
        long pinnedVersion,
        FragmentPlan plan,
        QueryBuilder query,
        QueryBuilder postFilter,
        List<SortBuilder<?>> sorts,
        Object[] searchAfter,
        int size,
        AggregatorFactories.Builder aggregations,
        List<Integer> fragmentIds,
        boolean trackScores,
        int trackTotalHitsUpTo,
        Float minScore,
        int terminateAfter,
        HitProjection projection
    ) {
        this(
            tableUri,
            indexName,
            storageOptions,
            pinnedVersion,
            plan,
            query,
            postFilter,
            sorts,
            searchAfter,
            size,
            aggregations,
            fragmentIds,
            trackScores,
            trackTotalHitsUpTo,
            minScore,
            terminateAfter,
            projection,
            Collections.emptyList(),
            null
        );
    }

    public LanceFragmentQueryRequest(
        String tableUri,
        String indexName,
        StorageOptions storageOptions,
        long pinnedVersion,
        FragmentPlan plan,
        QueryBuilder query,
        QueryBuilder postFilter,
        List<SortBuilder<?>> sorts,
        Object[] searchAfter,
        int size,
        AggregatorFactories.Builder aggregations,
        List<Integer> fragmentIds,
        boolean trackScores,
        int trackTotalHitsUpTo,
        Float minScore,
        int terminateAfter,
        HitProjection projection,
        List<RescorerBuilder<?>> rescores,
        CollapseBuilder collapse
    ) {
        this.tableUri = tableUri;
        this.indexName = indexName;
        this.storageOptions = storageOptions;
        this.pinnedVersion = pinnedVersion;
        this.plan = Objects.requireNonNull(plan, "plan");
        this.query = query;
        this.postFilter = postFilter;
        this.sorts = sorts == null ? Collections.emptyList() : List.copyOf(sorts);
        this.searchAfter = searchAfter;
        this.size = size;
        this.aggregations = aggregations;
        this.fragmentIds = List.copyOf(fragmentIds);
        this.trackScores = trackScores;
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        this.minScore = minScore;
        this.terminateAfter = Math.max(0, terminateAfter);
        this.projection = projection == null ? HitProjection.NONE : projection;
        this.rescores = rescores == null ? Collections.emptyList() : List.copyOf(rescores);
        this.collapse = collapse;
    }

    public LanceFragmentQueryRequest(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceFragmentQueryRequest", WIRE_VERSION);
        this.tableUri = in.readString();
        this.indexName = in.readString();
        this.storageOptions = StorageOptions.readFromStream(in);
        this.pinnedVersion = in.readLong();
        this.plan = new FragmentPlan(in);
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
        this.trackScores = in.readBoolean();
        this.trackTotalHitsUpTo = in.readInt();
        this.minScore = in.readOptionalFloat();
        this.terminateAfter = in.readVInt();
        this.projection = HitProjection.read(in);
        int rescoreCount = in.readVInt();
        if (rescoreCount == 0) {
            this.rescores = Collections.emptyList();
        } else {
            List<RescorerBuilder<?>> readRescores = new ArrayList<>(rescoreCount);
            for (int i = 0; i < rescoreCount; i++) {
                readRescores.add(in.readNamedWriteable(RescorerBuilder.class));
            }
            this.rescores = List.copyOf(readRescores);
        }
        this.collapse = in.readOptionalWriteable(CollapseBuilder::new);
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        out.writeString(tableUri);
        out.writeString(indexName);
        storageOptions.writeTo(out);
        out.writeLong(pinnedVersion);
        plan.writeTo(out);
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
        out.writeBoolean(trackScores);
        // Plain int rather than VInt: TRACK_TOTAL_HITS_DISABLED is -1
        // and TRACK_TOTAL_HITS_ACCURATE is Integer.MAX_VALUE, neither
        // of which VInt encodes compactly or (for -1) safely.
        out.writeInt(trackTotalHitsUpTo);
        out.writeOptionalFloat(minScore);
        out.writeVInt(terminateAfter);
        projection.writeTo(out);
        out.writeVInt(rescores.size());
        for (RescorerBuilder<?> rescore : rescores) {
            out.writeNamedWriteable(rescore);
        }
        out.writeOptionalWriteable(collapse);
    }

    @Override
    public ActionRequestValidationException validate() {
        // Coordinator resolves and validates everything before
        // sending; a null table or negative size would be a bug
        // rather than user input.
        return null;
    }

    /**
     * The executor's task is cancellable (see {@link LanceFragmentQueryTask})
     * so the coordinator can stop a timed out executor and a cancelled
     * coordinator task takes its executors down with it.
     */
    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        String fragments = fragmentIds.isEmpty() ? "all fragments" : fragmentIds.size() + " fragments";
        return new LanceFragmentQueryTask(
            id,
            type,
            action,
            "lance fragment query on [" + indexName + "], " + fragments,
            parentTaskId,
            headers
        );
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
     * Manifest version the executor reads, or {@code -1} when none
     * was resolved. Same encoding as the {@code index.lance.version}
     * index setting.
     */
    public long pinnedVersion() {
        return pinnedVersion;
    }

    /**
     * {@link #pinnedVersion()} in the shape
     * {@link org.opensearch.lance.LanceRegistry#openDataset(String, StorageOptions, Optional)}
     * takes: empty when no version was resolved.
     */
    public Optional<Long> pinnedVersionOrEmpty() {
        return pinnedVersion >= 0 ? Optional.of(pinnedVersion) : Optional.empty();
    }

    /**
     * The per node plan the coordinator chose for this request. Never
     * {@code null}: a request the planner could not spell carries a
     * Lucene plan whose kind names the envelope and whose query parts
     * are empty.
     */
    public FragmentPlan plan() {
        return plan;
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
     * values of the last hit from the previous page, as the client sent
     * them (JSON numbers arrive as Integer / Long / Double). {@code null}
     * when the request is not paginated with {@code search_after}. The
     * executor types them against its Lucene sort through
     * {@code SearchAfterBuilder.buildFieldDoc}, the way the stock search path
     * does, and refuses a cursor whose request builds no sort
     * ({@code sort} absent, or a lone descending {@code _score}) with
     * the stock search path's message.
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

    /**
     * Whether the caller opted into {@code track_scores}. When
     * {@code true} the per-node executor asks Lucene to keep
     * per-hit scores even when {@code sort} is present, so hits
     * come back with numeric {@code _score} values instead of
     * NaN. Ignored when {@code sort} is empty because plain
     * {@link org.apache.lucene.search.IndexSearcher#search(org.apache.lucene.search.Query, int)}
     * already collects scores.
     */
    public boolean trackScores() {
        return trackScores;
    }

    /**
     * Bound on the match count the executor has to establish for
     * {@code hits.total}: a positive limit,
     * {@link SearchContext#TRACK_TOTAL_HITS_ACCURATE}, or
     * {@link SearchContext#TRACK_TOTAL_HITS_DISABLED}. See the field
     * comment for the encoding.
     */
    public int trackTotalHitsUpTo() {
        return trackTotalHitsUpTo;
    }

    /** The request's {@code min_score}, or {@code null} when it has none. */
    public Float minScore() {
        return minScore;
    }

    /** The request's {@code terminate_after}, or 0 when it has none. */
    public int terminateAfter() {
        return terminateAfter;
    }

    /**
     * Whether the request carries {@code min_score} or
     * {@code terminate_after}. Both apply inside Lucene's collectors, so
     * the executor then collects the hits, the aggregations and the
     * match count through those collectors and runs no Lance side count
     * or bounded scan for the request.
     */
    public boolean hasCollectorKnobs() {
        return minScore != null || terminateAfter > 0;
    }

    /** The per hit projections of the body, never {@code null}. */
    public HitProjection projection() {
        return projection;
    }

    /** The request's {@code rescore} list in body order, empty when it has none. */
    public List<RescorerBuilder<?>> rescores() {
        return rescores;
    }

    /** The request's {@code collapse}, or {@code null} when it has none. */
    public CollapseBuilder collapse() {
        return collapse;
    }

    /**
     * The rows the executor's first pass collects before any rescorer
     * runs: {@link #size()}, raised to the largest {@code window_size} of
     * the rescorers the way {@code TopDocsCollectorContext} sizes the top
     * docs collector when a rescore is present. Without rescorers this
     * is {@link #size()}. A rescorer without an explicit window uses
     * {@link RescorerBuilder#DEFAULT_WINDOW_SIZE}, as its context will.
     */
    public int firstPassSize() {
        int firstPass = size;
        for (RescorerBuilder<?> rescore : rescores) {
            Integer window = rescore.windowSize();
            firstPass = Math.max(firstPass, window == null ? RescorerBuilder.DEFAULT_WINDOW_SIZE : window);
        }
        return firstPass;
    }

    /**
     * Convenience factory for single-node dispatch (all fragments,
     * latest manifest version, exact match count).
     *
     * <p>{@code trackScores} defaults to {@code false} because
     * every caller of this helper today either has no {@code sort}
     * clause (score-only queries collect scores automatically) or
     * runs in tests where the flag is not exercised. Callers that
     * need {@code track_scores:true} alongside a sort, a pinned
     * manifest version, or a {@code track_total_hits} bound should
     * use the full constructor instead.
     */
    public static LanceFragmentQueryRequest allFragments(
        String tableUri,
        String indexName,
        StorageOptions storageOptions,
        FragmentPlan plan,
        QueryBuilder query,
        List<SortBuilder<?>> sorts,
        int size,
        AggregatorFactories.Builder aggregations
    ) {
        return new LanceFragmentQueryRequest(
            tableUri,
            indexName,
            storageOptions,
            /* pinnedVersion */ -1L,
            plan,
            query,
            /* postFilter */ null,
            sorts,
            null,
            size,
            aggregations,
            Collections.emptyList(),
            /* trackScores */ false,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
    }
}

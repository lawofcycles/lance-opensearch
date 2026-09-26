/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.lance.WireVersion;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.InternalAggregations;

/**
 * Per-node dispatch response. Carries the partial numbers the
 * coordinator needs to merge into a shard-shape-free
 * {@link org.opensearch.action.search.SearchResponse}:
 * <ul>
 *   <li>{@link #matched()} — number of rows in the node's fragment
 *       subset that satisfied the query, counted up to the request's
 *       {@link LanceFragmentQueryRequest#trackTotalHitsUpTo()} bound.
 *       {@link #matchedIsLowerBound()} says whether the executor
 *       stopped counting at the bound, in which case the true count
 *       is at least {@code matched}. The coordinator sums the values
 *       across all nodes to populate {@code hits.total.value} and
 *       turns the relation into {@code gte} when any node reported a
 *       lower bound or the sum exceeds the bound.</li>
 *   <li>{@link #fragmentCount()} — number of fragments the node
 *       scanned. Coordinator sums these to populate
 *       {@code _shards.total} on the merged response so operators
 *       can see the fragment fan-out.</li>
 *   <li>{@link #hits()} — top-{@code size} hits from the node's
 *       fragment subset. Coordinator concatenates all node lists
 *       and truncates to the request's {@code size}.</li>
 *   <li>{@link #rowAddrs()} — the Lance row address
 *       ({@code fragmentId << 32 | offset}) of each hit, parallel to
 *       {@link #hits()}. The coordinator breaks ties between hits
 *       with equal sort values on it, ascending, which is the order
 *       Lucene's collectors produce on a single reader over the whole
 *       table (doc id order is fragment order then offset). The
 *       address is internal to the fan-out and never rendered into
 *       the response.</li>
 *   <li>{@link #aggregations()} — per-node
 *       {@link InternalAggregations} produced by driving OpenSearch's
 *       stock aggregator machinery against a per-fragment
 *       {@link org.apache.lucene.index.DirectoryReader}
 *       (see {@link TransportLanceFragmentQueryAction}). Every
 *       {@link org.opensearch.search.aggregations.InternalAggregation}
 *       inside is already Writeable, so the transport layer
 *       serialises the whole tree in one call. Coordinator merges
 *       them with
 *       {@link InternalAggregations#topLevelReduce(java.util.List,
 *       org.opensearch.search.aggregations.InternalAggregation.ReduceContext)}
 *       — the same reduction path the shard fan-out uses. May be
 *       {@code null} for hits-only requests where no aggregations
 *       were requested.</li>
 *   <li>{@link #profile()} — how long the executor spent on the query
 *       phase and on the fetch phase, and the take scans the request
 *       issued ({@link Profile}). The coordinator renders them per
 *       node under {@code profile.lance} when the request carried
 *       {@code profile: true}.</li>
 * </ul>
 *
 * <p>{@link SearchHit} is already {@link org.opensearch.core.common.io.stream.Writeable
 * Writeable} — the transport layer serialises the whole list in
 * one call and the coordinator rebuilds hits without any per-hit
 * conversion.
 *
 * <p>The response opens with {@link #WIRE_VERSION} (see
 * {@link WireVersion}), whose block framing lets a coordinator of the
 * previous plugin version read it during a rolling upgrade. Version 1
 * laid out every field but the profile, which version 2 added as an
 * optional block: an older coordinator steps over it and shows no
 * timings for the node. Version 3 added the columns the take scans
 * projected as a second optional block, read as zero by a coordinator
 * of version 2.
 */
public final class LanceFragmentQueryResponse extends ActionResponse {

    /** The wire format's version, the first field the response writes; 2 added the profile block, 3 the take columns block. */
    public static final int WIRE_VERSION = 3;

    /**
     * What one executor spent on a request: the query phase (from the
     * request reaching the executor to the page being collected, the
     * count and the aggregations included) and the fetch phase (the
     * rows behind the hits materialised) in wall milliseconds, and the
     * {@code _rowaddr IN (...)} take scans the request issued on the
     * executor, how many row addresses they carried, how many columns
     * they projected summed over the scans, and their wall time summed,
     * whichever phase issued them. The first five figures travel in the
     * version 2 block and the columns in the version 3 block, so a
     * response from an executor of an older plugin version reads as
     * {@link #NONE} or with zero columns.
     */
    public record Profile(long queryMillis, long fetchMillis, long takeCount, long takeRows, long takeMillis, long takeColumns)
        implements
            Writeable {

        /** What an executor that does not report timings stands for: every figure zero. */
        public static final Profile NONE = new Profile(0L, 0L, 0L, 0L, 0L, 0L);

        /** The version 2 block: the five figures before the take columns existed; {@code takeColumns} stays zero. */
        public Profile(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), 0L);
        }

        /** The version 2 block: the five figures before the take columns existed. */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(queryMillis);
            out.writeVLong(fetchMillis);
            out.writeVLong(takeCount);
            out.writeVLong(takeRows);
            out.writeVLong(takeMillis);
        }

        /** This profile with {@code takeColumns} in place of its own, for the version 3 block read after the version 2 one. */
        public Profile withTakeColumns(long takeColumns) {
            return new Profile(queryMillis, fetchMillis, takeCount, takeRows, takeMillis, takeColumns);
        }

        /** The figures of this and {@code other} added, for the responses one node returned to one request. */
        public Profile plus(Profile other) {
            return new Profile(
                queryMillis + other.queryMillis,
                fetchMillis + other.fetchMillis,
                takeCount + other.takeCount,
                takeRows + other.takeRows,
                takeMillis + other.takeMillis,
                takeColumns + other.takeColumns
            );
        }
    }

    private final long matched;
    private final boolean matchedIsLowerBound;
    private final int fragmentCount;
    private final List<SearchHit> hits;
    private final long[] rowAddrs;
    private final InternalAggregations aggregations;
    /**
     * Whether the executor stopped collecting at the request's
     * {@code terminate_after}: {@code null} when the request carried
     * none, otherwise the flag the stock search path reports per shard as
     * {@code terminated_early}. The coordinator ORs the flags of every
     * executor into the response.
     */
    private final Boolean terminatedEarly;
    /** The executor's timings and take counts; {@link Profile#NONE} from an executor that reports none. */
    private final Profile profile;

    /** A response of a request without {@code terminate_after}: {@link #terminatedEarly()} is {@code null}. */
    public LanceFragmentQueryResponse(
        long matched,
        boolean matchedIsLowerBound,
        int fragmentCount,
        List<SearchHit> hits,
        long[] rowAddrs,
        InternalAggregations aggregations
    ) {
        this(matched, matchedIsLowerBound, fragmentCount, hits, rowAddrs, aggregations, null);
    }

    /** A response without timings: {@link #profile()} is {@link Profile#NONE}. */
    public LanceFragmentQueryResponse(
        long matched,
        boolean matchedIsLowerBound,
        int fragmentCount,
        List<SearchHit> hits,
        long[] rowAddrs,
        InternalAggregations aggregations,
        Boolean terminatedEarly
    ) {
        this(matched, matchedIsLowerBound, fragmentCount, hits, rowAddrs, aggregations, terminatedEarly, Profile.NONE);
    }

    public LanceFragmentQueryResponse(
        long matched,
        boolean matchedIsLowerBound,
        int fragmentCount,
        List<SearchHit> hits,
        long[] rowAddrs,
        InternalAggregations aggregations,
        Boolean terminatedEarly,
        Profile profile
    ) {
        if (rowAddrs.length != hits.size()) {
            throw new IllegalArgumentException("rowAddrs has " + rowAddrs.length + " entries for " + hits.size() + " hits");
        }
        this.matched = matched;
        this.matchedIsLowerBound = matchedIsLowerBound;
        this.fragmentCount = fragmentCount;
        this.hits = List.copyOf(hits);
        this.rowAddrs = rowAddrs.clone();
        this.aggregations = aggregations;
        this.terminatedEarly = terminatedEarly;
        this.profile = profile == null ? Profile.NONE : profile;
    }

    public LanceFragmentQueryResponse(StreamInput in) throws IOException {
        this(in, WIRE_VERSION);
    }

    /**
     * Reads the response as a coordinator whose plugin is at wire
     * version {@code asVersion} would: the blocks of later versions are
     * stepped over as {@link WireVersion.Reader} describes. The
     * transport reads with {@link #WIRE_VERSION}; the mixed version
     * tests read with the versions before it.
     */
    static LanceFragmentQueryResponse read(StreamInput in, int asVersion) throws IOException {
        return new LanceFragmentQueryResponse(in, asVersion);
    }

    private LanceFragmentQueryResponse(StreamInput in, int asVersion) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceFragmentQueryResponse", asVersion);
        this.matched = in.readVLong();
        this.matchedIsLowerBound = in.readBoolean();
        this.fragmentCount = in.readVInt();
        int hitCount = in.readVInt();
        List<SearchHit> readHits = new ArrayList<>(hitCount);
        for (int i = 0; i < hitCount; i++) {
            readHits.add(new SearchHit(in));
        }
        this.hits = List.copyOf(readHits);
        this.rowAddrs = in.readLongArray();
        if (rowAddrs.length != hitCount) {
            throw new IOException("rowAddrs has " + rowAddrs.length + " entries for " + hitCount + " hits");
        }
        this.aggregations = in.readBoolean() ? InternalAggregations.readFrom(in) : null;
        this.terminatedEarly = in.readOptionalBoolean();
        Profile timings = reader.block(2, Profile::new, Profile.NONE);
        this.profile = timings.withTakeColumns(reader.block(3, StreamInput::readVLong, 0L));
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        WireVersion.write(out, WIRE_VERSION);
        out.writeVLong(matched);
        out.writeBoolean(matchedIsLowerBound);
        out.writeVInt(fragmentCount);
        out.writeVInt(hits.size());
        for (SearchHit hit : hits) {
            hit.writeTo(out);
        }
        out.writeLongArray(rowAddrs);
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
        }
        out.writeOptionalBoolean(terminatedEarly);
        // An older coordinator that steps over the timings still merges
        // the hits and aggregations correctly, so both blocks are
        // optional: the version 2 timings, then the version 3 columns.
        WireVersion.writeBlock(out, false, profile);
        WireVersion.writeBlock(out, false, columns -> columns.writeVLong(profile.takeColumns()));
    }

    public long matched() {
        return matched;
    }

    /**
     * Whether {@link #matched()} is a lower bound rather than the
     * exact count: the executor stopped counting once it had seen more
     * than the request's {@code trackTotalHitsUpTo} matches.
     */
    public boolean matchedIsLowerBound() {
        return matchedIsLowerBound;
    }

    public int fragmentCount() {
        return fragmentCount;
    }

    public List<SearchHit> hits() {
        return hits;
    }

    /**
     * Lance row address of each hit, {@code fragmentId << 32 | offset},
     * in the same order as {@link #hits()}. Ascending row address is
     * the doc id order of a single reader over the whole table, so the
     * coordinator uses it to break ties the way one executor would.
     */
    public long[] rowAddrs() {
        return rowAddrs;
    }

    /**
     * Per-node aggregation results. May be {@code null} when the
     * request carried no aggregations. Never returns an empty
     * {@link InternalAggregations} — a request with aggregations
     * always produces at least one {@link
     * org.opensearch.search.aggregations.InternalAggregation}.
     */
    public InternalAggregations aggregations() {
        return aggregations;
    }

    /**
     * Whether collection stopped at the request's {@code terminate_after}
     * on this executor; {@code null} when the request carried none.
     */
    public Boolean terminatedEarly() {
        return terminatedEarly;
    }

    /**
     * The executor's query and fetch phase timings and the take scans
     * of the request; {@link Profile#NONE} from an executor of a plugin
     * version that does not report them.
     */
    public Profile profile() {
        return profile;
    }
}

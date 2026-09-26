/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

import org.opensearch.lance.stats.LanceNodeStats;

/**
 * Node wide counters of the {@code _rowaddr IN (...)} take scans the
 * fragment path issues per request: the rows behind the hits of a page
 * ({@link LanceStoredFields#prefetchRows}, one scan per
 * {@link LanceStoredFields#TAKE_CHUNK} doc ids) and the sort or
 * aggregation column of a small full text or vector hit set
 * ({@code LanceColumnLoader.takeHintedRows}, one scan per chunk and
 * column). A row the node's {@link LanceFetchCache} holds is not taken,
 * so the counters say how many scans the requests still paid for, how
 * many rows and columns they asked for and how long they took, and the
 * share of a request the takes account for can be read off
 * {@code GET /_lance/stats} ({@code fetch}) next to the request's
 * {@code took}.
 *
 * <p>Static for the same reason as {@link HeapFallbackStats}: the
 * readers that issue the takes are opened from the fragment executor,
 * the shard engine and tests alike, and share no object the plugin
 * could hand a counter through. Each take adds to four {@link LongAdder}s
 * and one {@link LongAccumulator} around two {@code System.nanoTime()}
 * reads; a request issues a handful of takes, so the cost is well below
 * the JNI call each take already makes.
 *
 * <p>The node counters mix every request that runs on the node, so the
 * search profile of one request reads its takes from an
 * {@link Accumulator} of its own instead: the fragment executor makes
 * one per request and hands it to the leaves it opens
 * ({@link LanceFragmentLeafReader#setTakeAccumulator}), and every take
 * of those leaves adds to it next to the node counters.
 */
public final class FetchTakeStats {

    /**
     * The take scans of one request: how many ran, how many row
     * addresses they carried, how many columns they projected summed
     * over the scans, and their wall time summed. The slices of a
     * request take on several threads at once, so the fields are
     * {@link LongAdder}s.
     */
    public static final class Accumulator {
        private final LongAdder count = new LongAdder();
        private final LongAdder rows = new LongAdder();
        private final LongAdder columns = new LongAdder();
        private final LongAdder nanos = new LongAdder();

        void record(int rowCount, int columnCount, long elapsedNanos) {
            count.increment();
            rows.add(rowCount);
            columns.add(columnCount);
            nanos.add(elapsedNanos);
        }

        public long takeCount() {
            return count.sum();
        }

        public long takeRows() {
            return rows.sum();
        }

        /** Columns the scans projected, summed over the scans; per scan this is the width of one take. */
        public long takeColumns() {
            return columns.sum();
        }

        public long takeMillis() {
            return nanos.sum() / 1_000_000L;
        }
    }

    /** Which caller issued a take; each has its own scan counter under {@code fetch} in the stats. */
    public enum Kind {
        /** The rows behind the hits of a page, for {@code _id} and {@code _source}. */
        STORED_FIELDS,
        /** One sort or aggregation column for a hinted hit set. */
        COLUMN
    }

    private static final LongAdder COUNT = new LongAdder();
    private static final LongAdder ROWS = new LongAdder();
    private static final LongAdder COLUMNS = new LongAdder();
    private static final LongAdder NANOS = new LongAdder();
    private static final LongAccumulator MAX_NANOS = new LongAccumulator(Math::max, 0L);
    private static final LongAdder STORED_FIELDS_TAKES = new LongAdder();
    private static final LongAdder COLUMN_TAKES = new LongAdder();

    private FetchTakeStats() {}

    /**
     * Records one take scan of {@code kind} that addressed {@code rows}
     * row addresses, projected {@code columns} columns and took
     * {@code nanos} of wall time from the scan's creation to its close,
     * whether or not it completed, in the node counters and in
     * {@code perRequest} when the leaf that issued the take serves a
     * profiled request (null otherwise).
     */
    static void record(Kind kind, int rows, int columns, long nanos, Accumulator perRequest) {
        if (perRequest != null) {
            perRequest.record(rows, columns, nanos);
        }
        COUNT.increment();
        ROWS.add(rows);
        COLUMNS.add(columns);
        NANOS.add(nanos);
        MAX_NANOS.accumulate(nanos);
        switch (kind) {
            case STORED_FIELDS -> STORED_FIELDS_TAKES.increment();
            case COLUMN -> COLUMN_TAKES.increment();
        }
    }

    /** The counters since the node started, in the shape {@code GET /_lance/stats} reports. */
    public static LanceNodeStats.FetchStats snapshot() {
        return new LanceNodeStats.FetchStats(
            COUNT.sum(),
            ROWS.sum(),
            COLUMNS.sum(),
            NANOS.sum() / 1_000_000L,
            MAX_NANOS.get() / 1_000_000L,
            STORED_FIELDS_TAKES.sum(),
            COLUMN_TAKES.sum()
        );
    }
}

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
 * column). Nothing the plugin caches serves these rows, so every request
 * pays for its takes; the counters say how many scans ran, how many
 * rows and columns they asked for and how long they took, so the share
 * of a request the takes account for can be read off
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
 */
public final class FetchTakeStats {

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
     * whether or not it completed.
     */
    static void record(Kind kind, int rows, int columns, long nanos) {
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

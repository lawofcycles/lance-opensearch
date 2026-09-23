/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.search.IndexSearcher;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;

/**
 * Request scoped byte accounting of the hit buffers the Lance Weights
 * keep on heap, against OpenSearch's {@code request} circuit breaker.
 *
 * <p>{@link LanceFtsQuery} and {@link LanceKnnQuery} read their shard
 * level Lance scan into one {@link LanceFragmentHits} per fragment and
 * hold those buffers for the life of the Weight. An unbounded full text
 * scan (aggregations, sort by a field, {@code size 0}) holds one entry
 * per matching row, so on a large table the buffers are the dominant
 * heap cost of the request and, with nothing counting them, the path by
 * which a large enough hit set ends the JVM with an
 * {@code OutOfMemoryError}. The Weights reserve the bytes here before
 * they allocate them ({@link #reserve}), which turns that failure into
 * a {@link CircuitBreakingException} the executor answers with HTTP
 * 429, and give them back when the buffers are dropped
 * ({@link #release}) or when the request ends ({@link #close}).
 *
 * <p>One instance belongs to one searcher and therefore one request;
 * every Weight created against that searcher shares it, so the bytes
 * of every Lance scan of the request are counted together and are
 * returned together when the executor closes its search context. A
 * searcher exposes its instance through {@link Provider}; a Weight
 * created against any other searcher (unit tests, the shard path)
 * counts against a {@link NoopCircuitBreaker}, which keeps the code
 * path identical but never trips.
 */
public final class LanceHitsAccounting implements Releasable {

    /** Label the breaker reports the reservation under. */
    public static final String LABEL = "lance_fts_hits";

    /**
     * Implemented by an {@link IndexSearcher} that carries request
     * scoped accounting, so {@code Query.createWeight(searcher, ...)}
     * can pick it up.
     */
    public interface Provider {
        LanceHitsAccounting hitsAccounting();
    }

    private final CircuitBreaker breaker;
    private final AtomicLong reserved = new AtomicLong();
    /**
     * Whether the admission gate counted this request in flight (one
     * of its gated paths was admitted with a non zero estimate); set by
     * {@link #markAdmitted}, cleared by {@link #clearAdmitted} when the
     * request ends, so a request counts once however many paths it runs.
     */
    private final AtomicBoolean admitted = new AtomicBoolean();

    public LanceHitsAccounting(CircuitBreaker breaker) {
        this.breaker = Objects.requireNonNull(breaker, "breaker must not be null");
    }

    /** Accounting that counts but never trips, for searchers without a breaker. */
    public static LanceHitsAccounting unlimited() {
        return new LanceHitsAccounting(new NoopCircuitBreaker(CircuitBreaker.REQUEST));
    }

    /**
     * The accounting of {@code searcher} when it is a {@link Provider},
     * otherwise a fresh {@link #unlimited()} instance.
     */
    public static LanceHitsAccounting of(IndexSearcher searcher) {
        if (searcher instanceof Provider provider) {
            LanceHitsAccounting accounting = provider.hitsAccounting();
            if (accounting != null) {
                return accounting;
            }
        }
        return unlimited();
    }

    /**
     * Reserve {@code bytes} with the breaker before allocating them.
     * Throws {@link CircuitBreakingException} and reserves nothing when
     * the breaker refuses; the caller lets the exception propagate.
     */
    public void reserve(long bytes) {
        if (bytes <= 0L) {
            return;
        }
        breaker.addEstimateBytesAndMaybeBreak(bytes, LABEL);
        reserved.addAndGet(bytes);
    }

    /**
     * Give {@code bytes} back to the breaker after the buffers they
     * were reserved for have been dropped.
     */
    public void release(long bytes) {
        if (bytes <= 0L) {
            return;
        }
        breaker.addWithoutBreaking(-bytes);
        reserved.addAndGet(-bytes);
    }

    /** Bytes currently reserved through this instance and not yet released. */
    public long reservedBytes() {
        return reserved.get();
    }

    /**
     * The room left in the request breaker for the heap terms the
     * admission gate judges: its limit minus what it holds, or
     * {@link Long#MAX_VALUE} for a breaker without a limit (the
     * {@link NoopCircuitBreaker} of a searcher outside the fragment
     * path).
     */
    public long breakerRoomBytes() {
        long limit = breaker.getLimit();
        if (limit <= 0L) {
            return Long.MAX_VALUE;
        }
        return Math.max(0L, limit - breaker.getUsed());
    }

    /** Mark this request as counted in flight by the admission gate; {@code true} the first time. */
    boolean markAdmitted() {
        return admitted.compareAndSet(false, true);
    }

    /** Clear the in flight mark; {@code true} when it was set. */
    boolean clearAdmitted() {
        return admitted.getAndSet(false);
    }

    /**
     * Return every byte still reserved; the request is over. Also tells
     * the admission gate that this request has ended
     * ({@link ScanAdmission#requestEnded(LanceHitsAccounting)}): the
     * gated paths of the request were counted in flight on this
     * instance, and the executor closes the search context that owns it
     * whether the request succeeded or failed.
     */
    @Override
    public void close() {
        long outstanding = reserved.getAndSet(0L);
        if (outstanding != 0L) {
            breaker.addWithoutBreaking(-outstanding);
        }
        ScanAdmission.requestEnded(this);
    }
}

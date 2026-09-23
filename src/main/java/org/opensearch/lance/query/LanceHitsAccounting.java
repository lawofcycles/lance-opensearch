/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.Objects;
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
     * Return every byte still reserved; the request is over. Also tells
     * the admission gate that the request this thread admitted, if any,
     * has ended ({@link FtsAdmission#requestEnded}): the executor closes
     * the search context that owns this instance on the thread it ran
     * the gate on, whether the request succeeded or failed.
     */
    @Override
    public void close() {
        long outstanding = reserved.getAndSet(0L);
        if (outstanding != 0L) {
            breaker.addWithoutBreaking(-outstanding);
        }
        FtsAdmission.requestEnded();
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.opensearch.common.lucene.MinimumScoreCollector;

/**
 * The collector knobs of a request, {@code min_score} and
 * {@code terminate_after}, composed over a {@link CollectorManager} the
 * way {@code QueryPhase} composes its {@code QueryCollectorContext}s:
 * the early termination innermost, the minimum score outermost, so a
 * document below the score threshold neither reaches the wrapped
 * collector nor counts towards {@code terminate_after}.
 *
 * <p>{@code terminate_after} is a {@link MultiCollector} of a counting
 * collector and the wrapped one, as in
 * {@code QueryCollectorContext.createEarlyTerminationCollectorContext}:
 * the counter throws {@link Terminated} on the document past the bound,
 * before the wrapped collector sees it, and the exception aborts the
 * whole collection (a {@code CollectionTerminatedException} would only
 * end the leaf, and a {@link MultiCollector} would drop the counter and
 * carry on). The caller catches {@link Terminated} and reduces the
 * collectors it obtained from the manager itself, since the searcher
 * never reached {@code reduce}. A collector that answers its count from
 * {@code Weight.count} (Lucene's {@code TotalHitCountCollector} over
 * {@code match_all}) keeps that count: the stock search path reports the full
 * count next to {@code terminated_early: true} for a {@code size: 0}
 * request, and this composition does the same.
 *
 * <p>{@code min_score} is the stock {@link MinimumScoreCollector}, which
 * OpenSearch's {@code BucketCollectorProcessor} already sees through
 * when it post collects an aggregator tree.
 */
final class CollectorKnobs {

    /** Thrown by the terminate_after counter on the document past the bound. */
    static final class Terminated extends RuntimeException {
        Terminated(int maxCountHits) {
            super("early termination after " + maxCountHits + " documents", null, false, false);
        }
    }

    private final Float minScore;
    private final int terminateAfter;

    CollectorKnobs(Float minScore, int terminateAfter) {
        this.minScore = minScore;
        this.terminateAfter = terminateAfter;
    }

    boolean any() {
        return minScore != null || terminateAfter > 0;
    }

    boolean terminatesEarly() {
        return terminateAfter > 0;
    }

    /**
     * {@code in} wrapped with the knobs of this request: the collector
     * itself when there are none.
     */
    Collector wrap(Collector in) {
        Collector collector = in;
        if (terminateAfter > 0) {
            collector = MultiCollector.wrap(new Counter(terminateAfter), collector);
        }
        if (minScore != null) {
            collector = new MinimumScoreCollector(collector, minScore);
        }
        return collector;
    }

    /**
     * A manager whose collectors are {@code inner}'s wrapped with the
     * knobs, and whose {@link Wrapped#reduce} maps them back to the
     * inner collectors before {@code inner.reduce}. Collectors are
     * created on the calling thread before the slices run, so the map
     * is not shared between threads while it is written.
     */
    <C extends Collector, T> Wrapped<C, T> wrap(CollectorManager<C, T> inner) {
        return new Wrapped<>(inner, this);
    }

    /** See {@link #wrap(CollectorManager)}. */
    static final class Wrapped<C extends Collector, T> implements CollectorManager<Collector, T> {
        private final CollectorManager<C, T> inner;
        private final CollectorKnobs knobs;
        private final Map<Collector, C> inners = new IdentityHashMap<>();

        private Wrapped(CollectorManager<C, T> inner, CollectorKnobs knobs) {
            this.inner = inner;
            this.knobs = knobs;
        }

        @Override
        public Collector newCollector() throws IOException {
            C collector = inner.newCollector();
            Collector wrapped = knobs.wrap(collector);
            inners.put(wrapped, collector);
            return wrapped;
        }

        @Override
        public T reduce(Collection<Collector> collectors) throws IOException {
            return inner.reduce(innersOf(collectors));
        }

        /** The inner collectors created so far, for the reduce after a {@link Terminated} abort. */
        List<C> innersCreated() {
            return new ArrayList<>(inners.values());
        }

        /** The inner collectors behind {@code collectors}, in the same order. */
        List<C> innersOf(Collection<Collector> collectors) {
            List<C> out = new ArrayList<>(collectors.size());
            for (Collector collector : collectors) {
                C in = inners.get(collector);
                if (in == null) {
                    throw new IllegalStateException("collector " + collector + " was not created by this manager");
                }
                out.add(in);
            }
            return out;
        }

        /** Reduce the inner collectors created so far; for the caller that caught {@link Terminated}. */
        T reduceCreated() throws IOException {
            return inner.reduce(innersCreated());
        }
    }

    /**
     * Counts the documents it is offered and throws {@link Terminated}
     * on the one past {@code maxCountHits}, or on the first leaf offered
     * once the bound is reached, the way {@code EarlyTerminatingCollector}
     * does with forced termination.
     */
    private static final class Counter extends SimpleCollector {
        private final int maxCountHits;
        private int numCollected;

        Counter(int maxCountHits) {
            this.maxCountHits = maxCountHits;
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) {
            if (numCollected >= maxCountHits) {
                throw new Terminated(maxCountHits);
            }
        }

        @Override
        public void collect(int doc) {
            if (++numCollected > maxCountHits) {
                throw new Terminated(maxCountHits);
            }
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }
}

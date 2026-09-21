/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.engine.LanceFragmentLeafReader;

/**
 * Lucene query executing a Lance vector nearest search per shard.
 *
 * <p>Lance's scanner rejects {@code fragmentIds} together with a
 * {@code nearest} clause ("not supported for fragment scan"), so a naive
 * implementation would either run the scan per fragment (drastically
 * duplicating work) or across the entire shard for every leaf. We take a
 * middle path: run the shard-wide scan once per {@link Weight} instance,
 * bucket the returned row addresses by fragment id, and hand each leaf
 * only its own portion. The coordinator merge of per-shard results still
 * reconstructs a global top-k. Score = boost / (1 + distance).
 */
public final class LanceKnnQuery extends Query {

    private final String column;
    private final float[] vector;
    private final int k;
    private final Integer nprobes;
    private final Integer refineFactor;
    private final Integer ef;
    private final org.lance.index.DistanceType distanceType;
    private final Boolean useIndex;
    private final String filter;

    public LanceKnnQuery(String column, float[] vector, int k) {
        this(column, vector, k, null, null, null, null, null, null);
    }

    public LanceKnnQuery(
        String column,
        float[] vector,
        int k,
        Integer nprobes,
        Integer refineFactor,
        Integer ef,
        org.lance.index.DistanceType distanceType,
        Boolean useIndex,
        String filter
    ) {
        this.column = column;
        this.vector = vector;
        this.k = k;
        this.nprobes = nprobes;
        this.refineFactor = refineFactor;
        this.ef = ef;
        this.distanceType = distanceType;
        this.useIndex = useIndex;
        this.filter = filter;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new LanceKnnWeight(this, boost, LanceHitsAccounting.of(searcher), LanceCancellation.of(searcher));
    }

    /**
     * Projection of the nearest scan: the distance only. The row address
     * comes through {@code withRowAddress(true)}; no data column is
     * read. Named explicitly for the same reason as
     * {@link LanceFtsQuery#HITS_SCAN_COLUMNS}: Lance still adds
     * {@code _distance} to an empty projection by default but logs a
     * deprecation warning per scan for it.
     */
    static final List<String> HITS_SCAN_COLUMNS = List.of("_distance");

    private final class LanceKnnWeight extends Weight implements LanceHintingWeight {

        private final float boost;
        // Hit buffers are reserved with the request's accounting before
        // they are allocated and released with the search context; a
        // knn scan holds at most k rows, so the reservation is small,
        // but the path is the one the FTS Weight takes and a refusal
        // surfaces the same way (CircuitBreakingException, HTTP 429).
        private final LanceHitsAccounting accounting;
        // Checked at every batch boundary of the nearest scan.
        private final LanceCancellation cancellation;
        // Cache is populated on the first Lance-backed leaf we visit and then
        // reused for every other leaf in the same shard. The volatile field is
        // set once via CAS so concurrent readers see a fully constructed map.
        private final AtomicReference<Map<Integer, LanceFragmentHits>> shardHits = new AtomicReference<>();

        LanceKnnWeight(LanceKnnQuery query, float boost, LanceHitsAccounting accounting, LanceCancellation cancellation) {
            super(query);
            this.boost = boost;
            this.accounting = Objects.requireNonNull(accounting, "accounting must not be null");
            this.cancellation = Objects.requireNonNull(cancellation, "cancellation must not be null");
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
            return Explanation.match(0f, "lance knn");
        }

        /**
         * The k nearest rows of the shard that fall into {@code leaf}'s
         * fragment, running the scan on first use. Same as
         * LanceFtsQuery: a leaf outside the k nearest rows gets an
         * empty hit set rather than null, so Lucene's BulkScorer
         * request reaches it and the leaf learns that nothing will be
         * collected there.
         */
        private LanceFragmentHits leafHits(LanceFragmentLeafReader leaf) throws IOException {
            LanceFragmentHits hits = ensureShardScan(leaf).get(leaf.fragmentId());
            return hits == null ? LanceFragmentHits.EMPTY : hits;
        }

        @Override
        public void hintExclusive(LeafReaderContext context) throws IOException {
            LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(context.reader());
            if (leaf == null) {
                return;
            }
            leaf.hintMatchedOffsets(leafHits(leaf).sortedDocIds(), LanceFragmentLeafReader.wrappedOnlyByOwnReaders(context.reader()));
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(context.reader());
            if (leaf == null) {
                return null;
            }
            LanceFragmentHits hits = leafHits(leaf);
            // The hit set is the sparse list Lance's nearest scan
            // returned for this fragment, materialised directly rather
            // than through float[maxDoc] and FixedBitSet(maxDoc). On a
            // 250k-row fragment this keeps per-fragment heap at 8 bytes
            // per hit (typically <= k) instead of 1 MB + 31 KB, which is
            // what keeps 64 concurrent queries over 80 fragments under
            // the parent breaker. Lance returns rows in score order and
            // the Lucene DocIdSetIterator contract asks for ascending
            // docIds, so the sorted view is used; it is the same array
            // every other supplier and hint of this Weight sees for the
            // fragment.
            int[] docIds = hits.sortedDocIds();
            float[] hitScores = hits.sortedScores();
            // Same hint protocol as LanceFtsQuery: the leaf learns the
            // k nearest rows of this fragment so sort and aggregation
            // columns are fetched for those rows only.
            leaf.hintMatchedOffsets(docIds, false);
            LanceSparseHitIterator iterator = new LanceSparseHitIterator(docIds, hitScores, docIds.length);
            Scorer scorer = new Scorer() {
                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }

                @Override
                public float getMaxScore(int upTo) {
                    return Float.MAX_VALUE;
                }

                @Override
                public float score() {
                    return iterator.currentScore();
                }

                @Override
                public int docID() {
                    return iterator.docID();
                }
            };
            // Under a reader wrapper the plugin does not know (the security
            // plugin's document and field level security reader) the
            // hint is delivered but never marked exclusive, so keyword
            // dictionaries keep the full column path there.
            return new LanceHintingScorerSupplier(scorer, leaf, docIds, LanceFragmentLeafReader.wrappedOnlyByOwnReaders(context.reader()));
        }

        private Map<Integer, LanceFragmentHits> ensureShardScan(LanceFragmentLeafReader leaf) throws IOException {
            Map<Integer, LanceFragmentHits> cached = shardHits.get();
            if (cached != null) {
                return cached;
            }
            // First scan on this shard is where Lance loads the vector
            // index into native memory. Refuse to start it if the
            // breaker has already tripped so we do not push the cache
            // past its budget mid-query.
            LanceCircuitBreaker.checkAndTrip("lance_knn_query");
            Map<Integer, LanceFragmentHits> fresh = new HashMap<>();
            org.lance.ipc.Query.Builder qb = new org.lance.ipc.Query.Builder().setColumn(column).setKey(vector).setK(k);
            if (nprobes != null) {
                qb.setNprobes(nprobes);
            }
            if (refineFactor != null) {
                qb.setRefineFactor(refineFactor);
            }
            if (ef != null) {
                qb.setEf(ef);
            }
            if (distanceType != null) {
                qb.setDistanceType(distanceType);
            }
            if (useIndex != null) {
                qb.setUseIndex(useIndex);
            }
            // Only the distance and the row address come back; the
            // vector column itself and every other data column stay in
            // Lance, the Weight reads none of them.
            ScanOptions.Builder options = new ScanOptions.Builder().nearest(qb.build()).columns(HITS_SCAN_COLUMNS).withRowAddress(true);
            if (filter != null && !filter.isEmpty()) {
                // Push the filter down as a pre-filter so Lance evaluates
                // it BEFORE applying the k-nearest cutoff. Without this
                // the post-filter path drops filter-mismatching hits from
                // the top-K and can leave fewer than k results even when
                // the table has plenty of matching rows.
                options.filter(filter);
                options.prefilter(true);
            }
            try (LanceScanner scanner = leaf.dataset().newScan(options.build()); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    cancellation.checkCancelled();
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    Float4Vector distance = (Float4Vector) root.getVector("_distance");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long addr = rowAddr.get(i);
                        int fragId = (int) (addr >>> 32);
                        int offset = (int) (addr & 0xFFFFFFFFL);
                        // Score = boost / (1 + distance), so a smaller
                        // distance is a higher score.
                        fresh.computeIfAbsent(fragId, id -> new LanceFragmentHits(accounting)).add(offset, boost / (1f + distance.get(i)));
                    }
                }
            } catch (IOException | CircuitBreakingException | TaskCancelledException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            // Whichever thread wins the CAS installs the map; losers
            // reuse it and drop the buffers of their own scan.
            if (shardHits.compareAndSet(null, fresh)) {
                return fresh;
            }
            accounting.release(LanceFtsQuery.heapBytesOf(fresh));
            return shardHits.get();
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
    }

    @Override
    public String toString(String field) {
        return "LanceKnnQuery(" + column + ", k=" + k + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LanceKnnQuery q
            && column.equals(q.column)
            && Arrays.equals(vector, q.vector)
            && k == q.k
            && Objects.equals(nprobes, q.nprobes)
            && Objects.equals(refineFactor, q.refineFactor)
            && Objects.equals(ef, q.ef)
            && Objects.equals(distanceType, q.distanceType)
            && Objects.equals(useIndex, q.useIndex)
            && Objects.equals(filter, q.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, Arrays.hashCode(vector), k, nprobes, refineFactor, ef, distanceType, useIndex, filter);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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
import org.opensearch.lance.LanceCircuitBreaker;
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
        return new LanceKnnWeight(this, boost);
    }

    /**
     * Per-fragment result buckets for a single shard-wide Lance scan.
     * Sizes are typically at most {@code k} per fragment, so the arrays
     * grow geometrically from a small initial capacity.
     */
    static final class FragmentHits {
        int[] offsets = new int[Math.max(1, 8)];
        float[] distances = new float[offsets.length];
        int size = 0;

        void add(int offset, float distance) {
            if (size == offsets.length) {
                offsets = Arrays.copyOf(offsets, offsets.length * 2);
                distances = Arrays.copyOf(distances, distances.length * 2);
            }
            offsets[size] = offset;
            distances[size] = distance;
            size++;
        }
    }

    private final class LanceKnnWeight extends Weight {

        private final float boost;
        // Cache is populated on the first Lance-backed leaf we visit and then
        // reused for every other leaf in the same shard. The volatile field is
        // set once via CAS so concurrent readers see a fully constructed map.
        private final AtomicReference<Map<Integer, FragmentHits>> shardHits = new AtomicReference<>();

        LanceKnnWeight(LanceKnnQuery query, float boost) {
            super(query);
            this.boost = boost;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
            return Explanation.match(0f, "lance knn");
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(context.reader());
            if (leaf == null) {
                return null;
            }
            Map<Integer, FragmentHits> hitsByFragment = ensureShardScan(leaf);
            FragmentHits hits = hitsByFragment.get(leaf.fragmentId());
            // Same as LanceFtsQuery: a leaf outside the k nearest rows
            // gets a supplier over an empty hit set rather than null, so
            // Lucene's BulkScorer request reaches it and the leaf learns
            // that nothing will be collected there.
            int hitCount = hits == null ? 0 : hits.size;

            // FragmentHits.offsets / distances is already the sparse
            // list Lance's nearest scan returned for this fragment,
            // so materialise the hit set directly rather than
            // allocating float[maxDoc] and FixedBitSet(maxDoc). On
            // a 250k-row fragment this drops per-fragment heap from
            // 1 MB + 31 KB to 8 bytes * hits.size (typically <= k),
            // which is what keeps 64 concurrent queries over 80
            // fragments under the parent breaker.
            //
            // Lance's nearest scan returns rows in score order; the
            // Lucene DocIdSetIterator contract asks for ascending
            // docIds, so pack (offset, score) into longs, sort, and
            // hand a LanceSparseHitIterator to the Scorer.
            long[] packed = new long[hitCount];
            for (int i = 0; i < hitCount; i++) {
                int offset = hits.offsets[i];
                float score = boost / (1f + hits.distances[i]);
                packed[i] = ((long) offset << 32) | (Float.floatToIntBits(score) & 0xFFFFFFFFL);
            }
            Arrays.sort(packed);
            int[] docIds = new int[hitCount];
            float[] hitScores = new float[hitCount];
            for (int i = 0; i < hitCount; i++) {
                docIds[i] = (int) (packed[i] >>> 32);
                hitScores[i] = Float.intBitsToFloat((int) (packed[i] & 0xFFFFFFFFL));
            }
            // Same hint protocol as LanceFtsQuery: the leaf learns the
            // k nearest rows of this fragment so sort and aggregation
            // columns are fetched for those rows only.
            leaf.hintMatchedOffsets(docIds, false);
            LanceSparseHitIterator iterator = new LanceSparseHitIterator(docIds, hitScores, hitCount);
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

        private Map<Integer, FragmentHits> ensureShardScan(LanceFragmentLeafReader leaf) throws IOException {
            Map<Integer, FragmentHits> cached = shardHits.get();
            if (cached != null) {
                return cached;
            }
            // First scan on this shard is where Lance loads the vector
            // index into native memory. Refuse to start it if the
            // breaker has already tripped so we do not push the cache
            // past its budget mid-query.
            LanceCircuitBreaker.checkAndTrip("lance_knn_query");
            Map<Integer, FragmentHits> fresh = new HashMap<>();
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
            ScanOptions.Builder options = new ScanOptions.Builder().nearest(qb.build()).withRowAddress(true);
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
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    Float4Vector distance = (Float4Vector) root.getVector("_distance");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long addr = rowAddr.get(i);
                        int fragId = (int) (addr >>> 32);
                        int offset = (int) (addr & 0xFFFFFFFFL);
                        fresh.computeIfAbsent(fragId, id -> new FragmentHits()).add(offset, distance.get(i));
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            // Whichever thread wins the CAS installs the map; losers reuse it.
            if (shardHits.compareAndSet(null, fresh)) {
                return fresh;
            }
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
            && java.util.Arrays.equals(vector, q.vector)
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
        return Objects.hash(column, java.util.Arrays.hashCode(vector), k, nprobes, refineFactor, ef, distanceType, useIndex, filter);
    }
}

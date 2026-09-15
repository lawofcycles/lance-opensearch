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
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
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
            if (hits == null || hits.size == 0) {
                return null;
            }

            int maxDoc = leaf.maxDoc();
            FixedBitSet matches = new FixedBitSet(maxDoc);
            float[] scores = new float[maxDoc];
            for (int i = 0; i < hits.size; i++) {
                int offset = hits.offsets[i];
                matches.set(offset);
                scores[offset] = boost / (1f + hits.distances[i]);
            }
            BitSetIterator iterator = new BitSetIterator(matches, hits.size);
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
                    return scores[iterator.docID()];
                }

                @Override
                public int docID() {
                    return iterator.docID();
                }
            };
            return new Weight.DefaultScorerSupplier(scorer);
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

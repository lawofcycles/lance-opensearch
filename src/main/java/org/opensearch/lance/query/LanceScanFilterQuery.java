/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.lance.engine.LanceFragmentLeafReader;

/**
 * Lucene {@link org.apache.lucene.search.Query} that expresses a Lance SQL
 * filter as a per-leaf scorer. The scorer runs a Lance native scan against
 * {@code fragmentIds=[leaf.fragmentId()]} with the SQL filter applied, and
 * exposes every matching row's fragment offset as a doc id through a
 * {@link BitSetIterator}. Every returned doc is scored 1.0f (no BM25, no
 * ranking); this class is a bit-mask carrier for use as a filter, not a
 * relevance query.
 *
 * <p>Mirrors {@link LanceFtsQuery}'s per-leaf pattern so aggregators and
 * bucket collectors get exactly one Lance native scan per fragment, and
 * the matched doc set feeds them through the standard
 * {@link org.apache.lucene.search.LeafCollector#collect(int)} contract. The
 * fragment path uses this to run stock aggregators against Lance-native
 * filter results without translating the filter back into Lucene primitives
 * (Lucene has no inverted index or PointValues on top of Lance data).
 *
 * <p>Not {@link org.opensearch.core.common.io.stream.Writeable}: Lucene
 * {@code Query} objects live per-node during a single search cycle and never
 * cross the transport boundary. The coordinator ships {@code filterSql} as a
 * plain string in {@link org.opensearch.lance.dispatch.LanceFragmentQueryRequest};
 * each data node instantiates its own {@code LanceScanFilterQuery} from that
 * string.
 */
public final class LanceScanFilterQuery extends org.apache.lucene.search.Query {

    private final String filterSql;

    public LanceScanFilterQuery(String filterSql) {
        this.filterSql = Objects.requireNonNull(filterSql, "filterSql must not be null");
        if (filterSql.isEmpty()) {
            throw new IllegalArgumentException("filterSql must not be empty; use MatchAllDocsQuery for the null-filter case");
        }
    }

    public String filterSql() {
        return filterSql;
    }

    @Override
    public String toString(String field) {
        return "LanceScanFilterQuery{" + filterSql + "}";
    }

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o) && filterSql.equals(((LanceScanFilterQuery) o).filterSql);
    }

    @Override
    public int hashCode() {
        return classHash() ^ filterSql.hashCode();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) {
                return Explanation.match(1.0f, "lance scan filter");
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(context.reader());
                if (leaf == null) {
                    return null;
                }
                int maxDoc = leaf.maxDoc();
                FixedBitSet matches = new FixedBitSet(maxDoc);
                ScanOptions options = new ScanOptions.Builder()
                    .fragmentIds(Collections.singletonList(leaf.fragmentId()))
                    .filter(filterSql)
                    .withRowAddress(true)
                    .limit((long) maxDoc)
                    .build();
                try (LanceScanner scanner = leaf.dataset().newScan(options); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                        for (int i = 0; i < root.getRowCount(); i++) {
                            int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                            matches.set(offset);
                        }
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException(e);
                }
                if (matches.cardinality() == 0) {
                    return null;
                }
                DocIdSetIterator iterator = new BitSetIterator(matches, matches.cardinality());
                Scorer scorer = new Scorer() {
                    @Override
                    public DocIdSetIterator iterator() {
                        return iterator;
                    }

                    @Override
                    public float getMaxScore(int upTo) {
                        return 1.0f;
                    }

                    @Override
                    public float score() {
                        return 1.0f;
                    }

                    @Override
                    public int docID() {
                        return iterator.docID();
                    }
                };
                return new Weight.DefaultScorerSupplier(scorer);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // Same rationale as LanceFtsQuery: the scan reads live
                // Lance data whose visibility may advance between
                // searches. Skip Lucene's query cache and let the
                // reader lifecycle handle freshness.
                return false;
            }
        };
    }
}

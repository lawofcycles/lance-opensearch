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

    /** Sentinel that disables top-k pushdown; the scan is bounded only by fragment maxDoc. */
    public static final int SCAN_LIMIT_UNBOUNDED = 0;

    private final String filterSql;
    /**
     * Upper bound on rows the per-leaf Lance scan is allowed to return.
     * {@link #SCAN_LIMIT_UNBOUNDED} lets the scan fill the full fragment.
     *
     * <p>Callers that know they only need the top {@code size + from}
     * hits (pure scalar filter shape, no sort, no aggregations, no
     * post_filter) pass that value here and let Lance stop scanning
     * once it has enough rows. The scan is deterministic in Lance's
     * internal row-address order (which, for indexed scans, follows
     * the scalar index's ordering); OpenSearch does not promise a
     * particular hit order when the query has no sort clause, so
     * clipping in Lance rather than in Lucene collectors preserves the
     * visible contract while eliminating the Arrow transfer of hits
     * the caller will never look at.
     *
     * <p>Aggregations, sort, and post_filter all need the full matched
     * set; the resolver keeps this at {@link #SCAN_LIMIT_UNBOUNDED} for
     * those shapes.
     */
    private final int scanLimit;

    /**
     * Convenience constructor for callers that do not want to enable
     * top-k pushdown. Equivalent to
     * {@code new LanceScanFilterQuery(filterSql, SCAN_LIMIT_UNBOUNDED)}.
     */
    public LanceScanFilterQuery(String filterSql) {
        this(filterSql, SCAN_LIMIT_UNBOUNDED);
    }

    public LanceScanFilterQuery(String filterSql, int scanLimit) {
        this.filterSql = Objects.requireNonNull(filterSql, "filterSql must not be null");
        if (filterSql.isEmpty()) {
            throw new IllegalArgumentException("filterSql must not be empty; use MatchAllDocsQuery for the null-filter case");
        }
        if (scanLimit < 0) {
            throw new IllegalArgumentException("scanLimit must not be negative, was " + scanLimit);
        }
        this.scanLimit = scanLimit;
    }

    public String filterSql() {
        return filterSql;
    }

    public int scanLimit() {
        return scanLimit;
    }

    @Override
    public String toString(String field) {
        return "LanceScanFilterQuery{filter=" + filterSql + ", scanLimit=" + scanLimit + "}";
    }

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o)
            && filterSql.equals(((LanceScanFilterQuery) o).filterSql)
            && scanLimit == ((LanceScanFilterQuery) o).scanLimit;
    }

    @Override
    public int hashCode() {
        return classHash() ^ filterSql.hashCode() ^ Integer.hashCode(scanLimit);
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
                // Cap the scan at the caller-supplied top-k when they
                // asked for one (pure scalar filter shape). Fall back
                // to maxDoc otherwise so aggregations and other
                // consumers of the full match set keep working.
                long effectiveLimit = scanLimit == SCAN_LIMIT_UNBOUNDED ? (long) maxDoc : Math.min((long) scanLimit, (long) maxDoc);
                FixedBitSet matches = new FixedBitSet(maxDoc);
                ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(leaf.fragmentId()))
                    .filter(filterSql)
                    .withRowAddress(true)
                    .limit(effectiveLimit)
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

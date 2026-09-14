/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

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
import org.lance.ipc.FullTextQuery;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

/**
 * Lucene query that executes Lance FTS per leaf (fragment) and streams back
 * matching docids with Lance BM25 scores. This is the RFC's shard-level
 * rewrite of text queries to native Lance queries, surfaced through the real
 * search phase.
 */
public final class LanceFtsQuery extends Query {

    private final String column;
    private final String text;
    private final boolean phrase;
    private final int slop;
    private final FullTextQuery.Operator operator;

    public LanceFtsQuery(String column, String text) {
        this(column, text, false, 0, FullTextQuery.Operator.OR);
    }

    /**
     * Convenience for match queries with an explicit operator.
     */
    public LanceFtsQuery(String column, String text, FullTextQuery.Operator operator) {
        this(column, text, false, 0, operator);
    }

    /**
     * @param column Lance FTS-indexed column to query
     * @param text raw query text; Lance runs its own tokenizer on it
     * @param phrase true for phrase queries (Lance {@code FullTextQuery.phrase}),
     *     false for term/match queries (Lance {@code FullTextQuery.match})
     * @param slop phrase slop (allowed number of intervening tokens); ignored
     *     when {@code phrase} is false
     */
    public LanceFtsQuery(String column, String text, boolean phrase, int slop) {
        this(column, text, phrase, slop, FullTextQuery.Operator.OR);
    }

    /**
     * Full constructor: match queries can specify AND / OR operator to
     * control how Lance combines the tokens it derives from
     * {@code text}. Ignored when {@code phrase} is true.
     */
    public LanceFtsQuery(String column, String text, boolean phrase, int slop, FullTextQuery.Operator operator) {
        this.column = column;
        this.text = text;
        this.phrase = phrase;
        this.slop = Math.max(0, slop);
        this.operator = operator == null ? FullTextQuery.Operator.OR : operator;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) {
                return Explanation.match(0f, "lance fts");
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (!(org.apache.lucene.index.FilterLeafReader.unwrap(context.reader()) instanceof LanceFragmentLeafReader leaf)) {
                    return null;
                }
                // Security plugin FLS hides a field by dropping it from the
                // wrapper reader's FieldInfos. If the wrapper reader we were
                // handed no longer exposes `column`, the caller should not be
                // able to use it as a search term either. Return an empty
                // scorer supplier so the query contributes no hits, matching
                // the standard `match` behaviour on a Lucene index.
                if (context.reader().getFieldInfos().fieldInfo(column) == null) {
                    return null;
                }
                int maxDoc = leaf.maxDoc();
                float[] scores = new float[maxDoc];
                org.apache.lucene.util.FixedBitSet matches = new org.apache.lucene.util.FixedBitSet(maxDoc);
                ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(leaf.fragmentId()))
                    .fullTextQuery(
                        phrase
                            ? FullTextQuery.phrase(text, column, slop)
                            : FullTextQuery.match(text, column, 1f, java.util.Optional.empty(), 50, operator, 0)
                    )
                    .withRowAddress(true)
                    .limit((long) maxDoc)
                    .build();
                try (LanceScanner scanner = leaf.dataset().newScan(options); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                        Float4Vector score = (Float4Vector) root.getVector("_score");
                        for (int i = 0; i < root.getRowCount(); i++) {
                            int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                            matches.set(offset);
                            scores[offset] = score.get(i) * boost;
                        }
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException(e);
                }

                var iterator = new org.apache.lucene.util.BitSetIterator(matches, matches.cardinality());
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

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    @Override
    public String toString(String field) {
        return "LanceFtsQuery("
            + column
            + ":"
            + (phrase ? "phrase[" + slop + "]=" : (operator == FullTextQuery.Operator.AND ? "and:" : ""))
            + text
            + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LanceFtsQuery q
            && column.equals(q.column)
            && text.equals(q.text)
            && phrase == q.phrase
            && slop == q.slop
            && operator == q.operator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, text, phrase, slop, operator);
    }
}

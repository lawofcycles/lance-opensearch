/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import org.apache.calcite.rex.RexNode;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import org.lance.ipc.ColumnOrdering;

/**
 * One operation pushed into a {@link LanceTableScan}: the Lance dataset
 * scan computes it, so the plan above the scan no longer contains the
 * node it replaced. The kinds today are {@link PushedAggregate},
 * {@link PushedFilter}, {@link PushedFts}, {@link PushedKnn} and
 * {@link PushedTopK}; the project kind joins when its pushdown rule
 * lands.
 *
 * <p>A scan carries at most one query kind ({@link PushedAggregate},
 * {@link PushedFilter}, {@link PushedFts} or {@link PushedKnn}) and
 * optionally a {@link PushedTopK} that cuts the page of that query.
 * {@link PushedTopK} combines with any query kind other than
 * {@link PushedAggregate}, which consumes every matching row. The five
 * permits are mutually exclusive except for that one top-k
 * combination, and every {@code LanceTableScan.withPushed*} method
 * refuses the tree that would violate the invariant.
 */
public sealed interface PushedOperation permits PushedOperation.PushedAggregate, PushedOperation.PushedFilter, PushedOperation.PushedFts,
    PushedOperation.PushedKnn, PushedOperation.PushedTopK {

    /**
     * A sorted, cut hits page pushed into the scan: the
     * {@link LanceTopK} the scan replaced, the {@link LanceHitShape}
     * folded with it (null when the plan carried none, the executor's
     * own top-k route), the Lance {@code ColumnOrdering}s the executor
     * hands to {@code ScanOptions.setColumnOrderings} (empty for a
     * score ordered page, whose order the FTS or knn scan produces by
     * itself), and the SQL of the {@code search_after} cursor bound the
     * scan ANDs into its filter, or null when the page starts at the
     * top. Aggregates do not combine with a top-k; a pushed filter, FTS
     * or knn is the query the page is cut from.
     */
    record PushedTopK(LanceTopK topK, LanceHitShape hitShape, List<ColumnOrdering> orderings, String cursorSql) implements PushedOperation {

        public PushedTopK {
            Objects.requireNonNull(topK, "topK");
            orderings = List.copyOf(Objects.requireNonNull(orderings, "orderings"));
        }

        /** Rows the page keeps, the {@code limit} of the executor's scan. */
        public int fetch() {
            return topK.fetch();
        }

        /** The Lance orderings of the executor's scan; empty for a score ordered page. */
        public List<ColumnOrdering> toScanOrderings() {
            return orderings;
        }

        /**
         * Prints the collations, the page bounds, the cursor SQL and
         * the hit shape, so the digest of two scans with different
         * pushed pages differs and the explain output names what was
         * pushed.
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("topk{collations=").append(topK.collations())
                .append(", fetch=")
                .append(topK.fetch())
                .append(", offset=")
                .append(topK.offset());
            if (cursorSql != null) {
                sb.append(", cursor=").append(cursorSql);
            }
            if (hitShape != null) {
                sb.append(", hits{columns=")
                    .append(hitShape.outputColumns())
                    .append(", source=")
                    .append(hitShape.includeSource())
                    .append(", id=")
                    .append(hitShape.includeId())
                    .append(", score=")
                    .append(hitShape.includeScore())
                    .append(", sortValues=")
                    .append(hitShape.includeSortValues())
                    .append("}");
            }
            return sb.append("}").toString();
        }
    }

    /**
     * A full text match pushed into the scan: the {@link LanceFtsMatch}
     * the scan replaced (whose builder the executor turns into the
     * Lance {@code FullTextQuery}) and the SQL of the scalar filter the
     * scan evaluates as a Lance prefilter before the inverted-index
     * lookup, or null when the shape carries no filter. The row type
     * gains the {@code _score} column.
     */
    record PushedFts(LanceFtsMatch fts, String filterSql) implements PushedOperation {

        public PushedFts {
            Objects.requireNonNull(fts, "fts");
        }

        /**
         * Prints every FTS parameter and the filter SQL, so the digest
         * of two scans with different pushed operations differs and
         * the explain output names what was pushed.
         */
        @Override
        public String toString() {
            return "fts{kind="
                + fts.kind()
                + ", columns="
                + fts.columns()
                + ", query="
                + fts.queryJson()
                + (filterSql == null ? "" : ", filter=" + filterSql)
                + "}";
        }
    }

    /**
     * A vector nearest search pushed into the scan: the
     * {@link LanceKnnSearch} the scan replaced (whose builder the
     * executor turns into the Lance nearest query) and the SQL of the
     * inner filter the scan evaluates as a Lance prefilter before the
     * top-k cutoff, or null when the clause carries none. The row type
     * gains the {@code _distance} column.
     */
    record PushedKnn(LanceKnnSearch knn, String filterSql) implements PushedOperation {

        public PushedKnn {
            Objects.requireNonNull(knn, "knn");
        }

        /**
         * Prints every knn parameter and the filter SQL, so the digest
         * of two scans with different pushed operations differs and
         * the explain output names what was pushed.
         */
        @Override
        public String toString() {
            return "knn{query=" + knn.queryJson() + (filterSql == null ? "" : ", filter=" + filterSql) + "}";
        }
    }

    /**
     * A filter pushed into the scan: the predicate over the scan row
     * type and the Lance SQL string the printer produced from it, which
     * the executor hands to the dataset scan's {@code filter(sql)}. The
     * row type does not change; only the row count does.
     */
    record PushedFilter(RexNode condition, String sql) implements PushedOperation {

        public PushedFilter {
            Objects.requireNonNull(condition, "condition");
            Objects.requireNonNull(sql, "sql");
        }

        /**
         * Prints the predicate and its SQL, so the digest of two scans
         * with different pushed filters differs and the explain output
         * names what was pushed.
         */
        @Override
        public String toString() {
            return "filter{condition=" + condition + ", sql=" + sql + "}";
        }
    }

    /**
     * An aggregate pushed into the scan: the {@link LanceAggregate} the
     * scan replaced (its input rebuilt to the concrete project / filter
     * / scan tree, so the Substrait producer can re-read it for the
     * percentiles bin scans) and the Substrait bytes of the main scan.
     *
     * <p>The buffer is treated as immutable; readers take
     * {@link #substrait()} for a positioned duplicate instead of
     * touching the stored buffer's position.
     */
    record PushedAggregate(LanceAggregate aggregate, ByteBuffer bytes) implements PushedOperation {

        public PushedAggregate {
            Objects.requireNonNull(aggregate, "aggregate");
            Objects.requireNonNull(bytes, "bytes");
        }

        /** The encoded main scan as a read only view with its own position. */
        public ByteBuffer substrait() {
            return bytes.asReadOnlyBuffer();
        }

        /**
         * Prints the OpenSearch shape of the pushed aggregate, so the
         * digest of two scans with different pushed aggregates differs
         * and the explain output names what was pushed.
         */
        @Override
        public String toString() {
            return "aggregate{groups="
                + aggregate.getGroupCount()
                + ", buckets="
                + aggregate.bucketSpecs()
                + ", metrics="
                + aggregate.metricSpecs()
                + "}";
        }
    }
}

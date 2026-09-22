/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import org.apache.calcite.rex.RexNode;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * One operation pushed into a {@link LanceTableScan}: the Lance dataset
 * scan computes it, so the plan above the scan no longer contains the
 * node it replaced. The kinds today are {@link PushedAggregate} and
 * {@link PushedFilter}; project and sort kinds join when their pushdown
 * rules land.
 */
public sealed interface PushedOperation permits PushedOperation.PushedAggregate, PushedOperation.PushedFilter {

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

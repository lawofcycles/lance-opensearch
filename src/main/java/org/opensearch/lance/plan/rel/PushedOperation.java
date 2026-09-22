/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * One operation pushed into a {@link LanceTableScan}: the Lance dataset
 * scan computes it, so the plan above the scan no longer contains the
 * node it replaced. The only kind today is {@link PushedAggregate};
 * filter, project and sort kinds join when their pushdown rules land.
 */
public sealed interface PushedOperation permits PushedOperation.PushedAggregate {

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

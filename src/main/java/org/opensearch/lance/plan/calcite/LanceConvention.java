/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;

/**
 * Calling convention of operators that execute inside Lance's native scan
 * machinery (Substrait plans, SQL filters, projections pushed into the
 * dataset scan).
 */
public final class LanceConvention extends Convention.Impl {

    /** The single instance; conventions are compared by identity. */
    public static final LanceConvention INSTANCE = new LanceConvention();

    private LanceConvention() {
        super("LANCE", LanceRel.class);
    }

    /**
     * Conversions between the Lance and Lucene conventions are
     * expressed by dedicated converter rules, never by Calcite's abstract
     * converters.
     */
    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
        return false;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;

/**
 * Calling convention of operators that execute through Lucene's aggregator
 * and collector machinery over per-fragment leaf readers.
 */
public final class LuceneConvention extends Convention.Impl {

    /** The single instance; conventions are compared by identity. */
    public static final LuceneConvention INSTANCE = new LuceneConvention();

    private LuceneConvention() {
        super("LUCENE", LuceneRel.class);
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

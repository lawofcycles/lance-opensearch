/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.traits;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;

import java.util.Locale;

/**
 * Whether the figures an operator returns are exact or come from a
 * sketch. A physical operator declares the value in its trait set;
 * a request that needs an exact answer demands {@link #EXACT} at the
 * plan root and the Volcano planner only accepts a plan whose root
 * declares it.
 *
 * <p>{@code percentiles} / {@code percentile_ranks} (a t-digest) and
 * {@code cardinality} (HyperLogLog++) are {@link #APPROXIMATE} on the
 * pushed Lance scan and on the Lucene aggregators alike, because both
 * sides compute the same sketches. Every other aggregation, every hit
 * page and every count is {@link #EXACT}. The trait def's default is
 * {@link #APPROXIMATE}, the weakest declaration and the weakest
 * demand: a root that demands nothing accepts every plan, and an
 * operator that declares nothing promises nothing.
 *
 * <p>The values order: {@link #EXACT} satisfies a demand for either
 * value, {@link #APPROXIMATE} satisfies only itself. There is no
 * enforcer: no operator raises the accuracy of its input, so the trait
 * def's {@link Def#convert} returns null and a plan whose root cannot
 * declare the demanded value fails to plan.
 */
public enum Accuracy implements RelTrait {

    /** Exact figures: the count Lucene's collector returns, a sum, a min, a terms bucket count. */
    EXACT("exact figures"),

    /** Figures from a sketch: a t-digest percentile or a HyperLogLog++ distinct count. */
    APPROXIMATE("figures from a sketch (cardinality or percentiles), the same on the Lance scan and on the Lucene aggregators");

    private final String description;

    Accuracy(String description) {
        this.description = description;
    }

    /** What the value promises, for the refusal message a request that demands more receives. */
    public String description() {
        return description;
    }

    @Override
    public RelTraitDef<Accuracy> getTraitDef() {
        return Def.INSTANCE;
    }

    /** {@link #EXACT} satisfies either demand; {@link #APPROXIMATE} satisfies only itself. */
    @Override
    public boolean satisfies(RelTrait trait) {
        return trait == this || (this == EXACT && trait == APPROXIMATE);
    }

    @Override
    public void register(RelOptPlanner planner) {}

    /** Lower case, so a digest reads {@code LANCE.exact.stable_rowaddr}. */
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * The trait def the planner registers next to the convention def.
     * No conversion exists between the values.
     */
    public static final class Def extends RelTraitDef<Accuracy> {

        /** The single instance; {@code LancePlannerFactory.newCluster} registers it. */
        public static final Def INSTANCE = new Def();

        private Def() {}

        @Override
        public Class<Accuracy> getTraitClass() {
            return Accuracy.class;
        }

        @Override
        public String getSimpleName() {
            return "accuracy";
        }

        /** Null: nothing raises a sketch to an exact figure. */
        @Override
        public RelNode convert(RelOptPlanner planner, RelNode rel, Accuracy toTrait, boolean allowInfiniteCostConverters) {
            return null;
        }

        @Override
        public boolean canConvert(RelOptPlanner planner, Accuracy fromTrait, Accuracy toTrait) {
            return false;
        }

        @Override
        public Accuracy getDefault() {
            return APPROXIMATE;
        }
    }
}

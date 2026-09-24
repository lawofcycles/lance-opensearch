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
 * Whether the order in which an operator returns rows that compare
 * equal under the request's sort is reproducible between two calls. A
 * {@code search_after} cursor continues a page from the sort values of
 * its last hit, so the rows on either side of the cursor must be the
 * same rows on the next call; a physical operator declares what it
 * guarantees and a request with a cursor demands a reproducible value
 * at the plan root.
 *
 * <ul>
 *   <li>{@link #STABLE_ROWADDR}: rows come in Lance row address order,
 *   the order of a bare scan and of a page without a sort over a scalar
 *   query. The stock search path's answer (Lucene doc order over the whole
 *   table reader) is the same order.</li>
 *   <li>{@link #STABLE_KEY}: rows come in the order of a stored column,
 *   with ties resolved by the executor the same way on every call (the
 *   pushed scan's Lance ordering, or Lucene's collector over the same
 *   readers). This is what a cursor over a column sort needs, with or
 *   without a further tie breaker column.</li>
 *   <li>{@link #UNSTABLE}: rows come in an order the executor does not
 *   reproduce: a page cut in score order out of a full text or knn
 *   scan, whose equal scores land in whatever order the scanner's
 *   batches arrive. The trait def's default, so a root that demands
 *   nothing accepts every plan and an operator that declares nothing
 *   promises nothing.</li>
 * </ul>
 *
 * <p>Each stable value satisfies a demand for itself and for
 * {@link #UNSTABLE}; the two stable values do not satisfy each other,
 * because a cursor typed against one order cannot be evaluated in the
 * other. There is no enforcer: nothing turns a score ordered page into
 * a key ordered one after the fact, so the trait def's
 * {@link Def#convert} returns null and a plan whose root cannot declare
 * the demanded value fails to plan.
 */
public enum TieStability implements RelTrait {

    /** Lance row address order: a bare scan, or a page without a sort over a scalar query. */
    STABLE_ROWADDR("rows in Lance row address order"),

    /** The order of a stored column, ties resolved the same way on every call. */
    STABLE_KEY("rows in the order of a stored sort column"),

    /** Score order out of a full text or knn scan, whose equal scores have no reproducible order. */
    UNSTABLE("rows in score order, whose ties a second call may return in a different order");

    private final String description;

    TieStability(String description) {
        this.description = description;
    }

    /** What the value promises, for the refusal message a request that demands more receives. */
    public String description() {
        return description;
    }

    @Override
    public RelTraitDef<TieStability> getTraitDef() {
        return Def.INSTANCE;
    }

    /** A stable value satisfies itself and {@link #UNSTABLE}; {@link #UNSTABLE} satisfies only itself. */
    @Override
    public boolean satisfies(RelTrait trait) {
        return trait == this || (this != UNSTABLE && trait == UNSTABLE);
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
    public static final class Def extends RelTraitDef<TieStability> {

        /** The single instance; {@code LancePlannerFactory.newCluster} registers it. */
        public static final Def INSTANCE = new Def();

        private Def() {}

        @Override
        public Class<TieStability> getTraitClass() {
            return TieStability.class;
        }

        @Override
        public String getSimpleName() {
            return "tie_stability";
        }

        /** Null: nothing makes an unstable order reproducible after the fact. */
        @Override
        public RelNode convert(RelOptPlanner planner, RelNode rel, TieStability toTrait, boolean allowInfiniteCostConverters) {
            return null;
        }

        @Override
        public boolean canConvert(RelOptPlanner planner, TieStability fromTrait, TieStability toTrait) {
            return false;
        }

        @Override
        public TieStability getDefault() {
            return UNSTABLE;
        }
    }
}

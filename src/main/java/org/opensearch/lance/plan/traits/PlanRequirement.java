/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.traits;

import org.apache.calcite.plan.RelTraitSet;

import java.util.Objects;

/**
 * What a request demands of the plan root beyond its convention: an
 * {@link Accuracy} and a {@link TieStability}, each with the request
 * element that demanded it (null when the value is the trait def's
 * default, which demands nothing). {@code RequestPlanner} derives one
 * per request and {@code LancePlannerFactory.plan} places the two
 * values on the root trait set the Volcano planner must satisfy.
 *
 * @param accuracy the accuracy the root must declare
 * @param accuracyReason the request element demanding it, or null
 * @param tieStability the tie stability the root must declare
 * @param tieStabilityReason the request element demanding it, or null
 */
public record PlanRequirement(Accuracy accuracy, String accuracyReason, TieStability tieStability, String tieStabilityReason) {

    /** No demand: both trait defs' defaults, which every plan satisfies. */
    public static final PlanRequirement NONE = new PlanRequirement(
        Accuracy.Def.INSTANCE.getDefault(),
        null,
        TieStability.Def.INSTANCE.getDefault(),
        null
    );

    public PlanRequirement {
        Objects.requireNonNull(accuracy, "accuracy");
        Objects.requireNonNull(tieStability, "tieStability");
    }

    /** This requirement with {@code accuracy} demanded by {@code reason}. */
    public PlanRequirement withAccuracy(Accuracy required, String reason) {
        return new PlanRequirement(required, reason, tieStability, tieStabilityReason);
    }

    /** This requirement with {@code tieStability} demanded by {@code reason}. */
    public PlanRequirement withTieStability(TieStability required, String reason) {
        return new PlanRequirement(accuracy, accuracyReason, required, reason);
    }

    /** Whether both values are the defaults, so the root needs no second look. */
    public boolean isNone() {
        return accuracy == Accuracy.Def.INSTANCE.getDefault() && tieStability == TieStability.Def.INSTANCE.getDefault();
    }

    /** {@code traits} with the two demanded values in place of whatever it carried. */
    public RelTraitSet applyTo(RelTraitSet traits) {
        return traits.plus(accuracy).plus(tieStability);
    }

    /**
     * Whether an operator declaring {@code traits} meets the demand. A
     * trait set without one of the defs (a cluster that did not register
     * them) counts as the default declaration, which promises nothing.
     */
    public boolean satisfiedBy(RelTraitSet traits) {
        return declaredAccuracy(traits).satisfies(accuracy) && declaredTieStability(traits).satisfies(tieStability);
    }

    /** The accuracy {@code traits} declare, the default when the def is absent. */
    public static Accuracy declaredAccuracy(RelTraitSet traits) {
        Accuracy declared = traits.getTrait(Accuracy.Def.INSTANCE);
        return declared == null ? Accuracy.Def.INSTANCE.getDefault() : declared;
    }

    /** The tie stability {@code traits} declare, the default when the def is absent. */
    public static TieStability declaredTieStability(RelTraitSet traits) {
        TieStability declared = traits.getTrait(TieStability.Def.INSTANCE);
        return declared == null ? TieStability.Def.INSTANCE.getDefault() : declared;
    }
}

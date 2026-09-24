/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.traits;

import org.apache.calcite.plan.RelTraitSet;

import java.util.Objects;

/**
 * How the planner met a request's {@link PlanRequirement}. The Volcano
 * run is asked once for the convention alone; when its cheapest plan
 * already declares the demanded {@link Accuracy} and
 * {@link TieStability} nothing more happens ({@link #fired()} is
 * false). Otherwise the enforcer fires: the same cluster is asked again
 * with the demanded values on the root, and either a costlier plan
 * that declares them is chosen ({@link #met()} true) or no plan
 * declares them and the request is refused ({@link #met()} false).
 * The explain endpoint renders {@link #describe()} under
 * {@code traits.enforcer}, so the reader sees whether the demand
 * changed the plan and what the cheapest plan offered instead.
 *
 * @param requirement what the request demanded
 * @param cheapestAccuracy the accuracy the cheapest plan of the first
 *     pass declared, null when the enforcer did not fire
 * @param cheapestTieStability the tie stability the cheapest plan of
 *     the first pass declared, null when the enforcer did not fire
 * @param met whether the plan returned declares the demanded values
 */
public record TraitEnforcement(PlanRequirement requirement, Accuracy cheapestAccuracy, TieStability cheapestTieStability, boolean met) {

    /** No demand was placed, so nothing could fire. */
    public static final TraitEnforcement NONE = new TraitEnforcement(PlanRequirement.NONE, null, null, true);

    public TraitEnforcement {
        Objects.requireNonNull(requirement, "requirement");
        if ((cheapestAccuracy == null) != (cheapestTieStability == null)) {
            throw new IllegalArgumentException("the cheapest plan's traits are recorded together or not at all");
        }
        if (cheapestAccuracy == null && !met) {
            throw new IllegalArgumentException("a demand the first pass met cannot be unmet");
        }
    }

    /** The first pass met {@code requirement}, or there was none. */
    public static TraitEnforcement satisfied(PlanRequirement requirement) {
        return new TraitEnforcement(requirement, null, null, true);
    }

    /** The second pass chose a plan declaring the demand over the cheapest one, which declared {@code cheapest}. */
    public static TraitEnforcement enforced(PlanRequirement requirement, RelTraitSet cheapest) {
        return new TraitEnforcement(
            requirement,
            PlanRequirement.declaredAccuracy(cheapest),
            PlanRequirement.declaredTieStability(cheapest),
            true
        );
    }

    /** No plan declares the demand; the cheapest one declared {@code cheapest}. */
    public static TraitEnforcement unmet(PlanRequirement requirement, RelTraitSet cheapest) {
        return new TraitEnforcement(
            requirement,
            PlanRequirement.declaredAccuracy(cheapest),
            PlanRequirement.declaredTieStability(cheapest),
            false
        );
    }

    /** Whether the second Volcano pass ran. */
    public boolean fired() {
        return cheapestAccuracy != null;
    }

    /**
     * {@code none} when the enforcer did not fire; otherwise one clause
     * per demanded trait the cheapest plan missed, naming the request
     * element, the demanded value and the offered value, and whether a
     * costlier plan was chosen or no plan meets the demand.
     */
    public String describe() {
        if (!fired()) {
            return "none";
        }
        StringBuilder sb = new StringBuilder();
        if (!cheapestAccuracy.satisfies(requirement.accuracy())) {
            sb.append(requirement.accuracyReason())
                .append(" demanded Accuracy [")
                .append(requirement.accuracy().name())
                .append("], the cheapest plan offered [")
                .append(cheapestAccuracy.name())
                .append("]");
        }
        if (!cheapestTieStability.satisfies(requirement.tieStability())) {
            if (sb.length() > 0) {
                sb.append("; ");
            }
            sb.append(requirement.tieStabilityReason())
                .append(" demanded TieStability [")
                .append(requirement.tieStability().name())
                .append("], the cheapest plan offered [")
                .append(cheapestTieStability.name())
                .append("]");
        }
        sb.append(met ? "; a costlier plan declaring the demand was chosen" : "; no plan declares the demand (plan_failed)");
        return sb.toString();
    }
}

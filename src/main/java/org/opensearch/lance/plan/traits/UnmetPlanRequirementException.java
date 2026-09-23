/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.traits;

import org.apache.calcite.plan.RelTraitSet;

/**
 * Thrown by {@code LancePlannerFactory.plan} when a plan of the
 * request exists but no plan declares the traits the request demands:
 * the Volcano planner raised {@code CannotPlanException} for the root
 * with the demanded traits after finding a plan for the root without
 * them. The message names the demanded trait, the request element that
 * demanded it, and what the cheapest plan offers instead, so the 400
 * the coordinator answers tells the caller which part of the request
 * to change. {@code RequestPlanner} turns it into the request error.
 */
public final class UnmetPlanRequirementException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final PlanRequirement requirement;
    private final Accuracy offeredAccuracy;
    private final TieStability offeredTieStability;

    /**
     * @param requirement what the request demanded
     * @param offered the trait set of the cheapest plan the planner
     *     found without the demand
     */
    public UnmetPlanRequirementException(PlanRequirement requirement, RelTraitSet offered) {
        super(message(requirement, offered));
        this.requirement = requirement;
        this.offeredAccuracy = PlanRequirement.declaredAccuracy(offered);
        this.offeredTieStability = PlanRequirement.declaredTieStability(offered);
    }

    /** What the request demanded. */
    public PlanRequirement requirement() {
        return requirement;
    }

    /** The accuracy the cheapest plan declares. */
    public Accuracy offeredAccuracy() {
        return offeredAccuracy;
    }

    /** The tie stability the cheapest plan declares. */
    public TieStability offeredTieStability() {
        return offeredTieStability;
    }

    private static String message(PlanRequirement requirement, RelTraitSet offered) {
        StringBuilder sb = new StringBuilder("plan_failed: no plan of this request meets its trait requirement");
        Accuracy accuracy = PlanRequirement.declaredAccuracy(offered);
        if (!accuracy.satisfies(requirement.accuracy())) {
            sb.append("; ")
                .append(requirement.accuracyReason())
                .append(" requires Accuracy [")
                .append(requirement.accuracy())
                .append("] but every plan offers Accuracy [")
                .append(accuracy)
                .append("] (")
                .append(accuracy.description())
                .append(')');
        }
        TieStability tieStability = PlanRequirement.declaredTieStability(offered);
        if (!tieStability.satisfies(requirement.tieStability())) {
            sb.append("; ")
                .append(requirement.tieStabilityReason())
                .append(" requires TieStability [")
                .append(requirement.tieStability())
                .append("] but every plan offers TieStability [")
                .append(tieStability)
                .append("] (")
                .append(tieStability.description())
                .append(')');
        }
        return sb.toString();
    }
}

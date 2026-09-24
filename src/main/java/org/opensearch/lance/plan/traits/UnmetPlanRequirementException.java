/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.traits;

import org.apache.calcite.rel.RelNode;

import java.util.Objects;

/**
 * Thrown by {@code LancePlannerFactory.plan} when a plan of the
 * request exists but no plan declares the traits the request demands:
 * the Volcano planner raised {@code CannotPlanException} for the root
 * with the demanded traits after finding a plan for the root without
 * them. The message names the demanded trait, the request element that
 * demanded it, and what the cheapest plan offers instead, so the 400
 * the coordinator answers tells the caller which part of the request
 * to change. An {@link IllegalArgumentException}, so the coordinator
 * answers it as a request error without translating it; the explain
 * endpoint catches it and renders {@link #offered()}, the cheapest
 * plan, with the {@link #enforcement()} that refused it.
 */
public final class UnmetPlanRequirementException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final transient TraitEnforcement enforcement;
    private final transient RelNode logical;
    private final transient RelNode offered;

    /**
     * @param requirement what the request demanded
     * @param logical the tree the planner ran over
     * @param offered the cheapest plan the planner found without the
     *     demand, whose root trait set is what every plan offers
     */
    public UnmetPlanRequirementException(PlanRequirement requirement, RelNode logical, RelNode offered) {
        super(message(requirement, offered));
        this.enforcement = TraitEnforcement.unmet(requirement, offered.getTraitSet());
        this.logical = Objects.requireNonNull(logical, "logical");
        this.offered = Objects.requireNonNull(offered, "offered");
    }

    /** What the request demanded. */
    public PlanRequirement requirement() {
        return enforcement.requirement();
    }

    /** The demand and what the cheapest plan offered, unmet. */
    public TraitEnforcement enforcement() {
        return enforcement;
    }

    /** The tree the planner ran over. */
    public RelNode logical() {
        return logical;
    }

    /** The cheapest plan of the request, the one the demand refused. */
    public RelNode offered() {
        return offered;
    }

    /** The accuracy the cheapest plan declares. */
    public Accuracy offeredAccuracy() {
        return enforcement.cheapestAccuracy();
    }

    /** The tie stability the cheapest plan declares. */
    public TieStability offeredTieStability() {
        return enforcement.cheapestTieStability();
    }

    private static String message(PlanRequirement requirement, RelNode offered) {
        StringBuilder sb = new StringBuilder("plan_failed: no plan of this request meets its trait requirement");
        Accuracy accuracy = PlanRequirement.declaredAccuracy(offered.getTraitSet());
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
        TieStability tieStability = PlanRequirement.declaredTieStability(offered.getTraitSet());
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

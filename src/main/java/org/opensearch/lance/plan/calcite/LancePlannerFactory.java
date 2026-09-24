/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.cost.CostInputsHolder;
import org.opensearch.lance.plan.metadata.LanceRelMetadataProvider;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.physical.LuceneHandoffExec;
import org.opensearch.lance.plan.rules.CoordinatorLayerRules;
import org.opensearch.lance.plan.rules.FuseFtsWithFilter;
import org.opensearch.lance.plan.rules.FuseKnnWithFilter;
import org.opensearch.lance.plan.rules.LanceToLuceneConverterRule;
import org.opensearch.lance.plan.rules.PushAggregateIntoLanceScan;
import org.opensearch.lance.plan.rules.PushFilterIntoLanceScan;
import org.opensearch.lance.plan.rules.PushSortLimitIntoLanceScan;
import org.opensearch.lance.plan.traits.Accuracy;
import org.opensearch.lance.plan.traits.PlanRequirement;
import org.opensearch.lance.plan.traits.TieStability;
import org.opensearch.lance.plan.traits.TraitEnforcement;
import org.opensearch.lance.plan.traits.UnmetPlanRequirementException;

/**
 * Assembles the Calcite planner objects for Lance backed indexes: a Volcano
 * cluster costed by {@link LanceCostFactory}, a Hep planner for
 * normalization passes, and a {@link RelBuilder} that resolves index names
 * against a {@link LanceSchema} under the {@code lance} schema name.
 *
 * <p>The factory is stateless apart from the cost budgets it is constructed
 * with, so one instance can serve concurrent plan constructions; every call
 * builds fresh planner objects.
 */
public final class LancePlannerFactory {

    private static final Logger LOGGER = LogManager.getLogger(LancePlannerFactory.class);

    /** Name the {@link LanceSchema} is registered under in the root schema. */
    public static final String SCHEMA_NAME = "lance";

    private final LanceCostFactory costFactory;

    /**
     * @param nativeBudgetBytes budget for predicted native (off-heap) bytes
     * @param heapBudgetBytes budget for predicted heap bytes
     */
    public LancePlannerFactory(long nativeBudgetBytes, long heapBudgetBytes) {
        this.costFactory = new LanceCostFactory(nativeBudgetBytes, heapBudgetBytes);
    }

    /**
     * A fresh cluster over a Volcano planner costed by
     * {@link LanceCostFactory}, with the pushdown rules and the Lucene
     * converter rules registered. The type factory uses
     * {@link LanceTypeSystem#INSTANCE} so timestamp
     * precision above 3 and {@code DECIMAL(20, 0)} survive. Registering
     * the convention trait def is what makes conventions available to
     * the planner; the individual conventions need no explicit
     * registration. The {@link Accuracy} and {@link TieStability} defs
     * are registered next to it, so every trait set the cluster hands
     * out carries a value for each and a request can demand one at the
     * root ({@link #plan(RelNode, CostInputs, PlanRequirement)}). The
     * metadata provider is
     * {@link LanceRelMetadataProvider#INSTANCE}: Lance's row count and
     * distinct row count handlers for the scan, Calcite's defaults for
     * everything else.
     */
    public RelOptCluster newCluster() {
        // The holder travels in the planner's context so every
        // operator's computeSelfCost can read the run's CostInputs; plan
        // fills it before the first cost is computed.
        VolcanoPlanner planner = new VolcanoPlanner(costFactory, Contexts.of(new CostInputsHolder()));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        // The two request demandable traits. Registering the defs puts
        // their defaults (the weakest declaration) into every trait set
        // the cluster hands out; the physical operators replace them
        // with what they guarantee, and plan(...) places what a request
        // demands on the root.
        planner.addRelTraitDef(Accuracy.Def.INSTANCE);
        planner.addRelTraitDef(TieStability.Def.INSTANCE);
        for (PushAggregateIntoLanceScan rule : PushAggregateIntoLanceScan.rules()) {
            planner.addRule(rule);
        }
        for (PushFilterIntoLanceScan rule : PushFilterIntoLanceScan.rules()) {
            planner.addRule(rule);
        }
        for (FuseFtsWithFilter rule : FuseFtsWithFilter.rules()) {
            planner.addRule(rule);
        }
        for (FuseKnnWithFilter rule : FuseKnnWithFilter.rules()) {
            planner.addRule(rule);
        }
        for (PushSortLimitIntoLanceScan rule : PushSortLimitIntoLanceScan.rules()) {
            planner.addRule(rule);
        }
        for (RelOptRule rule : LanceToLuceneConverterRule.rules()) {
            planner.addRule(rule);
        }
        for (RelOptRule rule : CoordinatorLayerRules.rules()) {
            planner.addRule(rule);
        }
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(new SqlTypeFactoryImpl(LanceTypeSystem.INSTANCE)));
        cluster.setMetadataProvider(LanceRelMetadataProvider.INSTANCE);
        return cluster;
    }

    /**
     * {@link #plan(RelNode, CostInputs)} with {@link CostInputs#local()}:
     * one node, local storage, this JVM's CPUs and the parallelism
     * settings at their defaults. What a data node uses to plan the
     * request it received, and what explain and the unit tests use
     * when they have no cluster to describe.
     */
    public RelNode plan(RelNode logical) {
        return plan(logical, CostInputs.local());
    }

    /**
     * Runs the Volcano planner over {@code logical} demanding
     * {@link LuceneConvention} at the root and returns the best
     * physical plan, costing the alternatives under {@code inputs}
     * (the fan out width, the storage kind and the parallelism the
     * request will run with; see {@link CostInputs}). Both physical
     * forms reach that root: a tree the pushdown rules folded into the
     * scan arrives as a zero cost {@link LuceneHandoffExec} over the
     * {@link LanceConvention} scan (unwrapped here, so the caller sees
     * the scan itself and reads its pushed operations), and an
     * aggregation or hits tree arrives as the {@code LuceneAggregateExec}
     * / {@code HeapTopKExec} alternative the converter rules produce
     * when one exists. For an aggregation over a table in the fitted
     * cost model's range the two forms compete on predicted
     * milliseconds, so the answer depends on the table size, the node
     * count and the storage kind; for a smaller table, and for every
     * hits tree, the Lucene operator's constant is pinned above the
     * handoff so the Lance form wins whenever both exist. When neither
     * form exists (a query tree without a top-k or aggregate that no
     * rule fused), or when the planner fails for any other reason, the
     * logical plan itself is returned: the caller reads the root's type
     * to see what was planned, and the fragment routing promises a
     * Lucene fallback for every plan it does not push, so a planner
     * failure must not surface as a request error.
     */
    public RelNode plan(RelNode logical, CostInputs inputs) {
        return plan(logical, inputs, PlanRequirement.NONE);
    }

    /**
     * {@link #plan(RelNode, CostInputs)} under a request's
     * {@link PlanRequirement}. The first Volcano pass is the one above,
     * demanding the convention alone; when its cheapest plan already
     * declares the demanded {@link Accuracy} and {@link TieStability}
     * (the common case: every hits page and every exact aggregate is
     * {@code EXACT}, a column ordered page is {@code STABLE_KEY}) it is
     * returned as is. Otherwise the same cluster is asked again with the
     * two demanded values on the root trait set, so a costlier plan
     * that meets the demand wins over the cheaper one that does not;
     * when no plan declares them the Volcano planner raises
     * {@code CannotPlanException} and this method raises
     * {@link UnmetPlanRequirementException} naming the demand and what
     * the cheapest plan offers, which the caller answers as the
     * request's error. The logical plan the first pass falls back to
     * is untouched: it is only reached when nothing plans at all, and a
     * demand never turns it into an error.
     *
     * @throws UnmetPlanRequirementException when a plan exists but none
     *     declares the demanded traits
     */
    public RelNode plan(RelNode logical, CostInputs inputs, PlanRequirement requirement) {
        return planUnder(logical, inputs, requirement).plan();
    }

    /**
     * The best plan together with how the demand was met: whether the
     * first pass already satisfied it, or the second pass replaced the
     * cheapest plan with one declaring the demanded traits
     * ({@link TraitEnforcement#fired()}). The logical plan the first
     * pass falls back to reports {@link TraitEnforcement#satisfied} for
     * the requirement, as {@link #plan(RelNode, CostInputs, PlanRequirement)}
     * never turns it into an error.
     *
     * @param plan the best plan, the pushed scan unwrapped from its handoff
     * @param enforcement how the requirement was met
     */
    public record PlanOutcome(RelNode plan, TraitEnforcement enforcement) {
    }

    /**
     * {@link #plan(RelNode, CostInputs, PlanRequirement)}, also
     * answering how the demand was met. The explain endpoint renders
     * the outcome's enforcement under {@code traits.enforcer}.
     *
     * @throws UnmetPlanRequirementException when a plan exists but none
     *     declares the demanded traits
     */
    public PlanOutcome planUnder(RelNode logical, CostInputs inputs, PlanRequirement requirement) {
        VolcanoPlanner planner = (VolcanoPlanner) logical.getCluster().getPlanner();
        CostInputsHolder holder = planner.getContext().unwrap(CostInputsHolder.class);
        if (holder != null) {
            holder.set(inputs);
        }
        RelTraitSet luceneRoot = logical.getTraitSet().replace(LuceneConvention.INSTANCE);
        RelNode root = planner.changeTraits(logical, luceneRoot);
        planner.setRoot(root);
        RelNode best;
        try {
            best = planner.findBestExp();
        } catch (RelOptPlanner.CannotPlanException noLucenePlan) {
            return new PlanOutcome(logical, TraitEnforcement.satisfied(requirement));
        } catch (RuntimeException plannerFailure) {
            LOGGER.warn(
                "lance.plan: Volcano planning failed, keeping the logical plan: {}: {}",
                plannerFailure.getClass().getName(),
                plannerFailure.getMessage()
            );
            return new PlanOutcome(logical, TraitEnforcement.satisfied(requirement));
        }
        if (requirement.isNone() || requirement.satisfiedBy(best.getTraitSet())) {
            return new PlanOutcome(unwrapHandoff(best), TraitEnforcement.satisfied(requirement));
        }
        try {
            planner.setRoot(planner.changeTraits(logical, requirement.applyTo(luceneRoot)));
            RelNode enforced = planner.findBestExp();
            return new PlanOutcome(unwrapHandoff(enforced), TraitEnforcement.enforced(requirement, best.getTraitSet()));
        } catch (RelOptPlanner.CannotPlanException nothingMeetsTheDemand) {
            throw new UnmetPlanRequirementException(requirement, logical, unwrapHandoff(best));
        }
    }

    /** The scan itself for a plan that arrived as the zero cost handoff over it, {@code best} otherwise. */
    private static RelNode unwrapHandoff(RelNode best) {
        return best instanceof LuceneHandoffExec handoff ? handoff.getInput() : best;
    }

    /** A fresh Hep planner over an empty program; no rules exist to run yet. */
    public HepPlanner newHepPlanner() {
        return new HepPlanner(new HepProgramBuilder().build());
    }

    /**
     * A builder whose {@code scan(SCHEMA_NAME, index)} resolves the index
     * name against the given schema and yields a
     * {@link LanceTableScan}, because
     * {@link LanceTable} is a {@code TranslatableTable}.
     */
    public RelBuilder relBuilder(LanceSchema schema) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add(SCHEMA_NAME, schema);
        RelOptCluster cluster = newCluster();
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema,
            ImmutableList.of(),
            cluster.getTypeFactory(),
            CalciteConnectionConfig.DEFAULT
        );
        return RelFactories.LOGICAL_BUILDER.create(cluster, catalogReader);
    }
}

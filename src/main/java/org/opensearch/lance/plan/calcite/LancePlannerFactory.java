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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rules.FuseFtsWithFilter;
import org.opensearch.lance.plan.rules.FuseKnnWithFilter;
import org.opensearch.lance.plan.rules.PushAggregateIntoLanceScan;
import org.opensearch.lance.plan.rules.PushFilterIntoLanceScan;
import org.opensearch.lance.plan.rules.PushSortLimitIntoLanceScan;

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
     * {@link LanceCostFactory}, with the pushdown rules registered. The
     * type factory uses {@link LanceTypeSystem#INSTANCE} so timestamp
     * precision above 3 and {@code DECIMAL(20, 0)} survive. Registering
     * the convention trait def is what makes conventions available to
     * the planner; the individual conventions need no explicit
     * registration. The metadata provider is Calcite's default for now;
     * wiring Lance statistics into it is later work.
     */
    public RelOptCluster newCluster() {
        VolcanoPlanner planner = new VolcanoPlanner(costFactory, Contexts.empty());
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
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
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(new SqlTypeFactoryImpl(LanceTypeSystem.INSTANCE)));
        cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
        return cluster;
    }

    /**
     * Runs the Volcano planner over {@code logical} demanding
     * {@link LanceConvention} at the root and returns the best physical
     * plan. When no physical form exists (the Substrait producer
     * refused every candidate, so no rule fired), or when the planner
     * fails for any other reason, the logical plan itself is returned:
     * the caller reads the root's type to see whether anything was
     * pushed, and the fragment routing promises a Lucene aggregator
     * fallback for every plan it does not push, so a planner failure
     * must not surface as a request error.
     */
    public RelNode plan(RelNode logical) {
        VolcanoPlanner planner = (VolcanoPlanner) logical.getCluster().getPlanner();
        RelNode root = planner.changeTraits(logical, logical.getTraitSet().replace(LanceConvention.INSTANCE));
        planner.setRoot(root);
        try {
            return planner.findBestExp();
        } catch (RelOptPlanner.CannotPlanException nothingPushed) {
            return logical;
        } catch (RuntimeException plannerFailure) {
            LOGGER.debug(
                "lance.plan: Volcano planning failed, keeping the logical plan: {}: {}",
                plannerFailure.getClass().getName(),
                plannerFailure.getMessage()
            );
            return logical;
        }
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

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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.RelBuilder;

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
     * {@link LanceCostFactory}. The type factory uses
     * {@link LanceTypeSystem#INSTANCE} so timestamp precision above 3 and
     * {@code DECIMAL(20, 0)} survive. Registering the convention trait def
     * is what makes conventions available to the planner; the individual
     * conventions need no explicit registration. The metadata provider is
     * Calcite's default for now; Lance statistics replace it in a later
     * phase.
     */
    public RelOptCluster newCluster() {
        VolcanoPlanner planner = new VolcanoPlanner(costFactory, Contexts.empty());
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(new SqlTypeFactoryImpl(LanceTypeSystem.INSTANCE)));
        cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
        return cluster;
    }

    /** A fresh Hep planner over an empty program; rules come in later phases. */
    public HepPlanner newHepPlanner() {
        return new HepPlanner(new HepProgramBuilder().build());
    }

    /**
     * A builder whose {@code scan(SCHEMA_NAME, index)} resolves the index
     * name against the given schema and yields a
     * {@link org.opensearch.lance.plan.rel.LanceTableScan}, because
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

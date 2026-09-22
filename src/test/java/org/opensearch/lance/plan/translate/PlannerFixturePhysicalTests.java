/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.lance.plan.rel.PushedOperation;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs the Volcano planner with the pushdown rules over every
 * translation fixture and asserts it terminates with the expected
 * physical root: the scan carrying the pushed aggregate, for every
 * shape, because the producer covers everything the translator
 * accepts. The pushed bytes must deserialize to a Substrait plan whose
 * aggregate carries the measures and groupings the shape's specs
 * expand to.
 */
public class PlannerFixturePhysicalTests extends OpenSearchTestCase {

    private final String fixture;

    public PlannerFixturePhysicalTests(@Name("fixture") String fixture) {
        this.fixture = fixture;
    }

    @ParametersFactory
    public static Iterable<Object[]> fixtures() {
        List<Object[]> parameters = new ArrayList<>(AggregationToRelFixtureTests.FIXTURES.size());
        for (String name : AggregationToRelFixtureTests.FIXTURES) {
            parameters.add(new Object[] { name });
        }
        return parameters;
    }

    public void testVolcanoTerminatesWithThePushedScan() throws IOException {
        RelNode logical = PlanTestFixtures.translate(PlanTestFixtures.parse(body()));
        VolcanoPlanner planner = (VolcanoPlanner) logical.getCluster().getPlanner();
        RelNode root = planner.changeTraits(logical, logical.getTraitSet().replace(LanceConvention.INSTANCE));
        planner.setRoot(root);
        RelNode physical = planner.findBestExp();
        assertTrue("physical root of [" + fixture + "] is the scan: " + physical, physical instanceof LanceTableScan);
        PushedOperation.PushedAggregate pushed = ((LanceTableScan) physical).pushedAggregate().orElse(null);
        assertNotNull("the scan of [" + fixture + "] carries the pushed aggregate", pushed);
        assertEquals("row type of [" + fixture + "] survives the push", logical.getRowType(), physical.getRowType());

        AggregateRel rel = decode(pushed.substrait());
        int cardinalities = 0;
        int measures = 1; // count(*)
        for (MetricSpec spec : pushed.aggregate().metricSpecs()) {
            switch (spec.kind()) {
                case SUM, MIN, MAX, VALUE_COUNT -> measures += 1;
                case AVG, PERCENTILES, PERCENTILE_RANKS -> measures += 2;
                case STATS -> measures += 4;
                case EXTENDED_STATS -> measures += 5;
                case CARDINALITY -> cardinalities += 1;
            }
        }
        int groupings = pushed.aggregate().getGroupCount() + cardinalities;
        assertEquals("measures of [" + fixture + "]", measures, rel.getMeasuresCount());
        if (groupings == 0) {
            assertEquals("groupings of [" + fixture + "]", 0, rel.getGroupingsCount());
        } else {
            assertEquals("grouping lists of [" + fixture + "]", 1, rel.getGroupingsCount());
            assertEquals("grouping expressions of [" + fixture + "]", groupings, rel.getGroupings(0).getGroupingExpressionsCount());
        }
    }

    private static AggregateRel decode(ByteBuffer buffer) throws IOException {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        Plan plan = Plan.parseFrom(bytes);
        assertEquals(1, plan.getRelationsCount());
        Rel input = plan.getRelations(0).getRoot().getInput();
        assertTrue("the bytes decode to an aggregate", input.hasAggregate());
        return input.getAggregate();
    }

    private String body() throws IOException {
        String path = "/translate/aggregations/" + fixture + ".json";
        try (InputStream in = PlannerFixturePhysicalTests.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IOException("resource not found: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}

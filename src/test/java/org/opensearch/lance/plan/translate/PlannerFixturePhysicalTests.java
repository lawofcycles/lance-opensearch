/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rules.PushAggregateIntoLanceScan;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs the Volcano planner with the pushdown rules over every
 * translation fixture and asserts it terminates with the expected
 * physical root: the scan carrying the pushed aggregate, for every
 * shape, because the producer covers everything the translator
 * accepts.
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
        for (PushAggregateIntoLanceScan rule : PushAggregateIntoLanceScan.rules()) {
            planner.addRule(rule);
        }
        RelNode root = planner.changeTraits(logical, logical.getTraitSet().replace(LanceConvention.INSTANCE));
        planner.setRoot(root);
        RelNode physical = planner.findBestExp();
        assertTrue("physical root of [" + fixture + "] is the scan: " + physical, physical instanceof LanceTableScan);
        assertTrue("the scan of [" + fixture + "] carries the pushed aggregate", ((LanceTableScan) physical).pushedAggregate().isPresent());
        assertEquals("row type of [" + fixture + "] survives the push", logical.getRowType(), physical.getRowType());
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.PushedOperation.PushedTopK;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Runs the Volcano planner over translated hits requests and asserts
 * the whole hits plan (hit shape, top-k, query root) folds into the
 * scan: a sorted scalar page with and without a search_after cursor, a
 * score sorted full text page, and a page without a sort clause.
 */
public class HitsPlanPhysicalTests extends OpenSearchTestCase {

    private static RelNode physical(String body) throws IOException {
        RelNode logical = PlanTestFixtures.translate(PlanTestFixtures.parse(body));
        VolcanoPlanner planner = (VolcanoPlanner) logical.getCluster().getPlanner();
        RelNode root = planner.changeTraits(logical, logical.getTraitSet().replace(LanceConvention.INSTANCE));
        planner.setRoot(root);
        return planner.findBestExp();
    }

    private static LanceTableScan foldedScan(String body) throws IOException {
        RelNode physical = physical(body);
        assertTrue("physical root is the scan: " + physical, physical instanceof LanceTableScan);
        return (LanceTableScan) physical;
    }

    public void testSortedScalarPageFolds() throws IOException {
        LanceTableScan scan = foldedScan("{\"size\":5,\"query\":{\"term\":{\"category\":\"c0\"}},\"sort\":[{\"rating\":\"desc\"}]}");
        assertTrue(scan.pushedFilter().isPresent());
        PushedTopK pushed = scan.pushedTopK().orElseThrow();
        assertEquals(5, pushed.fetch());
        assertEquals("rating", pushed.toScanOrderings().get(0).getColumnName());
        assertNotNull("the fold carries the hit shape", pushed.hitShape());
    }

    public void testCursorPageFolds() throws IOException {
        LanceTableScan scan = foldedScan("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}],\"search_after\":[100]}");
        assertEquals("(rating > 100 OR rating IS NULL)", scan.pushedTopK().orElseThrow().cursorSql());
    }

    public void testScoreSortedFtsPageFolds() throws IOException {
        LanceTableScan scan = foldedScan(
            "{\"size\":3,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\"]}"
        );
        assertTrue(scan.pushedFts().isPresent());
        PushedTopK pushed = scan.pushedTopK().orElseThrow();
        assertTrue("a score page needs no orderings", pushed.toScanOrderings().isEmpty());
    }

    public void testSortlessPageFolds() throws IOException {
        LanceTableScan scan = foldedScan("{\"size\":10}");
        PushedTopK pushed = scan.pushedTopK().orElseThrow();
        assertEquals(10, pushed.fetch());
        assertTrue(pushed.toScanOrderings().isEmpty());
    }
}

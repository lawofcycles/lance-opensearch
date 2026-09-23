/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Locale;

/**
 * The fitted cost model through the Volcano planner: the same
 * aggregation tree answers with the Lucene operator on a four node
 * cluster reading a billion rows from S3 and with the pushed scan on a
 * single node reading twenty million rows from local disk, because the
 * inputs and the table statistics move the cost comparison. Also pins
 * that the hits shapes and the small tables keep the choices they had
 * before the fit.
 */
public class CostModelPlannerTests extends OpenSearchTestCase {

    private static final String TERMS_CATEGORY = "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}";
    private static final String SUM_PRICE = "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}";
    private static final String TERMS_RATING = "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"rating\"}}}}";
    private static final String SORTED_PAGE = "{\"size\":10,\"sort\":[{\"ts\":\"desc\"}]}";

    private static final CostInputs PERF1B_FOUR_NODES_S3 = CostInputs.forCluster(4, "s3://bench/perf1b.lance", 16, 8, 8);
    /** One 4xlarge node over local NVMe with the aggregator path collecting on one thread, the configuration the 20M rows were measured in. */
    private static final CostInputs PERF20M_ONE_NODE_LOCAL_UNSLICED = new CostInputs(1, StorageKind.LOCAL, 16, 8, 1);
    private static final CostInputs PERF20M_ONE_NODE_LOCAL = CostInputs.local(16);

    private static RelNode plan(LanceSchemas.IndexModel model, String body, CostInputs inputs) throws IOException {
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        return factory.plan(PerfTableFixture.translate(model, factory, body), inputs);
    }

    private static String explain(RelNode physical) {
        VolcanoPlanner planner = (VolcanoPlanner) physical.getCluster().getPlanner();
        RelOptCost cost = planner.getCost(physical, physical.getCluster().getMetadataQuery());
        return RelOptUtil.toString(physical) + "cost: " + cost;
    }

    public void testKeywordTermsOnABillionRowsOverS3AnswersWithTheLuceneOperator() throws IOException {
        RelNode physical = plan(PerfTableFixture.perf1b(), TERMS_CATEGORY, PERF1B_FOUR_NODES_S3);
        logger.info("explain terms(category) perf1b / 4 nodes / S3:\n{}", explain(physical));
        assertTrue("the aggregators win over S3: " + physical, physical instanceof LuceneAggregateExec);
        LuceneAggregateExec exec = (LuceneAggregateExec) physical;
        assertTrue(exec.getInput() instanceof LanceTableScan);
        assertTrue(((LanceTableScan) exec.getInput()).pushedOperations().isEmpty());
    }

    public void testKeywordTermsOnTwentyMillionRowsLocallyAnswersWithThePushedScan() throws IOException {
        RelNode physical = plan(PerfTableFixture.perf20m(), TERMS_CATEGORY, PERF20M_ONE_NODE_LOCAL_UNSLICED);
        logger.info("explain terms(category) perf20m / 1 node / local / slices 1:\n{}", explain(physical));
        assertTrue("the pushed scan wins locally: " + physical, physical instanceof LanceTableScan);
        assertTrue(((LanceTableScan) physical).pushedAggregate().isPresent());
    }

    public void testMetricOnlyOnABillionRowsOverS3AnswersWithTheLuceneOperator() throws IOException {
        RelNode physical = plan(PerfTableFixture.perf1b(), SUM_PRICE, PERF1B_FOUR_NODES_S3);
        logger.info("explain sum(price) perf1b / 4 nodes / S3:\n{}", explain(physical));
        assertTrue("the aggregators win over S3: " + physical, physical instanceof LuceneAggregateExec);
    }

    public void testMetricOnlyOnTwentyMillionRowsLocallyAnswersWithThePushedScan() throws IOException {
        RelNode physical = plan(PerfTableFixture.perf20m(), SUM_PRICE, PERF20M_ONE_NODE_LOCAL);
        logger.info("explain sum(price) perf20m / 1 node / local:\n{}", explain(physical));
        assertTrue("the pushed scan wins locally: " + physical, physical instanceof LanceTableScan);
        assertTrue(((LanceTableScan) physical).pushedAggregate().isPresent());
    }

    public void testNumericTermsStaysPushedOnBothClusters() throws IOException {
        // terms(rating) measured 346 ms pushed vs 780 ms on 4 nodes at 1B
        // and 37 ms vs 526 ms on one node at 20M: the numeric hash is
        // cheap inside the scan and dear in the aggregator.
        RelNode s3 = plan(PerfTableFixture.perf1b(), TERMS_RATING, PERF1B_FOUR_NODES_S3);
        assertTrue("pushed over S3: " + s3, s3 instanceof LanceTableScan && ((LanceTableScan) s3).pushedAggregate().isPresent());
        RelNode local = plan(PerfTableFixture.perf20m(), TERMS_RATING, PERF20M_ONE_NODE_LOCAL);
        assertTrue("pushed locally: " + local, local instanceof LanceTableScan && ((LanceTableScan) local).pushedAggregate().isPresent());
    }

    public void testTheSameTreeCostsDifferentlyUnderDifferentInputs() throws IOException {
        // The planner factory is shared and stateless: two runs over the
        // same logical shape with different inputs answer differently,
        // and the inputs of one run never leak into the other.
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        RelNode overS3 = factory.plan(PerfTableFixture.translate(PerfTableFixture.perf1b(), factory, TERMS_CATEGORY), PERF1B_FOUR_NODES_S3);
        RelNode localUnsliced = factory.plan(
            PerfTableFixture.translate(PerfTableFixture.perf1b(), factory, TERMS_CATEGORY),
            new CostInputs(1, StorageKind.LOCAL, 64, 32, 1)
        );
        assertTrue(overS3 instanceof LuceneAggregateExec);
        assertTrue(localUnsliced instanceof LanceTableScan);
    }

    public void testPlanWithoutInputsUsesTheLocalDefaults() throws IOException {
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        RelNode logical = PerfTableFixture.translate(PerfTableFixture.perf20m(), factory, SUM_PRICE);
        RelNode physical = factory.plan(logical);
        assertTrue("sum(price) on 20M local rows is pushed under the defaults: " + physical, physical instanceof LanceTableScan);
    }

    public void testSortedPageKeepsThePushedScanOnALargeTable() throws IOException {
        // No measurement compares the two hits forms, so their order is
        // the one the placeholders had: the pushed top-k scan ahead of
        // the heap collector, over S3 as well as locally.
        RelNode s3 = plan(PerfTableFixture.perf1b(), SORTED_PAGE, PERF1B_FOUR_NODES_S3);
        assertTrue("the pushed top-k wins over S3: " + s3, s3 instanceof LanceTableScan && ((LanceTableScan) s3).pushedTopK().isPresent());
        RelNode local = plan(PerfTableFixture.perf20m(), SORTED_PAGE, PERF20M_ONE_NODE_LOCAL);
        assertTrue("the pushed top-k wins locally: " + local, local instanceof LanceTableScan);
        assertFalse(local instanceof HeapTopKExec);
    }

    public void testSmallTableKeepsThePlaceholderChoicesWhateverTheInputs() throws IOException {
        // Six rows over S3 on four nodes: below the fitted range the
        // pushed form wins as before, for the shapes the model would
        // otherwise send to the aggregators.
        LanceSchemas.IndexModel tiny = PerfTableFixture.model("tiny", 6L, 1);
        for (String body : new String[] { TERMS_CATEGORY, SUM_PRICE }) {
            RelNode physical = plan(tiny, body, PERF1B_FOUR_NODES_S3);
            assertTrue(
                String.format(Locale.ROOT, "%s stays pushed on a tiny table: %s", body, physical),
                physical instanceof LanceTableScan
            );
            assertTrue(((LanceTableScan) physical).pushedAggregate().isPresent());
        }
    }
}

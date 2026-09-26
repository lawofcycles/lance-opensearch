/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
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

    private static final CostInputs PERF1B_FOUR_NODES_S3 = CostInputs.forCluster(
        4,
        "s3://bench/perf1b.lance",
        16,
        8,
        8,
        true,
        CostInputs.DEFAULT_MAX_GROUPS
    );
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
        // Every plan construction builds its own cluster and holder, so
        // two runs of the same factory over the same logical shape with
        // different inputs answer differently, and the inputs of one run
        // never leak into the other.
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

    public void testPushdownOffPricesThePushedScanAsInfiniteAtEverySize() throws IOException {
        // lance.aggregation.pushdown: false is a cost input: the pushed
        // scan costs infinity, so the Lucene operator wins for the
        // shapes the pushed scan otherwise takes (the unsliced local
        // node, where all three shapes measured faster pushed), on the
        // tiny table (placeholder regime) as on twenty million local
        // rows (fitted regime), and nothing is refused upstream of the
        // planner.
        LanceSchemas.IndexModel tiny = PerfTableFixture.model("tiny", 6L, 1);
        for (LanceSchemas.IndexModel model : List.of(tiny, PerfTableFixture.perf20m())) {
            for (String body : new String[] { TERMS_CATEGORY, SUM_PRICE, TERMS_RATING }) {
                RelNode off = plan(model, body, PERF20M_ONE_NODE_LOCAL_UNSLICED.withPushdownEnabled(false));
                assertTrue(
                    String.format(Locale.ROOT, "%s on %s goes to the aggregators with the pushdown off: %s", body, model.indexName(), off),
                    off instanceof LuceneAggregateExec
                );
                RelNode on = plan(model, body, PERF20M_ONE_NODE_LOCAL_UNSLICED);
                assertTrue(
                    String.format(Locale.ROOT, "%s on %s stays pushed with the pushdown on: %s", body, model.indexName(), on),
                    on instanceof LanceTableScan && ((LanceTableScan) on).pushedAggregate().isPresent()
                );
            }
        }
    }

    public void testGroupBoundPricesThePushedScanAsInfiniteWhenTheEstimateExceedsIt() throws IOException {
        // terms(category) estimates 200 groups from the bitmap index and
        // the count ordered top-k keeps 100 of them per scan (shard_size
        // 25, four times over); a bound of 99 sends it to the
        // aggregators and a bound of 100 keeps it pushed. sum(price) is
        // one group and stays pushed even under a bound of 1. Both
        // regimes, since the bound is read before the fitted /
        // placeholder split.
        LanceSchemas.IndexModel tiny = PerfTableFixture.model("tiny", 6L, 1);
        RelNode boundedTiny = plan(tiny, TERMS_CATEGORY, PERF20M_ONE_NODE_LOCAL_UNSLICED.withMaxGroups(5L));
        assertTrue("six rows cap the estimate at six, above a bound of five: " + boundedTiny, boundedTiny instanceof LuceneAggregateExec);
        RelNode bounded = plan(PerfTableFixture.perf20m(), TERMS_CATEGORY, PERF20M_ONE_NODE_LOCAL_UNSLICED.withMaxGroups(99L));
        assertTrue("100 retained groups exceed a bound of 99: " + bounded, bounded instanceof LuceneAggregateExec);
        RelNode atBound = plan(PerfTableFixture.perf20m(), TERMS_CATEGORY, PERF20M_ONE_NODE_LOCAL_UNSLICED.withMaxGroups(100L));
        assertTrue("100 retained groups fit a bound of 100: " + atBound, atBound instanceof LanceTableScan);
        for (LanceSchemas.IndexModel model : List.of(tiny, PerfTableFixture.perf20m())) {
            RelNode metric = plan(model, SUM_PRICE, PERF20M_ONE_NODE_LOCAL_UNSLICED.withMaxGroups(1L));
            assertTrue("one group fits a bound of one on " + model.indexName() + ": " + metric, metric instanceof LanceTableScan);
        }
    }

    public void testGroupBoundIsNotJudgedOnAGuessedDomain() throws IOException {
        // price has a BTree whose float bounds enclose no finite set of
        // values, so its domain is Calcite's share of the rows (two
        // million at 20M, a hundred million at 1B), which no default
        // bound would admit whatever the column holds. Such an estimate
        // is not judged against the bound: terms(price) stays pushed
        // where a numeric key measured faster, under the default bound
        // and under a bound of one alike, and the executor's bound from
        // the request shape stands alone. A nested tree over the same
        // key is priced by the fitted model (the merge of the guessed
        // groups sends it to the aggregators) and the bound does not
        // enter: the answer is the same under both bounds.
        String termsPrice = "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"price\"}}}}";
        for (CostInputs inputs : List.of(PERF20M_ONE_NODE_LOCAL_UNSLICED, PERF20M_ONE_NODE_LOCAL_UNSLICED.withMaxGroups(1L))) {
            RelNode single = plan(PerfTableFixture.perf20m(), termsPrice, inputs);
            assertTrue(termsPrice + " stays pushed under " + inputs + ": " + single, single instanceof LanceTableScan);
        }
        String nested =
            "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"p\":{\"terms\":{\"field\":\"price\"}}}}}}";
        RelNode nestedDefault = plan(PerfTableFixture.perf20m(), nested, PERF20M_ONE_NODE_LOCAL_UNSLICED);
        RelNode nestedTight = plan(PerfTableFixture.perf20m(), nested, PERF20M_ONE_NODE_LOCAL_UNSLICED.withMaxGroups(1L));
        assertEquals("the bound does not decide a guessed nested estimate", nestedDefault.getClass(), nestedTight.getClass());
    }

    public void testGroupBoundJudgesANestedTreeWhoseLevelsTheStatisticsBound() throws IOException {
        // terms(category) > terms(rating): 200 categories times the five
        // values the rating BTree bounds, a thousand groups no scan
        // cuts. A bound of 999 sends the tree to the aggregators, a
        // bound of 1000 keeps it pushed.
        String nested =
            "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\"}}}}}}";
        RelNode over = plan(PerfTableFixture.perf20m(), nested, PERF20M_ONE_NODE_LOCAL_UNSLICED.withMaxGroups(999L));
        assertTrue("1000 groups exceed a bound of 999: " + over, over instanceof LuceneAggregateExec);
        RelNode within = plan(PerfTableFixture.perf20m(), nested, PERF20M_ONE_NODE_LOCAL_UNSLICED.withMaxGroups(1000L));
        assertTrue("1000 groups fit a bound of 1000: " + within, within instanceof LanceTableScan);
    }

    public void testMetricLessDateHistogramOnTwentyMillionRowsIsPricedForTheAggregators() throws IOException {
        // date_histogram month with no metric under it, 20M rows on one
        // 4xlarge node with eight slices: the model prices the
        // aggregators at about 74 ms (fixed, one column, the date key
        // over 2.5 million rows per slice) and the pushed scan at about
        // 154 ms (fixed, decode, the date key over 2.5 million rows per
        // scan), so the tree goes to the aggregators. The shape measured
        // 148 ms pushed and 197 ms on the path this choice sends it to,
        // which no explain has confirmed; the pair is recorded in the
        // measurements and kept out of the fit until one does.
        String body = "{\"size\":0,\"aggs\":{\"h\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"}}}}";
        RelNode physical = plan(PerfTableFixture.perf20m(), body, PERF20M_ONE_NODE_LOCAL);
        logger.info("explain {} perf20m / 1 node / local / 8 slices:\n{}", body, explain(physical));
        assertTrue("the aggregators win: " + physical, physical instanceof LuceneAggregateExec);
        LuceneAggregateExec exec = (LuceneAggregateExec) physical;
        RelNode node = exec.aggregate().getInput();
        while (node instanceof Project || node instanceof Filter) {
            node = node.getInput(0);
        }
        AggregateProfile shape = AggregateProfile.of(exec.aggregate(), (LanceTableScan) node, physical.getCluster().getMetadataQuery());
        double lucene = CostModel.luceneAggregateMillis(PERF20M_ONE_NODE_LOCAL, shape);
        double pushed = CostModel.pushedAggregateMillis(PERF20M_ONE_NODE_LOCAL, shape);
        assertEquals(74.0, lucene, 1.0);
        assertEquals(154.0, pushed, 1.0);
    }

    public void testTwoLevelBucketTreesOverS3StayPushedOnFourNodes() throws IOException {
        // terms(category) > terms(rating) > avg(price) measured 1.69 s
        // pushed against 2.86 s through the aggregators on four nodes at
        // 1B, composite(category, rating) 1.11 s against 1.80 s, and
        // terms(rating) > max(id) + terms(category) 91 ms against 223 ms
        // on one node at 20M. The rating level is five values by its
        // BTree range, so the trees hold a thousand groups and neither
        // side pays the hash table penalty.
        String nestedAvg =
            "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":5},\"aggs\":{\"a\":{\"avg\":{\"field\":\"price\"}}}}}}}}";
        String composite =
            "{\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":10,\"sources\":[{\"cat\":{\"terms\":{\"field\":\"category\"}}},{\"rat\":{\"terms\":{\"field\":\"rating\"}}}]}}}}";
        for (String body : new String[] { nestedAvg, composite }) {
            RelNode physical = plan(PerfTableFixture.perf1b(), body, PERF1B_FOUR_NODES_S3);
            logger.info("explain {} perf1b / 4 nodes / S3:\n{}", body, explain(physical));
            assertTrue(
                body + " is pushed over S3 on four nodes: " + physical,
                physical instanceof LanceTableScan && ((LanceTableScan) physical).pushedAggregate().isPresent()
            );
        }
        String ratingThenCategory =
            "{\"size\":0,\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\"},\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}},\"c\":{\"terms\":{\"field\":\"category\",\"size\":2}}}}}}";
        RelNode local = plan(PerfTableFixture.perf20m(), ratingThenCategory, PERF20M_ONE_NODE_LOCAL);
        logger.info("explain {} perf20m / 1 node / local:\n{}", ratingThenCategory, explain(local));
        assertTrue(
            ratingThenCategory + " is pushed on one local node: " + local,
            local instanceof LanceTableScan && ((LanceTableScan) local).pushedAggregate().isPresent()
        );
    }

    public void testCardinalityLosesToTheAggregatorsInBothRegimes() throws IOException {
        // No rule refuses the cardinality any more: the pushed scan is
        // enumerated and loses on cost, through the fitted model on the
        // measured tables and through the placeholder penalty below the
        // fitted range, alone and under a bucket.
        String cardinality = "{\"size\":0,\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"user_id\"}}}}";
        String bucketed =
            "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"user_id\"}}}}}}";
        LanceSchemas.IndexModel tiny = PerfTableFixture.model("tiny", 6L, 1);
        for (LanceSchemas.IndexModel model : List.of(tiny, PerfTableFixture.perf20m(), PerfTableFixture.perf1b())) {
            for (CostInputs inputs : List.of(
                PERF20M_ONE_NODE_LOCAL,
                PERF1B_FOUR_NODES_S3,
                new CostInputs(1, StorageKind.LOCAL, 64, 32, 1)
            )) {
                for (String body : new String[] { cardinality, bucketed }) {
                    RelNode physical = plan(model, body, inputs);
                    assertTrue(
                        String.format(Locale.ROOT, "%s on %s under %s: %s", body, model.indexName(), inputs, explain(physical)),
                        physical instanceof LuceneAggregateExec
                    );
                }
            }
        }
    }
}

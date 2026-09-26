/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The fitted model against the measurements it was fitted to
 * ({@code cost/measurements.csv}): for every (shape, table, cluster)
 * measured on both paths the model must prefer the measured faster path
 * when the two differ by more than 1.3x, and the Java formulas must
 * reproduce the predictions the fit script printed into
 * {@code cost/fit-report.md}, so the report and the code cannot drift
 * apart.
 */
public class CostModelTests extends OpenSearchTestCase {

    /** Pairs closer than this are reported, not asserted: the measurement noise is of that order. */
    private static final double CHOICE_MARGIN = 1.3;

    private static final Pattern RESIDUAL_ROW = Pattern.compile(
        "^\\| (r\\d+) \\| (\\S+) \\| (\\S+) \\| (.+?) \\| (pushed|lucene) \\| (\\d+) \\| ([^|]+?) \\| ([^|]+?) \\| ([0-9.]+) \\|$"
    );

    public void testModelPrefersTheMeasuredFasterPathOnEveryClearPair() throws IOException {
        Map<String, List<CostMeasurements.Row>> pairs = new LinkedHashMap<>();
        for (CostMeasurements.Row row : CostMeasurements.load()) {
            if (row.excluded()) {
                continue;
            }
            pairs.computeIfAbsent(row.pairKey(), k -> new ArrayList<>()).add(row);
        }
        int asserted = 0;
        int reported = 0;
        List<String> disagreements = new ArrayList<>();
        for (List<CostMeasurements.Row> rows : pairs.values()) {
            CostMeasurements.Row pushed = null;
            List<CostMeasurements.Row> lucene = new ArrayList<>();
            for (CostMeasurements.Row row : rows) {
                if (row.pushed()) {
                    // The latest round's pushed measurement stands for the pair.
                    if (pushed == null || row.round().compareTo(pushed.round()) > 0) {
                        pushed = row;
                    }
                } else {
                    lucene.add(row);
                }
            }
            if (pushed == null || lucene.isEmpty()) {
                continue;
            }
            for (CostMeasurements.Row alternative : lucene) {
                double measuredPushed = pushed.latencyMillis();
                double measuredLucene = alternative.latencyMillis();
                double margin = Math.max(measuredPushed, measuredLucene) / Math.min(measuredPushed, measuredLucene);
                boolean measuredPushedWins = measuredPushed < measuredLucene;
                boolean modelPushedWins = pushed.predictedMillis() <= alternative.predictedMillis();
                if (margin <= CHOICE_MARGIN) {
                    reported++;
                    logger.info(
                        "inside the {}x band: {} (slices {}): measured pushed {} ms vs lucene {} ms, model {} ms vs {} ms",
                        CHOICE_MARGIN,
                        pushed.pairKey(),
                        alternative.slices(),
                        measuredPushed,
                        measuredLucene,
                        pushed.predictedMillis(),
                        alternative.predictedMillis()
                    );
                    continue;
                }
                asserted++;
                if (measuredPushedWins != modelPushedWins) {
                    disagreements.add(
                        String.format(
                            Locale.ROOT,
                            "%s (slices %d): measured pushed %.0f ms vs lucene %.0f ms, model pushed %.0f ms vs lucene %.0f ms",
                            pushed.pairKey(),
                            alternative.slices(),
                            measuredPushed,
                            measuredLucene,
                            pushed.predictedMillis(),
                            alternative.predictedMillis()
                        )
                    );
                }
            }
        }
        assertTrue("the CSV carries pairs to assert", asserted > 100);
        assertTrue("some pairs fall inside the band and are only reported", reported > 0);
        assertTrue("the model disagrees with the measurement on: " + disagreements, disagreements.isEmpty());
    }

    public void testJavaFormulasReproduceTheFitReport() throws IOException {
        Map<String, Double> reported = new LinkedHashMap<>();
        for (String line : CostMeasurements.readText("/cost/fit-report.md").split("\n")) {
            Matcher m = RESIDUAL_ROW.matcher(line);
            if (m.matches()) {
                reported.put(
                    m.group(1) + "|" + m.group(2) + "|" + m.group(3) + "|" + m.group(4) + "|" + m.group(5) + "|" + m.group(6),
                    parseMillis(m.group(8))
                );
            }
        }
        assertTrue("the report carries a residual table", reported.size() > 100);
        int compared = 0;
        for (CostMeasurements.Row row : CostMeasurements.load()) {
            if (row.excluded()) {
                continue;
            }
            String key = row.round()
                + "|"
                + row.table()
                + "|"
                + row.cluster()
                + "|"
                + row.shapeName()
                + "|"
                + (row.pushed() ? "pushed" : "lucene")
                + "|"
                + row.slices();
            Double expected = reported.get(key);
            assertNotNull("the report has a residual row for " + key, expected);
            double predicted = row.predictedMillis();
            // The report prints whole milliseconds below ten seconds and
            // hundredths of a second above, so allow that rounding.
            double tolerance = expected >= 10_000 ? 5.0 : 0.5;
            assertEquals("prediction for " + key, expected, predicted, tolerance + expected * 1e-6);
            compared++;
        }
        assertEquals("every fitted row is compared", reported.size(), compared);
    }

    public void testSmallTablesStayOnThePlaceholderCosts() {
        assertFalse(CostModel.usesFittedModel(6));
        assertFalse(CostModel.usesFittedModel(999_999));
        assertTrue(CostModel.usesFittedModel(1_000_000));
        assertTrue(CostModel.usesFittedModel(20_000_000));
    }

    public void testObjectStoreInvertsTheKeywordTermsChoiceAtOneBillionRows() {
        // terms(category) on perf1b: 4 nodes reading S3 prefer the
        // aggregators (measured 848 ms pushed vs 346 ms), one 16xlarge
        // node reading NVMe with the slicing switched off prefers the
        // pushed scan (measured 716 ms vs 8.86 s).
        AggregateProfile terms = new AggregateProfile(
            1e9,
            200,
            100,
            true,
            1,
            2,
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            false,
            0,
            false,
            false,
            false,
            0,
            1.0
        );
        CostInputs s3 = new CostInputs(4, StorageKind.OBJECT_STORE, 16, 8, 8);
        assertTrue(CostModel.luceneAggregateMillis(s3, terms) < CostModel.pushedAggregateMillis(s3, terms));
        CostInputs nvmeUnsliced = new CostInputs(1, StorageKind.LOCAL, 64, 32, 1);
        assertTrue(CostModel.pushedAggregateMillis(nvmeUnsliced, terms) < CostModel.luceneAggregateMillis(nvmeUnsliced, terms));
    }

    public void testObjectStoreInvertsTheMetricOnlyChoiceAtOneBillionRows() {
        // sum(price): 4 nodes on S3 measured 784 ms pushed vs 442 ms;
        // 20M rows on one local node measured 30 ms vs 255 ms.
        AggregateProfile sum = new AggregateProfile(1e9, 1, 1, true, 1, 8, 1, 0, 0, 0, 0, 0, 0, false, 1, false, false, false, 0, 1.0);
        CostInputs s3 = new CostInputs(4, StorageKind.OBJECT_STORE, 16, 8, 8);
        assertTrue(CostModel.luceneAggregateMillis(s3, sum) < CostModel.pushedAggregateMillis(s3, sum));
        AggregateProfile small = new AggregateProfile(2e7, 1, 1, true, 1, 8, 1, 0, 0, 0, 0, 0, 0, false, 1, false, false, false, 0, 1.0);
        CostInputs local = CostInputs.local(16);
        assertTrue(CostModel.pushedAggregateMillis(local, small) < CostModel.luceneAggregateMillis(local, small));
    }

    public void testMetricUnderATermsKeyStaysPushedOnTwentyMillionSlicedRows() {
        // terms(category) + avg(rating) on 20M rows, one 4xlarge node
        // with eight slices: measured 70 ms pushed vs 201 ms, where a
        // bare terms(category) measured 67 ms vs 38 ms. The metric under
        // the bucket is what turns the choice.
        AggregateProfile bare = new AggregateProfile(2e7, 200, 100, true, 1, 2, 1, 1, 0, 0, 0, 0, 0, false, 0, false, false, false, 0, 1.0);
        AggregateProfile withAvg = new AggregateProfile(
            2e7,
            200,
            68,
            true,
            2,
            6,
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            false,
            1,
            false,
            false,
            false,
            0,
            1.0
        );
        CostInputs local = CostInputs.local(16);
        assertTrue(CostModel.luceneAggregateMillis(local, bare) < CostModel.pushedAggregateMillis(local, bare));
        assertTrue(CostModel.pushedAggregateMillis(local, withAvg) < CostModel.luceneAggregateMillis(local, withAvg));
        // The metric under the key is charged per bucket on top of the
        // metric itself and its column: the Lucene side moves by at
        // least the bucket metric term over the rows one slice collects.
        double mrowThread = 2e7 / local.slices() / 1e6;
        double added = CostModel.luceneAggregateMillis(local, withAvg) - CostModel.luceneAggregateMillis(local, bare);
        assertTrue(
            "the avg under the terms adds " + added + " ms on the Lucene side",
            added >= CostCoefficients.LUCENE_BUCKET_METRIC_MS_PER_MROW_THREAD * mrowThread
        );
        // The same metric with no bucket key above it pays the metric term alone.
        AggregateProfile avgAlone = new AggregateProfile(2e7, 1, 1, true, 1, 4, 1, 0, 0, 0, 0, 0, 0, false, 1, false, false, false, 0, 1.0);
        AggregateProfile nothing = new AggregateProfile(2e7, 1, 1, true, 1, 4, 1, 0, 0, 0, 0, 0, 0, false, 0, false, false, false, 0, 1.0);
        double alone = CostModel.luceneAggregateMillis(local, avgAlone) - CostModel.luceneAggregateMillis(local, nothing);
        assertEquals(CostCoefficients.LUCENE_SIMPLE_METRIC_MS_PER_MROW_THREAD * mrowThread, alone, 1e-9);
    }

    public void testTwoLevelBucketTreeOverS3StaysPushedWhenItsGroupsAreBounded() {
        // terms(category) > terms(rating) > avg(price) on perf1b, 4
        // nodes on S3, 8 slices: measured 1.69 s pushed vs 2.86 s. The
        // tree holds 200 x 5 groups when the rating BTree's range bounds
        // the second level; with the level guessed as a tenth of the
        // rows the product is capped at the billion rows and both sides
        // pay the hash table penalty, the pushed side its merge of a
        // node's share of them as well, which is what kept the tree on
        // the aggregators.
        AggregateProfile bounded = nestedTermsAvg(1000, true);
        AggregateProfile guessed = nestedTermsAvg(1e9, false);
        CostInputs s3 = new CostInputs(4, StorageKind.OBJECT_STORE, 16, 8, 8);
        assertTrue(CostModel.pushedAggregateMillis(s3, bounded) < CostModel.luceneAggregateMillis(s3, bounded));
        assertTrue(CostModel.luceneAggregateMillis(s3, guessed) < CostModel.pushedAggregateMillis(s3, guessed));
        // The guess adds exactly the two group terms to the pushed side:
        // the per thread hash table misses over the rows one scan
        // aggregates, and the merge of a node's row share of group rows
        // by each of the node's scans; the Lucene side gains its per
        // node hash table misses alone.
        double rowsPerNode = 1e9 / s3.nodes();
        double mrowThread = rowsPerNode / s3.pushdownParallelism() / 1e6;
        double pushedAdded = CostModel.pushedAggregateMillis(s3, guessed) - CostModel.pushedAggregateMillis(s3, bounded);
        double expectedPushed = CostCoefficients.PUSHED_LARGE_GROUPS_MS_PER_MROW_THREAD * mrowThread
            + CostCoefficients.PUSHED_MERGE_MS_PER_MGROUP * (rowsPerNode - 1000) / 1e6 * s3.pushdownParallelism();
        assertEquals("the guessed groups add " + pushedAdded + " ms to the pushed scan", expectedPushed, pushedAdded, 1e-6);
        Map<String, Double> pushedTerms = CostModel.pushedAggregateTerms(s3, guessed);
        assertEquals(
            CostCoefficients.PUSHED_MERGE_MS_PER_MGROUP * rowsPerNode / 1e6 * s3.pushdownParallelism(),
            pushedTerms.get("PUSHED_MERGE_MS_PER_MGROUP"),
            1e-6
        );
        assertTrue(
            "the merge term alone outweighs the whole Lucene side",
            pushedTerms.get("PUSHED_MERGE_MS_PER_MGROUP") > CostModel.luceneAggregateMillis(s3, guessed)
        );
        double luceneAdded = CostModel.luceneAggregateMillis(s3, guessed) - CostModel.luceneAggregateMillis(s3, bounded);
        assertEquals(
            "the guessed groups add " + luceneAdded + " ms to the aggregators",
            CostCoefficients.LUCENE_LARGE_GROUPS_MS_PER_MROW_NODE * rowsPerNode / 1e6,
            luceneAdded,
            1e-6
        );
        // composite(category, rating) size 10 on the same cluster:
        // measured 1.11 s pushed vs 1.80 s.
        AggregateProfile composite = new AggregateProfile(
            1e9,
            1000,
            1000,
            true,
            2,
            6,
            1,
            1,
            1,
            0,
            0,
            0,
            0,
            true,
            0,
            false,
            false,
            false,
            0,
            1.0
        );
        assertTrue(CostModel.pushedAggregateMillis(s3, composite) < CostModel.luceneAggregateMillis(s3, composite));
        // terms(rating) > max(id) + terms(category) size 2 on 20M rows,
        // one 4xlarge node with eight slices: measured 91 ms pushed vs
        // 223 ms; guessed, the same tree goes to the aggregators.
        CostInputs local = CostInputs.local(16);
        AggregateProfile small = new AggregateProfile(
            2e7,
            1000,
            1000,
            true,
            3,
            14,
            1,
            1,
            1,
            0,
            0,
            0,
            0,
            false,
            1,
            false,
            false,
            false,
            0,
            1.0
        );
        AggregateProfile smallGuessed = new AggregateProfile(
            2e7,
            2e7,
            2e7,
            false,
            3,
            14,
            1,
            1,
            1,
            0,
            0,
            0,
            0,
            false,
            1,
            false,
            false,
            false,
            0,
            1.0
        );
        assertTrue(CostModel.pushedAggregateMillis(local, small) < CostModel.luceneAggregateMillis(local, small));
        assertTrue(CostModel.luceneAggregateMillis(local, smallGuessed) < CostModel.pushedAggregateMillis(local, smallGuessed));
    }

    private static AggregateProfile nestedTermsAvg(double groups, boolean known) {
        return new AggregateProfile(1e9, groups, groups, known, 3, 14, 1, 1, 1, 0, 0, 0, 0, false, 1, false, false, false, 0, 1.0);
    }

    public void testTermBreakdownSumsToThePrediction() {
        AggregateProfile shape = nestedTermsAvg(1000, true);
        for (CostInputs inputs : List.of(new CostInputs(4, StorageKind.OBJECT_STORE, 16, 8, 8), CostInputs.local(16))) {
            double pushed = 0.0;
            for (double term : CostModel.pushedAggregateTerms(inputs, shape).values()) {
                pushed += term;
            }
            assertEquals(CostModel.pushedAggregateMillis(inputs, shape), pushed, 1e-9);
            double lucene = 0.0;
            for (double term : CostModel.luceneAggregateTerms(inputs, shape).values()) {
                lucene += term;
            }
            assertEquals(CostModel.luceneAggregateMillis(inputs, shape), lucene, 1e-9);
        }
        // Every fitted coefficient of a side is a term of that side.
        Map<String, Double> pushedTerms = CostModel.pushedAggregateTerms(CostInputs.local(16), shape);
        Map<String, Double> luceneTerms = CostModel.luceneAggregateTerms(CostInputs.local(16), shape);
        for (String name : CostCoefficients.fitted().keySet()) {
            boolean pushedSide = name.startsWith("PUSHED_") || name.startsWith("OBJECT_STORE_");
            boolean luceneSide = name.startsWith("LUCENE_") || name.equals("OBJECT_STORE_OPEN_MS");
            assertEquals(name + " on the pushed side", pushedSide, pushedTerms.containsKey(name));
            assertEquals(name + " on the Lucene side", luceneSide, luceneTerms.containsKey(name));
        }
    }

    public void testTenMillionDistinctTermsStayPushedOverS3WithSlices() {
        // terms(user_id) size 10 on perf1b, 4 nodes on S3, 8 slices:
        // measured 4.82 s pushed vs 22.7 s; 6 nodes 3.22 s vs 15.1 s.
        AggregateProfile terms = new AggregateProfile(
            1e9,
            1e7,
            100,
            true,
            1,
            16,
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            false,
            0,
            false,
            false,
            false,
            0,
            1.0
        );
        for (int nodes : new int[] { 4, 6 }) {
            CostInputs s3 = new CostInputs(nodes, StorageKind.OBJECT_STORE, 16, 8, 8);
            assertTrue(nodes + " nodes", CostModel.pushedAggregateMillis(s3, terms) < CostModel.luceneAggregateMillis(s3, terms));
        }
    }

    public void testFilteredAggregateOverS3StaysPushedOnFourAndSixNodes() {
        // filter rating=5 + terms(category) on perf1b: measured 4.26 s
        // pushed vs 5.58 s on 4 nodes, 3.90 s vs 5.27 s on 6 nodes.
        AggregateProfile filtered = new AggregateProfile(
            1e9,
            200,
            100,
            true,
            2,
            6,
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            false,
            0,
            false,
            false,
            false,
            0,
            0.2
        );
        for (int nodes : new int[] { 4, 6 }) {
            CostInputs s3 = new CostInputs(nodes, StorageKind.OBJECT_STORE, 16, 8, 8);
            assertTrue(nodes + " nodes", CostModel.pushedAggregateMillis(s3, filtered) < CostModel.luceneAggregateMillis(s3, filtered));
        }
        // The row address set of the filter is charged per matching row
        // of the whole table, so raising the selectivity from 5 % to
        // 100 % on one node adds at least that term over the 950
        // million rows that join the match, on top of the per thread
        // string key the same rows pay.
        CostInputs oneNode = new CostInputs(1, StorageKind.OBJECT_STORE, 16, 8, 8);
        AggregateProfile sparse = withSelectivity(filtered, 0.05);
        AggregateProfile dense = withSelectivity(filtered, 0.999);
        double added = CostModel.pushedAggregateMillis(oneNode, dense) - CostModel.pushedAggregateMillis(oneNode, sparse);
        double matchingMrows = 1e9 * (0.999 - 0.05) / 1e6;
        double keyPerThread = CostCoefficients.PUSHED_STRING_KEY_MS_PER_MROW_THREAD * matchingMrows / oneNode.pushdownParallelism();
        assertEquals(
            "the matching rows add " + added + " ms to the pushed scan",
            CostCoefficients.PUSHED_FILTER_MATCH_MS_PER_MROW * matchingMrows,
            added - keyPerThread,
            1e-6
        );
        // Six nodes hold a sixth of the rows each but the same match set:
        // the term does not shrink with the node count.
        CostInputs sixNodes = new CostInputs(6, StorageKind.OBJECT_STORE, 16, 8, 8);
        double addedOnSix = CostModel.pushedAggregateMillis(sixNodes, dense) - CostModel.pushedAggregateMillis(sixNodes, sparse);
        assertTrue(addedOnSix >= CostCoefficients.PUSHED_FILTER_MATCH_MS_PER_MROW * matchingMrows);
    }

    private static AggregateProfile withSelectivity(AggregateProfile p, double selectivity) {
        return new AggregateProfile(
            p.tableRows(),
            p.groups(),
            p.mergedGroups(),
            p.groupsKnown(),
            p.columnsRead(),
            p.bytesPerRow(),
            p.scanPasses(),
            p.stringKeys(),
            p.numericKeys(),
            p.dateKeys(),
            p.rangeKeys(),
            p.filterKeys(),
            p.compositeDateKeys(),
            p.composite(),
            p.simpleMetrics(),
            p.extendedStats(),
            p.percentiles(),
            p.cardinality(),
            p.cardinalityDistinct(),
            selectivity
        );
    }

    public void testCardinalityStaysOnTheAggregators() {
        // cardinality(user_id) measured 22.6 s pushed vs 4.03 s on 4
        // nodes at 1B, and 14.1 s vs 446 ms on one node at 20M.
        AggregateProfile card = new AggregateProfile(1e9, 1, 1, true, 1, 8, 1, 0, 0, 0, 0, 0, 0, false, 0, false, false, true, 1e7, 1.0);
        CostInputs s3 = new CostInputs(4, StorageKind.OBJECT_STORE, 16, 8, 8);
        assertTrue(CostModel.luceneAggregateMillis(s3, card) < CostModel.pushedAggregateMillis(s3, card));
        AggregateProfile small = new AggregateProfile(2e7, 1, 1, true, 1, 8, 1, 0, 0, 0, 0, 0, 0, false, 0, false, false, true, 1e7, 1.0);
        CostInputs local = CostInputs.local(16);
        assertTrue(CostModel.luceneAggregateMillis(local, small) < CostModel.pushedAggregateMillis(local, small));
        // cardinality(category), 200 distinct values: measured 71 ms
        // pushed vs 35 ms at 20M. The aggregator collects ordinals
        // without hashing a row, the pushed scan hashes every row.
        AggregateProfile few = new AggregateProfile(2e7, 1, 1, true, 1, 2, 1, 0, 0, 0, 0, 0, 0, false, 0, false, false, true, 200, 1.0);
        assertTrue(CostModel.luceneAggregateMillis(local, few) < CostModel.pushedAggregateMillis(local, few));
    }

    public void testEveryMeasuredCardinalityShapePrefersTheAggregators() throws IOException {
        // The pushdown rule no longer refuses a cardinality metric; the
        // fitted model is what keeps such trees on the aggregators, so
        // every (table, cluster) the CSV measured a cardinality shape on
        // must price the Lucene form below the pushed form.
        int checked = 0;
        for (CostMeasurements.Row row : CostMeasurements.load()) {
            if (!row.shape().cardinality()) {
                continue;
            }
            double pushed = CostModel.pushedAggregateMillis(row.inputs(), row.shape());
            double lucene = CostModel.luceneAggregateMillis(row.inputs(), row.shape());
            assertTrue(
                String.format(
                    Locale.ROOT,
                    "%s (slices %d): model pushed %.0f ms vs lucene %.0f ms",
                    row.pairKey(),
                    row.slices(),
                    pushed,
                    lucene
                ),
                lucene < pushed
            );
            checked++;
        }
        assertTrue("the CSV carries cardinality rows", checked > 0);
    }

    public void testPercentilesStayPushed() {
        // percentiles(price) measured 2.04 s pushed vs 3.35 s on 4 nodes
        // at 1B, and 1.02 s vs 3.60 s on the 16xlarge with 32 slices.
        AggregateProfile percentiles = new AggregateProfile(
            1e9,
            1,
            1,
            true,
            1,
            8,
            2,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
            0,
            false,
            true,
            false,
            0,
            1.0
        );
        CostInputs s3 = new CostInputs(4, StorageKind.OBJECT_STORE, 16, 8, 8);
        assertTrue(CostModel.pushedAggregateMillis(s3, percentiles) < CostModel.luceneAggregateMillis(s3, percentiles));
        CostInputs xl = CostInputs.local(64);
        assertTrue(CostModel.pushedAggregateMillis(xl, percentiles) < CostModel.luceneAggregateMillis(xl, percentiles));
    }

    private static double parseMillis(String text) {
        String trimmed = text.trim();
        if (trimmed.endsWith(" s")) {
            return Double.parseDouble(trimmed.substring(0, trimmed.length() - 2)) * 1000.0;
        }
        return Double.parseDouble(trimmed.substring(0, trimmed.length() - 3));
    }
}

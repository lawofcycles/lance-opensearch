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
        AggregateProfile terms = new AggregateProfile(1e9, 200, 100, 1, 2, 1, 1, 0, 0, 0, 0, 0, false, 0, false, false, false, 0, 1.0);
        CostInputs s3 = new CostInputs(4, StorageKind.OBJECT_STORE, 16, 8, 8);
        assertTrue(CostModel.luceneAggregateMillis(s3, terms) < CostModel.pushedAggregateMillis(s3, terms));
        CostInputs nvmeUnsliced = new CostInputs(1, StorageKind.LOCAL, 64, 32, 1);
        assertTrue(CostModel.pushedAggregateMillis(nvmeUnsliced, terms) < CostModel.luceneAggregateMillis(nvmeUnsliced, terms));
    }

    public void testObjectStoreInvertsTheMetricOnlyChoiceAtOneBillionRows() {
        // sum(price): 4 nodes on S3 measured 784 ms pushed vs 442 ms;
        // 20M rows on one local node measured 30 ms vs 255 ms.
        AggregateProfile sum = new AggregateProfile(1e9, 1, 1, 1, 8, 1, 0, 0, 0, 0, 0, 0, false, 1, false, false, false, 0, 1.0);
        CostInputs s3 = new CostInputs(4, StorageKind.OBJECT_STORE, 16, 8, 8);
        assertTrue(CostModel.luceneAggregateMillis(s3, sum) < CostModel.pushedAggregateMillis(s3, sum));
        AggregateProfile small = new AggregateProfile(2e7, 1, 1, 1, 8, 1, 0, 0, 0, 0, 0, 0, false, 1, false, false, false, 0, 1.0);
        CostInputs local = CostInputs.local(16);
        assertTrue(CostModel.pushedAggregateMillis(local, small) < CostModel.luceneAggregateMillis(local, small));
    }

    public void testCardinalityStaysOnTheAggregators() {
        // cardinality(user_id) measured 22.6 s pushed vs 4.03 s on 4
        // nodes at 1B, and 14.1 s vs 446 ms on one node at 20M.
        AggregateProfile card = new AggregateProfile(1e9, 1, 1, 1, 8, 1, 0, 0, 0, 0, 0, 0, false, 0, false, false, true, 1e7, 1.0);
        CostInputs s3 = new CostInputs(4, StorageKind.OBJECT_STORE, 16, 8, 8);
        assertTrue(CostModel.luceneAggregateMillis(s3, card) < CostModel.pushedAggregateMillis(s3, card));
        AggregateProfile small = new AggregateProfile(2e7, 1, 1, 1, 8, 1, 0, 0, 0, 0, 0, 0, false, 0, false, false, true, 1e7, 1.0);
        CostInputs local = CostInputs.local(16);
        assertTrue(CostModel.luceneAggregateMillis(local, small) < CostModel.pushedAggregateMillis(local, small));
    }

    public void testPercentilesStayPushed() {
        // percentiles(price) measured 2.04 s pushed vs 3.35 s on 4 nodes
        // at 1B, and 1.02 s vs 3.60 s on the 16xlarge with 32 slices.
        AggregateProfile percentiles = new AggregateProfile(1e9, 1, 1, 1, 8, 2, 0, 0, 0, 0, 0, 0, false, 0, false, true, false, 0, 1.0);
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

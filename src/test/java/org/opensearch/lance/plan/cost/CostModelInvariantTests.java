/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Properties of the fitted cost model that hold whatever the fitted
 * values are, checked over a systematic grid of shapes and clusters
 * rather than over the measured rows of {@code cost/measurements.csv}.
 * {@link CostModelTests} asserts the model's choice on the pairs the CSV
 * carries; a coefficient that is wrong on a shape nobody has measured
 * passes there until the shape is measured and added. Here every point
 * of the grid must give finite, non negative costs that grow with the
 * work (rows, groups, columns, bucket levels), shrink or stay with more
 * nodes, pay for an object store, price a string key above a numeric
 * key on the pushed scan, and keep the choice under a one percent move
 * of any continuous parameter when the two sides are outside the
 * margin {@link CostModelTests} treats as noise.
 *
 * <p>The grid varies the data nodes, the storage, the table rows, the
 * groups, the string and numeric terms keys, the simple metrics, the
 * depth of the bucket tree and the filter selectivity; the parallelism
 * settings stay at the 4xlarge defaults the measured clusters ran with.
 * The bucket tree's depth is filled with date histogram levels below
 * the terms keys, so a point with more terms keys than depth is as deep
 * as its keys. Every point reads one column per key and per metric plus
 * one for the filter, dictionary width for a string key and eight bytes
 * for the rest, the same widths {@link AggregateProfile#of} assumes.
 */
public class CostModelInvariantTests extends OpenSearchTestCase {

    /** Pairs closer than this are noise, the same band {@link CostModelTests} reports rather than asserts. */
    private static final double CHOICE_MARGIN = 1.3;

    private static final int[] NODES = { 1, 3, 4, 6, 8 };
    private static final StorageKind[] STORAGE = { StorageKind.LOCAL, StorageKind.OBJECT_STORE };
    private static final double[] ROWS = { 2e7, 1e9, 1e10 };
    private static final double[] GROUPS = { 1, 10, 1e3, 1e5, 1e7, 1e9 };
    private static final int[] STRING_KEYS = { 0, 1, 2 };
    private static final int[] NUMERIC_KEYS = { 0, 1, 2 };
    private static final int[] METRICS = { 0, 1, 3 };
    private static final int[] LEVELS = { 1, 2, 3 };
    private static final double[] SELECTIVITY = { 0.001, 0.1, 1.0 };

    /** One 4xlarge data node: 16 CPUs, both parallelism settings at their default of half the CPUs. */
    private static final int CPUS = 16;
    private static final int PARALLELISM = 8;
    private static final int SLICES = 8;

    private static final double NUMERIC_BYTES = 8;
    /** Relative slack for comparing two sums of doubles that should be ordered or equal. */
    private static final double SLACK = 1e-9;
    /** Violations listed in a failure message before the rest are counted. */
    private static final int REPORTED_VIOLATIONS = 20;

    /** One point of the grid: the cluster and the shape's free parameters. */
    record Point(int nodes, StorageKind storage, double rows, double groups, int stringKeys, int numericKeys, int metrics, int levels,
        double selectivity) {

        CostInputs inputs() {
            return new CostInputs(nodes, storage, CPUS, PARALLELISM, SLICES);
        }

        /** Date histogram levels that fill the bucket tree below the terms keys up to {@code levels}. */
        int dateKeys() {
            return Math.max(0, levels - stringKeys - numericKeys);
        }

        boolean filtered() {
            return selectivity < 1.0;
        }

        AggregateProfile profile() {
            int filterColumns = filtered() ? 1 : 0;
            int numericColumns = numericKeys + dateKeys() + metrics + filterColumns;
            return new AggregateProfile(
                rows,
                Math.min(groups, rows),
                Math.min(groups, rows),
                true,
                stringKeys + numericColumns,
                stringKeys * CostCoefficients.DICTIONARY_STRING_BYTES_PER_ROW + numericColumns * NUMERIC_BYTES,
                1,
                stringKeys,
                numericKeys,
                dateKeys(),
                0,
                0,
                0,
                false,
                metrics,
                false,
                false,
                false,
                0,
                selectivity
            );
        }

        Point withNodes(int n) {
            return new Point(n, storage, rows, groups, stringKeys, numericKeys, metrics, levels, selectivity);
        }

        Point withStorage(StorageKind kind) {
            return new Point(nodes, kind, rows, groups, stringKeys, numericKeys, metrics, levels, selectivity);
        }

        Point withRows(double r) {
            return new Point(nodes, storage, r, groups, stringKeys, numericKeys, metrics, levels, selectivity);
        }

        Point withGroups(double g) {
            return new Point(nodes, storage, rows, g, stringKeys, numericKeys, metrics, levels, selectivity);
        }

        Point withLevels(int l) {
            return new Point(nodes, storage, rows, groups, stringKeys, numericKeys, metrics, l, selectivity);
        }

        /** The same point with its single string key replaced by a numeric one. */
        Point withNumericTwin() {
            return new Point(nodes, storage, rows, groups, 0, 1, metrics, levels, selectivity);
        }

        double pushed() {
            return CostModel.pushedAggregateMillis(inputs(), profile());
        }

        double lucene() {
            return CostModel.luceneAggregateMillis(inputs(), profile());
        }
    }

    public void testEveryTermIsFiniteAndNonNegativeOnEveryPoint() {
        List<String> violations = new ArrayList<>();
        int[] points = { 0 };
        forEachPoint(point -> {
            points[0]++;
            CostInputs inputs = point.inputs();
            AggregateProfile shape = point.profile();
            for (Map.Entry<String, Double> term : CostModel.pushedAggregateTerms(inputs, shape).entrySet()) {
                if (!(term.getValue() >= 0.0) || !Double.isFinite(term.getValue())) {
                    violations.add("pushed " + term.getKey() + " = " + term.getValue() + " at " + point);
                }
            }
            for (Map.Entry<String, Double> term : CostModel.luceneAggregateTerms(inputs, shape).entrySet()) {
                if (!(term.getValue() >= 0.0) || !Double.isFinite(term.getValue())) {
                    violations.add("lucene " + term.getKey() + " = " + term.getValue() + " at " + point);
                }
            }
            double pushed = point.pushed();
            double lucene = point.lucene();
            if (!(pushed >= 0.0) || !Double.isFinite(pushed) || !(lucene >= 0.0) || !Double.isFinite(lucene)) {
                violations.add("pushed " + pushed + " ms, lucene " + lucene + " ms at " + point);
            }
        });
        assertTrue("the grid has points", points[0] > 10_000);
        assertNoViolations("a term or a sum is negative, infinite or NaN", violations);
    }

    public void testMoreGroupsNeverLowerEitherCost() {
        List<String> violations = new ArrayList<>();
        forEachPoint(point -> {
            for (int i = 0; i + 1 < GROUPS.length; i++) {
                if (point.groups() == GROUPS[i]) {
                    assertNotLower("groups", point, point.withGroups(GROUPS[i + 1]), violations);
                }
            }
        });
        assertNoViolations("more groups lowered a cost", violations);
    }

    public void testMoreRowsNeverLowerEitherCost() {
        List<String> violations = new ArrayList<>();
        forEachPoint(point -> {
            for (int i = 0; i + 1 < ROWS.length; i++) {
                if (point.rows() == ROWS[i]) {
                    assertNotLower("rows", point, point.withRows(ROWS[i + 1]), violations);
                }
            }
        });
        assertNoViolations("more rows lowered a cost", violations);
    }

    public void testReadingAnotherColumnNeverLowersEitherCost() {
        List<String> violations = new ArrayList<>();
        forEachPoint(point -> {
            CostInputs inputs = point.inputs();
            AggregateProfile shape = point.profile();
            AggregateProfile wider = withColumns(shape, shape.columnsRead() + 1, shape.bytesPerRow() + NUMERIC_BYTES);
            double pushed = CostModel.pushedAggregateMillis(inputs, shape);
            double pushedWider = CostModel.pushedAggregateMillis(inputs, wider);
            double lucene = CostModel.luceneAggregateMillis(inputs, shape);
            double luceneWider = CostModel.luceneAggregateMillis(inputs, wider);
            if (lower(pushedWider, pushed)) {
                violations.add(format("pushed %.3f ms falls to %.3f ms with one more column at %s", pushed, pushedWider, point));
            }
            if (lower(luceneWider, lucene)) {
                violations.add(format("lucene %.3f ms falls to %.3f ms with one more column at %s", lucene, luceneWider, point));
            }
        });
        assertNoViolations("one more column lowered a cost", violations);
    }

    public void testADeeperBucketTreeNeverLowersEitherCost() {
        List<String> violations = new ArrayList<>();
        forEachPoint(point -> {
            for (int i = 0; i + 1 < LEVELS.length; i++) {
                if (point.levels() == LEVELS[i]) {
                    assertNotLower("levels", point, point.withLevels(LEVELS[i + 1]), violations);
                }
            }
        });
        assertNoViolations("a deeper bucket tree lowered a cost", violations);
    }

    public void testMoreNodesNeverRaiseEitherCost() {
        // Every per thread and per node term shrinks with the node
        // count and the fixed terms, the object store open and the
        // filter's table wide match set stay; the merge and the sketch
        // are bounded by the node's row share, which shrinks too. The
        // fan out floor fitted to zero and lives in the fixed terms, so
        // the sum on either side never grows with the cluster.
        List<String> violations = new ArrayList<>();
        forEachPoint(point -> {
            for (int i = 0; i + 1 < NODES.length; i++) {
                if (point.nodes() == NODES[i]) {
                    assertNotLower("nodes", point.withNodes(NODES[i + 1]), point, violations);
                }
            }
        });
        assertNoViolations("more nodes raised a cost", violations);
    }

    public void testAnObjectStoreCostsAtLeastTheLocalTablePlusItsOpen() {
        List<String> violations = new ArrayList<>();
        forEachPoint(point -> {
            if (point.storage() != StorageKind.LOCAL) {
                return;
            }
            Point remote = point.withStorage(StorageKind.OBJECT_STORE);
            double pushedLocal = point.pushed();
            double pushedRemote = remote.pushed();
            double luceneLocal = point.lucene();
            double luceneRemote = remote.lucene();
            if (lower(pushedRemote, pushedLocal) || lower(pushedRemote - pushedLocal, CostCoefficients.OBJECT_STORE_OPEN_MS)) {
                violations.add(format("pushed local %.3f ms vs object store %.3f ms at %s", pushedLocal, pushedRemote, point));
            }
            // The aggregators read the warmed column store, so the open is the whole difference.
            if (lower(luceneRemote, luceneLocal)
                || Math.abs(luceneRemote - luceneLocal - CostCoefficients.OBJECT_STORE_OPEN_MS) > slack(luceneLocal)) {
                violations.add(format("lucene local %.3f ms vs object store %.3f ms at %s", luceneLocal, luceneRemote, point));
            }
        });
        assertNoViolations("an object store table was priced below the local table", violations);
    }

    public void testAStringKeyCostsAtLeastANumericKeyOnThePushedScan() {
        // Hashing a string is dearer than hashing a number. Both twins
        // read one key column of the same width, so only the key term
        // tells them apart; the string's narrower dictionary width is
        // what AggregateProfile.of would assume, but it would let the
        // decode term hide a broken key coefficient.
        List<String> violations = new ArrayList<>();
        forEachPoint(point -> {
            if (point.stringKeys() != 1 || point.numericKeys() != 0) {
                return;
            }
            CostInputs inputs = point.inputs();
            AggregateProfile string = point.profile();
            AggregateProfile numeric = point.withNumericTwin().profile();
            double width = numeric.bytesPerRow();
            double stringCost = CostModel.pushedAggregateMillis(inputs, withColumns(string, string.columnsRead(), width));
            double numericCost = CostModel.pushedAggregateMillis(inputs, numeric);
            if (lower(stringCost, numericCost)) {
                violations.add(format("pushed string key %.3f ms vs numeric key %.3f ms at %s", stringCost, numericCost, point));
            }
        });
        assertNoViolations("a string key was priced below a numeric key", violations);
    }

    public void testTheChoiceHoldsUnderOnePercentMovesOutsideTheMargin() {
        // Every term is linear or a minimum in the rows, the groups, the
        // width and the selectivity, so a one percent move of one of
        // them moves a side by one percent at most, far inside the 1.3x
        // band. A point inside the band is noise and is not judged; the
        // selectivity of an unfiltered point is not moved, because a
        // filter that keeps 99 % of the rows is a different shape, not a
        // nearby one.
        List<String> violations = new ArrayList<>();
        int[] judged = { 0 };
        forEachPoint(point -> {
            CostInputs inputs = point.inputs();
            AggregateProfile shape = point.profile();
            double pushed = CostModel.pushedAggregateMillis(inputs, shape);
            double lucene = CostModel.luceneAggregateMillis(inputs, shape);
            if (Math.max(pushed, lucene) / Math.min(pushed, lucene) <= CHOICE_MARGIN) {
                return;
            }
            judged[0]++;
            boolean pushedWins = pushed < lucene;
            for (double factor : new double[] { 0.99, 1.01 }) {
                List<AggregateProfile> moved = new ArrayList<>();
                moved.add(withRows(shape, shape.tableRows() * factor));
                moved.add(withGroups(shape, shape.groups() * factor));
                moved.add(withColumns(shape, shape.columnsRead(), shape.bytesPerRow() * factor));
                if (shape.filtered()) {
                    moved.add(withSelectivity(shape, shape.filterSelectivity() * factor));
                }
                for (AggregateProfile near : moved) {
                    double nearPushed = CostModel.pushedAggregateMillis(inputs, near);
                    double nearLucene = CostModel.luceneAggregateMillis(inputs, near);
                    if ((nearPushed < nearLucene) != pushedWins) {
                        violations.add(
                            format(
                                "pushed %.3f ms vs lucene %.3f ms flips to %.3f vs %.3f at %s moved to %s",
                                pushed,
                                lucene,
                                nearPushed,
                                nearLucene,
                                point,
                                near
                            )
                        );
                    }
                }
            }
        });
        assertTrue("points outside the margin were judged", judged[0] > 1_000);
        assertNoViolations("a one percent move flipped a choice outside the margin", violations);
    }

    // ---- grid --------------------------------------------------------------

    private static void forEachPoint(Consumer<Point> visitor) {
        for (int nodes : NODES) {
            for (StorageKind storage : STORAGE) {
                for (double rows : ROWS) {
                    for (double groups : GROUPS) {
                        for (int stringKeys : STRING_KEYS) {
                            for (int numericKeys : NUMERIC_KEYS) {
                                for (int metrics : METRICS) {
                                    for (int levels : LEVELS) {
                                        for (double selectivity : SELECTIVITY) {
                                            visitor.accept(
                                                new Point(
                                                    nodes,
                                                    storage,
                                                    rows,
                                                    groups,
                                                    stringKeys,
                                                    numericKeys,
                                                    metrics,
                                                    levels,
                                                    selectivity
                                                )
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // ---- comparisons -------------------------------------------------------

    /** Records a violation when either side of {@code more} costs less than the same side of {@code less}. */
    private static void assertNotLower(String axis, Point less, Point more, List<String> violations) {
        double pushedLess = less.pushed();
        double pushedMore = more.pushed();
        if (lower(pushedMore, pushedLess)) {
            violations.add(format("pushed %.3f ms falls to %.3f ms along %s from %s to %s", pushedLess, pushedMore, axis, less, more));
        }
        double luceneLess = less.lucene();
        double luceneMore = more.lucene();
        if (lower(luceneMore, luceneLess)) {
            violations.add(format("lucene %.3f ms falls to %.3f ms along %s from %s to %s", luceneLess, luceneMore, axis, less, more));
        }
    }

    /** Whether {@code a} is below {@code b} by more than the rounding slack. */
    private static boolean lower(double a, double b) {
        return a < b - slack(b);
    }

    private static double slack(double reference) {
        return SLACK * Math.max(1.0, Math.abs(reference));
    }

    private static void assertNoViolations(String what, List<String> violations) {
        if (violations.isEmpty()) {
            return;
        }
        StringBuilder message = new StringBuilder(what).append(" (").append(violations.size()).append(" points):");
        for (String violation : violations.subList(0, Math.min(REPORTED_VIOLATIONS, violations.size()))) {
            message.append("\n  ").append(violation);
        }
        if (violations.size() > REPORTED_VIOLATIONS) {
            message.append("\n  ... and ").append(violations.size() - REPORTED_VIOLATIONS).append(" more");
        }
        fail(message.toString());
    }

    private static String format(String pattern, Object... arguments) {
        return String.format(Locale.ROOT, pattern, arguments);
    }

    // ---- profile edits -----------------------------------------------------

    private static AggregateProfile withRows(AggregateProfile p, double rows) {
        return copy(p, rows, p.groups(), p.mergedGroups(), p.columnsRead(), p.bytesPerRow(), p.filterSelectivity());
    }

    private static AggregateProfile withGroups(AggregateProfile p, double groups) {
        return copy(p, p.tableRows(), groups, groups, p.columnsRead(), p.bytesPerRow(), p.filterSelectivity());
    }

    private static AggregateProfile withColumns(AggregateProfile p, int columnsRead, double bytesPerRow) {
        return copy(p, p.tableRows(), p.groups(), p.mergedGroups(), columnsRead, bytesPerRow, p.filterSelectivity());
    }

    private static AggregateProfile withSelectivity(AggregateProfile p, double selectivity) {
        return copy(p, p.tableRows(), p.groups(), p.mergedGroups(), p.columnsRead(), p.bytesPerRow(), selectivity);
    }

    private static AggregateProfile copy(
        AggregateProfile p,
        double rows,
        double groups,
        double mergedGroups,
        int columnsRead,
        double bytesPerRow,
        double selectivity
    ) {
        return new AggregateProfile(
            rows,
            groups,
            mergedGroups,
            p.groupsKnown(),
            columnsRead,
            bytesPerRow,
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
}

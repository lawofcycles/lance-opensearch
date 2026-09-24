/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.opensearch.lance.plan.calcite.LanceRel;
import org.opensearch.lance.plan.calcite.LuceneRel;
import org.opensearch.lance.plan.calcite.ShardPathRel;
import org.opensearch.lance.plan.traits.PlanRequirement;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Renders a plan tree for the explain response, one operator per line
 * as {@code RelOptUtil.toString} does, and appends to every physical
 * operator (a {@link LanceRel}, {@link LuceneRel} or
 * {@link ShardPathRel}) the trait values it declares and the cost the
 * planner charged it:
 *
 * <pre>
 * MergeExec(reduce=[COUNT_SUM], accuracy=[EXACT], tie_stability=[STABLE_ROWADDR], cost=[{ms=1, native_bytes=1, heap_bytes=0}], total_cost=[{ms=8, native_bytes=2, heap_bytes=0}])
 *   FanOutExec(fanOut=[1], partitioning=[EQUAL_FRAGMENT_GROUPS], accuracy=[EXACT], tie_stability=[STABLE_ROWADDR], cost=[{ms=1, native_bytes=1, heap_bytes=0}])
 *     LanceTableScan(table=[[lance, demo]], accuracy=[EXACT], tie_stability=[STABLE_ROWADDR], cost=[{ms=6, native_bytes=0, heap_bytes=0}])
 * </pre>
 *
 * <p>The terms are added by this writer, not by the operators'
 * {@code explainTerms}: the trait set already carries the values (the
 * digest the Volcano planner compares includes it), the logical tree
 * renders without them, and the cost is a planner figure the operators
 * should not compute while being written into a digest. {@code cost}
 * is the operator's own charge ({@code computeSelfCost}), read through
 * the cluster's metadata query with the same {@code CostInputs} the
 * plan was made under; the root carries {@code total_cost} as well,
 * the cumulative cost of the whole tree, which is the figure the
 * Volcano run compared candidates by (plus the coordinator layer's two
 * constants when the tree is the coordinator's). The three numbers are
 * the {@code LanceCost} slots: predicted milliseconds (a placeholder
 * count of rows or units below the fitted model's range), predicted
 * native bytes and predicted heap bytes, each rounded to two
 * significant digits. A logical operator (one the planner kept when it
 * could not plan the tree) gets neither term.
 */
final class PlanText extends RelWriterImpl {

    private static final MathContext TWO_SIGNIFICANT_DIGITS = new MathContext(2);

    private final RelNode root;

    private PlanText(PrintWriter pw, RelNode root) {
        super(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
        this.root = root;
    }

    /** The tree under {@code root}, one operator per line, with the trait and cost terms on every physical operator. */
    static String render(RelNode root) {
        StringWriter sw = new StringWriter();
        root.explain(new PlanText(new PrintWriter(sw), root));
        return sw.toString();
    }

    @Override
    protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
        if (!physical(rel)) {
            super.explain_(rel, values);
            return;
        }
        List<Pair<String, Object>> withTraitsAndCost = new ArrayList<>(values);
        withTraitsAndCost.add(Pair.of("accuracy", PlanRequirement.declaredAccuracy(rel.getTraitSet()).name()));
        withTraitsAndCost.add(Pair.of("tie_stability", PlanRequirement.declaredTieStability(rel.getTraitSet()).name()));
        RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        withTraitsAndCost.add(Pair.of("cost", format(mq.getNonCumulativeCost(rel))));
        if (rel == root) {
            withTraitsAndCost.add(Pair.of("total_cost", format(mq.getCumulativeCost(rel))));
        }
        super.explain_(rel, withTraitsAndCost);
    }

    /** Whether {@code rel} is one of the plugin's physical operators, the ones that declare traits and are costed. */
    static boolean physical(RelNode rel) {
        return rel instanceof LanceRel || rel instanceof LuceneRel || rel instanceof ShardPathRel;
    }

    /** {@code {ms=..., native_bytes=..., heap_bytes=...}}, each slot rounded to two significant digits; {@code unknown} without a cost. */
    static String format(RelOptCost cost) {
        if (cost == null) {
            return "unknown";
        }
        return "{ms="
            + significant(cost.getRows())
            + ", native_bytes="
            + significant(cost.getCpu())
            + ", heap_bytes="
            + significant(cost.getIo())
            + "}";
    }

    /**
     * {@code value} rounded to two significant digits in plain
     * notation ({@code 1200}, {@code 1.2}, {@code 0.0012}), {@code 0} for
     * zero, {@code inf} for an infinite cost, and scientific notation
     * for the huge cost the factory makes from {@code Double.MAX_VALUE}.
     */
    static String significant(double value) {
        if (Double.isInfinite(value)) {
            return "inf";
        }
        if (Double.isNaN(value)) {
            return "nan";
        }
        if (value == 0.0) {
            return "0";
        }
        if (Math.abs(value) >= 1.0e15) {
            return String.format(Locale.ROOT, "%.1e", value);
        }
        return new BigDecimal(value).round(TWO_SIGNIFICANT_DIGITS).stripTrailingZeros().toPlainString();
    }
}

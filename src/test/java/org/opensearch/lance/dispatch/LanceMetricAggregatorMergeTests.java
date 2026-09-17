/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Collections;
import java.util.List;

import org.opensearch.lance.dispatch.LanceMetricAggregator.MetricSpec;
import org.opensearch.lance.dispatch.LanceMetricAggregator.MetricType;
import org.opensearch.lance.dispatch.LanceMetricAggregator.PartialState;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Exercises {@link LanceMetricAggregator#mergePartials} directly with
 * multi-group input, without a live Lance scan. Milestone 5-C1
 * introduced the partial + merge split but every existing IT drives
 * it with a single group (single-node dispatch). Milestone 5-C4's
 * fan-out will feed the same {@code mergePartials} routine with N
 * groups once a data node cluster exists; if the associativity or
 * min/max sentinel handling breaks under N &gt; 1 groups, the
 * multi-node code path will silently return wrong numbers.
 *
 * <p>These tests fabricate the partials directly so multi-group
 * merge correctness can be verified without waiting on a multi-node
 * IT. They cover:
 * <ul>
 *   <li>Every supported metric type ({@code value_count},
 *       {@code sum}, {@code avg}, {@code min}, {@code max}).</li>
 *   <li>Empty and non-empty group mixing so
 *       {@link PartialState#EMPTY}'s neutral behaviour is exercised.</li>
 *   <li>Order independence so a reshuffled coordinator would return
 *       the same result.</li>
 * </ul>
 */
public class LanceMetricAggregatorMergeTests extends OpenSearchTestCase {

    private static final MetricSpec COUNT = new MetricSpec("c", MetricType.VALUE_COUNT, "id");
    private static final MetricSpec SUM = new MetricSpec("s", MetricType.SUM, "id");
    private static final MetricSpec AVG = new MetricSpec("a", MetricType.AVG, "id");
    private static final MetricSpec MIN = new MetricSpec("mn", MetricType.MIN, "id");
    private static final MetricSpec MAX = new MetricSpec("mx", MetricType.MAX, "id");
    private static final List<MetricSpec> ALL = List.of(COUNT, SUM, AVG, MIN, MAX);

    public void testMergeAcrossTwoGroupsSumsPartials() {
        // Group A saw rows [1, 2, 3]: count=3, sum=6, min=1, max=3.
        // Group B saw rows [4, 5]: count=2, sum=9, min=4, max=5.
        // Total: count=5, sum=15, avg=3, min=1, max=5.
        List<PartialState> groupA = List.of(
            new PartialState(3, 6, 1, 3), // COUNT
            new PartialState(3, 6, 1, 3), // SUM
            new PartialState(3, 6, 1, 3), // AVG
            new PartialState(3, 6, 1, 3), // MIN
            new PartialState(3, 6, 1, 3)  // MAX
        );
        List<PartialState> groupB = List.of(
            new PartialState(2, 9, 4, 5),
            new PartialState(2, 9, 4, 5),
            new PartialState(2, 9, 4, 5),
            new PartialState(2, 9, 4, 5),
            new PartialState(2, 9, 4, 5)
        );

        InternalAggregations merged = LanceMetricAggregator.mergePartials(ALL, List.of(groupA, groupB));

        assertEquals(5L, ((InternalValueCount) merged.asMap().get("c")).getValue());
        assertEquals(15.0d, ((InternalSum) merged.asMap().get("s")).getValue(), 0.0d);
        assertEquals(3.0d, ((InternalAvg) merged.asMap().get("a")).getValue(), 0.0d);
        assertEquals(1.0d, ((InternalMin) merged.asMap().get("mn")).getValue(), 0.0d);
        assertEquals(5.0d, ((InternalMax) merged.asMap().get("mx")).getValue(), 0.0d);
    }

    public void testMergeIsOrderIndependent() {
        // The merge must be associative and commutative so a
        // coordinator reordering per-node responses (e.g. because a
        // node replied faster) produces the same result. Feed two
        // groups in each order and assert every metric matches.
        List<PartialState> groupA = List.of(
            new PartialState(2, 4, 1, 3),
            new PartialState(2, 4, 1, 3),
            new PartialState(2, 4, 1, 3),
            new PartialState(2, 4, 1, 3),
            new PartialState(2, 4, 1, 3)
        );
        List<PartialState> groupB = List.of(
            new PartialState(3, 10, 2, 6),
            new PartialState(3, 10, 2, 6),
            new PartialState(3, 10, 2, 6),
            new PartialState(3, 10, 2, 6),
            new PartialState(3, 10, 2, 6)
        );

        InternalAggregations forward = LanceMetricAggregator.mergePartials(ALL, List.of(groupA, groupB));
        InternalAggregations reverse = LanceMetricAggregator.mergePartials(ALL, List.of(groupB, groupA));

        for (MetricSpec spec : ALL) {
            String name = spec.name();
            assertEquals(
                "metric [" + name + "] must be order-independent",
                extractDouble(forward, name),
                extractDouble(reverse, name),
                0.0d
            );
        }
    }

    public void testMergeAbsorbsEmptyGroups() {
        // A node with no matching rows in its fragment slice must
        // return a partial that leaves every metric untouched. Two
        // groups: one with data, one with the neutral element.
        List<PartialState> populated = List.of(
            new PartialState(4, 20, 2, 8),
            new PartialState(4, 20, 2, 8),
            new PartialState(4, 20, 2, 8),
            new PartialState(4, 20, 2, 8),
            new PartialState(4, 20, 2, 8)
        );
        List<PartialState> empty = List.of(
            PartialState.EMPTY,
            PartialState.EMPTY,
            PartialState.EMPTY,
            PartialState.EMPTY,
            PartialState.EMPTY
        );

        InternalAggregations merged = LanceMetricAggregator.mergePartials(ALL, List.of(populated, empty));

        // count=4, sum=20, avg=5, min=2, max=8. Empty group must not
        // pull min toward +Infinity or max toward -Infinity.
        assertEquals(4L, ((InternalValueCount) merged.asMap().get("c")).getValue());
        assertEquals(20.0d, ((InternalSum) merged.asMap().get("s")).getValue(), 0.0d);
        assertEquals(5.0d, ((InternalAvg) merged.asMap().get("a")).getValue(), 0.0d);
        assertEquals(2.0d, ((InternalMin) merged.asMap().get("mn")).getValue(), 0.0d);
        assertEquals(8.0d, ((InternalMax) merged.asMap().get("mx")).getValue(), 0.0d);
    }

    public void testMergeAllEmptyLeavesSentinelInMinMax() {
        // Every node returned an empty partial. The response must
        // still be well-formed: count=0, sum=0, avg=NaN, and
        // min/max hold Lance's +/- Infinity sentinel that matches
        // the standard aggregator's empty-shard output.
        List<PartialState> empty = List.of(
            PartialState.EMPTY,
            PartialState.EMPTY,
            PartialState.EMPTY,
            PartialState.EMPTY,
            PartialState.EMPTY
        );

        InternalAggregations merged = LanceMetricAggregator.mergePartials(ALL, List.of(empty, empty));

        assertEquals(0L, ((InternalValueCount) merged.asMap().get("c")).getValue());
        assertEquals(0.0d, ((InternalSum) merged.asMap().get("s")).getValue(), 0.0d);
        assertTrue(Double.isNaN(((InternalAvg) merged.asMap().get("a")).getValue()));
        assertEquals(Double.POSITIVE_INFINITY, ((InternalMin) merged.asMap().get("mn")).getValue(), 0.0d);
        assertEquals(Double.NEGATIVE_INFINITY, ((InternalMax) merged.asMap().get("mx")).getValue(), 0.0d);
    }

    public void testMergeAcrossManyGroupsMatchesSingleGroupSum() {
        // Split rows [0..9] into three groups of arbitrary size and
        // verify the merged sum matches what a single-scan pass on
        // the same rows would return. This is the safety net for
        // any future coordinator that chooses different fragment
        // partitions.
        List<PartialState> groupA = List.of(new PartialState(3, 0 + 1 + 2, 0, 2)); // rows [0,1,2]
        List<PartialState> groupB = List.of(new PartialState(4, 3 + 4 + 5 + 6, 3, 6)); // rows [3..6]
        List<PartialState> groupC = List.of(new PartialState(3, 7 + 8 + 9, 7, 9)); // rows [7,8,9]

        InternalAggregations merged = LanceMetricAggregator.mergePartials(List.of(SUM), List.of(groupA, groupB, groupC));

        int expectedSum = 0 + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9;
        assertEquals((double) expectedSum, ((InternalSum) merged.asMap().get("s")).getValue(), 0.0d);
    }

    public void testMergeWithNoGroupsHandlesEmptyInput() {
        // If the coordinator finds no matching indexes, it still
        // calls mergePartials with an empty list. The result must
        // be a well-formed InternalAggregations that emits zero
        // for count and sum, NaN for avg, and the +/- Infinity
        // sentinels for min/max. This mirrors the shard-mode
        // "empty search across zero shards" output.
        InternalAggregations merged = LanceMetricAggregator.mergePartials(ALL, Collections.emptyList());

        assertNotNull(merged);
        assertEquals(0L, ((InternalValueCount) merged.asMap().get("c")).getValue());
        assertEquals(0.0d, ((InternalSum) merged.asMap().get("s")).getValue(), 0.0d);
        assertTrue(Double.isNaN(((InternalAvg) merged.asMap().get("a")).getValue()));
        assertEquals(Double.POSITIVE_INFINITY, ((InternalMin) merged.asMap().get("mn")).getValue(), 0.0d);
        assertEquals(Double.NEGATIVE_INFINITY, ((InternalMax) merged.asMap().get("mx")).getValue(), 0.0d);
    }

    private static double extractDouble(InternalAggregations aggregations, String name) {
        Object agg = aggregations.asMap().get(name);
        if (agg instanceof InternalValueCount c) return (double) c.getValue();
        if (agg instanceof InternalSum s) return s.getValue();
        if (agg instanceof InternalAvg a) return a.getValue();
        if (agg instanceof InternalMin mn) return mn.getValue();
        if (agg instanceof InternalMax mx) return mx.getValue();
        throw new AssertionError("unexpected aggregation type for " + name + ": " + agg);
    }
}

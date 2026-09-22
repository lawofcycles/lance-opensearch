/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import org.opensearch.lance.plan.rel.BucketSpec;
import org.opensearch.lance.plan.rel.MetricSpec;

/**
 * View over the OpenSearch aggregation semantics a Calcite
 * {@code Aggregate} node cannot carry on its own. The planner's
 * aggregate node implements this interface so the Substrait producer
 * (and the executor) can read, per group key, which bucket aggregation
 * produced the key expression and, per aggregate call, which metric
 * aggregation the call stands for.
 *
 * <p>Calcite's {@code Aggregate} vocabulary covers group keys and
 * standard aggregate functions only. OpenSearch shapes such as
 * {@code stats} (five measures behind one call), {@code cardinality}
 * (an extra grouping, no measure) or {@code percentiles} (a second
 * scan) need the original aggregation kind next to the call, and the
 * response builder needs names, sizes, orders and formats that play no
 * role in planning. The {@link BucketSpec} / {@link MetricSpec}
 * records carry exactly those fields; everything Calcite already
 * models (the key expressions, the call arguments) stays on the
 * {@code Aggregate}.
 */
public interface LanceAggregateSpecs {

    /**
     * The bucket specification of one group key, aligned with the group
     * set order: index {@code i} describes the {@code i}-th set bit of
     * the aggregate's group set.
     */
    BucketSpec bucket(int groupKeyIndex);

    /**
     * The metric specification of one aggregate call, aligned with the
     * call order: index {@code i} describes
     * {@code getAggCallList().get(i)}.
     */
    MetricSpec metric(int callIndex);
}

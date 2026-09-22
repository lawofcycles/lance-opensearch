/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.calcite.rel.RelNode;

/**
 * Marker for relational operators that answer through OpenSearch's regular
 * shard search path instead of the fragment fan-out.
 * {@link ShardPathConvention} uses this interface as its convention
 * interface class.
 */
public interface ShardPathRel extends RelNode {}

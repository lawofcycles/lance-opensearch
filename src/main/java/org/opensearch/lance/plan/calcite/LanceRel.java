/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.calcite.rel.RelNode;

/**
 * Marker for relational operators that execute inside Lance's native scan
 * machinery. {@link LanceConvention} uses this interface as its convention
 * interface class, so the planner can tell Lance native operators apart
 * from Lucene backed and shard path operators.
 */
public interface LanceRel extends RelNode {}

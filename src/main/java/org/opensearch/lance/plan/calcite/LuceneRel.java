/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.calcite.rel.RelNode;

/**
 * Marker for relational operators that execute through Lucene's aggregator
 * and collector machinery over per-fragment leaf readers.
 * {@link LuceneConvention} uses this interface as its convention interface
 * class.
 */
public interface LuceneRel extends RelNode {}

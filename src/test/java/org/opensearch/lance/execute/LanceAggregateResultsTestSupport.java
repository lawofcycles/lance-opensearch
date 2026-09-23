/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Builds a {@link LanceAggregateResults} without a mapping or a
 * dataset, for tests outside this package that only need a resolved
 * aggregate to exist (the plan refiner's guards, which hand the
 * executor through untouched or drop it). The instance carries no
 * groupings and no metrics and must not be executed.
 */
public final class LanceAggregateResultsTestSupport {

    private LanceAggregateResultsTestSupport() {}

    /** A resolved aggregate of {@code shape} over an empty main scan. */
    public static LanceAggregateResults resolvedStub(LanceAggregateResults.PushedShape shape) {
        ResolvedAggregate resolved = new ResolvedAggregate(shape, ByteBuffer.allocate(0), List.of(), null, List.of(), List.of(), 0, null);
        return new LanceAggregateResults(resolved);
    }
}

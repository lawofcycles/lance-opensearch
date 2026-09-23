/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.opensearch.lance.plan.calcite.LanceTable;
import org.opensearch.lance.plan.rel.LanceTableScan;

import java.util.Optional;

/**
 * The planner's metadata provider: Lance's own {@code RowCount} and
 * {@code DistinctRowCount} handlers for {@link LanceTableScan}, chained
 * in front of Calcite's {@link DefaultRelMetadataProvider} so every
 * other node and every other kind of metadata keeps Calcite's answer.
 *
 * <p>One instance for the whole node. Calcite generates and compiles
 * the dispatching handler class once per {@code (handler kind,
 * provider)} and caches it keyed on the provider's equality, so a fresh
 * chain per plan would recompile on every request; the singleton keeps
 * the compiled dispatcher shared. The handlers are reflective sources
 * over public top level classes, which is what the generated dispatcher
 * can name from its own package.
 */
public final class LanceRelMetadataProvider {

    private LanceRelMetadataProvider() {}

    /** The chained provider the planner factory installs on every cluster. */
    public static final RelMetadataProvider INSTANCE = ChainedRelMetadataProvider.of(
        ImmutableList.of(
            ReflectiveRelMetadataProvider.reflectiveSource(new LanceRelMdRowCount(), BuiltInMetadata.RowCount.Handler.class),
            ReflectiveRelMetadataProvider.reflectiveSource(
                new LanceRelMdDistinctRowCount(),
                BuiltInMetadata.DistinctRowCount.Handler.class
            ),
            DefaultRelMetadataProvider.INSTANCE
        )
    );

    /** The table statistics behind {@code scan}, empty when its table carries none. */
    static Optional<TableStatistics> statisticsOf(LanceTableScan scan) {
        LanceTable table = scan.getTable().unwrap(LanceTable.class);
        return table == null ? Optional.empty() : table.tableStatistics();
    }

    /**
     * Whether the scan stands in for a pushed aggregate, full text match
     * or knn search, whose row type and row estimate are the pushed
     * node's rather than the table's.
     */
    static boolean carriesPushedQuery(LanceTableScan scan) {
        return scan.pushedAggregate().isPresent() || scan.pushedFts().isPresent() || scan.pushedKnn().isPresent();
    }
}

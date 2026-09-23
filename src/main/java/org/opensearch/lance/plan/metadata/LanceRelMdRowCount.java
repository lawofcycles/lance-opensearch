/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.metadata;

import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFilter;
import org.opensearch.lance.plan.rel.PushedOperation.PushedTopK;

import java.util.Optional;

/**
 * {@code RowCount} handler for {@link LanceTableScan}: the live row
 * count of the table's {@link TableStatistics}, scaled by Calcite's
 * guessed selectivity of a pushed filter and capped by a pushed top-k
 * page. A scan whose table carries no statistics, or that stands in for
 * a pushed aggregate, full text match or knn search (whose row estimate
 * is the pushed node's own), answers what the scan itself estimates,
 * which is what Calcite's default handler returns for a table scan.
 *
 * <p>Public with public handler methods because Calcite's generated
 * metadata dispatcher names this class and calls the methods from
 * another package.
 */
public final class LanceRelMdRowCount implements MetadataHandler<BuiltInMetadata.RowCount> {

    @Override
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
        return BuiltInMetadata.RowCount.DEF;
    }

    /** Row estimate of a Lance scan; see the class comment. */
    public Double getRowCount(LanceTableScan scan, RelMetadataQuery mq) {
        Optional<TableStatistics> statistics = LanceRelMetadataProvider.statisticsOf(scan);
        if (statistics.isEmpty() || LanceRelMetadataProvider.carriesPushedQuery(scan)) {
            return scan.estimateRowCount(mq);
        }
        double rows = statistics.get().rowCount();
        Optional<PushedFilter> filter = scan.pushedFilter();
        if (filter.isPresent()) {
            rows *= RelMdUtil.guessSelectivity(filter.get().condition());
        }
        Optional<PushedTopK> topK = scan.pushedTopK();
        if (topK.isPresent()) {
            rows = Math.min(rows, (double) topK.get().topK().fetch() + topK.get().topK().offset());
        }
        return rows;
    }
}

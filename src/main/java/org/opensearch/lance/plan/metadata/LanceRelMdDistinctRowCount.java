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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;
import org.opensearch.lance.plan.rel.LanceTableScan;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * {@code DistinctRowCount} handler for {@link LanceTableScan}: when
 * every group key column carries an index that reports a distinct value
 * estimate (a bitmap index's {@code num_bitmaps}), the product of those
 * estimates capped by the scan's row count; with a predicate, Calcite's
 * usual reduction of a domain of that size to the rows the predicate
 * selects. When a key column has no such estimate, or the scan stands in
 * for a pushed aggregate, full text match, knn search or top-k page
 * (whose row type may no longer name the table's columns), Calcite's
 * default answer for a table scan: the row count when the key columns
 * are known unique, unknown otherwise.
 *
 * <p>Public with public handler methods because Calcite's generated
 * metadata dispatcher names this class and calls the methods from
 * another package.
 */
public final class LanceRelMdDistinctRowCount implements MetadataHandler<BuiltInMetadata.DistinctRowCount> {

    @Override
    public MetadataDef<BuiltInMetadata.DistinctRowCount> getDef() {
        return BuiltInMetadata.DistinctRowCount.DEF;
    }

    /** Distinct row estimate of a Lance scan over {@code groupKey}; see the class comment. */
    public Double getDistinctRowCount(LanceTableScan scan, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
        Optional<TableStatistics> statistics = LanceRelMetadataProvider.statisticsOf(scan);
        if (statistics.isPresent()
            && !groupKey.isEmpty()
            && !LanceRelMetadataProvider.carriesPushedQuery(scan)
            && scan.pushedTopK().isEmpty()) {
            List<String> columns = scan.getRowType().getFieldNames();
            double domain = 1.0;
            boolean allKnown = true;
            for (int ordinal : groupKey) {
                OptionalLong distinct = ordinal < columns.size()
                    ? statistics.get().column(columns.get(ordinal)).map(ColumnStatistics::distinctCount).orElse(OptionalLong.empty())
                    : OptionalLong.empty();
                if (distinct.isEmpty()) {
                    allKnown = false;
                    break;
                }
                domain *= distinct.getAsLong();
            }
            if (allKnown) {
                Double rows = mq.getRowCount(scan);
                double capped = rows == null ? domain : Math.min(domain, rows);
                if (predicate == null || rows == null) {
                    return capped;
                }
                Double selectivity = mq.getSelectivity(scan, predicate);
                if (selectivity == null) {
                    return capped;
                }
                return RelMdUtil.numDistinctVals(capped, rows * selectivity);
            }
        }
        if (RelMdUtil.areColumnsDefinitelyUnique(mq, scan, groupKey)) {
            return NumberUtil.multiply(mq.getRowCount(scan), mq.getSelectivity(scan, predicate));
        }
        return null;
    }
}

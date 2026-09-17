/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.lance.dispatch.LanceMetricAggregator;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;

/**
 * Phase A prototype for direction 1: verify a per-fragment
 * {@link org.opensearch.lance.engine.LanceFragmentLeafReader} can drive
 * OpenSearch's stock {@link org.opensearch.search.aggregations.metrics.SumAggregator}
 * with the same numeric result as the current fragment-path
 * {@link LanceMetricAggregator}.
 *
 * <p>If green, this is proof-of-concept that fragment path can reuse
 * shard path's execution machinery per-fragment without a separate
 * implementation. Kill criteria for Phase A:
 * <ul>
 *   <li>SearchContext substitute exceeds 1000 lines
 *       (mitigated by reusing {@link AggregatorTestCase} which brings
 *       its own substitute for free)</li>
 *   <li>{@code AggregatorFactory.build} demands SearchContext plumbing
 *       that plugin code cannot construct</li>
 * </ul>
 *
 * <p>The test opens a small Lance table via {@link LanceTableFactory},
 * wraps its fragments in a single {@link LanceDirectoryReader} (shardId=0,
 * numShards=1 which is "all fragments in one reader"), and calls
 * {@link AggregatorTestCase#searchAndReduce} against that. The reference
 * value is computed both analytically (arithmetic series sum) and via
 * {@link LanceMetricAggregator#aggregatePartials} so any drift between
 * the two paths is caught.
 *
 * <p>Progress log: {@code direction1-prototype.md} in the design notes.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class PerFragmentAggregatorPrototypeTests extends AggregatorTestCase {

    public void testSumOnIntColumnMatchesLanceMetricAggregatorAndArithmeticSeries() throws Exception {
        Path scratch = Files.createTempDirectory("lance-phaseA-");
        int rowCount = 100;
        String uri = LanceTableFactory.writeTable(scratch, "phaseA", rowCount);

        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            // Reference computations that need to see the Lance Dataset must
            // happen before we hand ownership to LanceDirectoryReader — its
            // doClose closes the Dataset, so anything Lance-side after the
            // reader closes would fail with "Dataset is closed".
            List<Integer> fragmentIds = dataset.getFragments().stream().map(Fragment::getId).toList();
            LanceMetricAggregator.MetricSpec spec = new LanceMetricAggregator.MetricSpec(
                "id_sum",
                LanceMetricAggregator.MetricType.SUM,
                "id"
            );
            List<LanceMetricAggregator.PartialState> partials = LanceMetricAggregator.aggregatePartials(
                dataset,
                fragmentIds,
                null,
                List.of(spec)
            );
            double sumViaLanceMetricAggregator = 0.0;
            for (LanceMetricAggregator.PartialState p : partials) {
                sumViaLanceMetricAggregator += p.sum();
            }

            // OpenSearch SumAggregator path: LanceDirectoryReader wraps every
            // fragment as a Lucene leaf, and searchAndReduce() drives the
            // aggregator through the standard SearchContext substitute
            // AggregatorTestCase installs. This is the exact code the shard
            // path runs today, just plugged into the reader that fragment
            // path would give it on the per-node handler.
            MappedFieldType idType = new NumberFieldMapper.NumberFieldType("id", NumberFieldMapper.NumberType.INTEGER);
            SumAggregationBuilder sumAgg = new SumAggregationBuilder("id_sum").field("id");

            double sumViaOpenSearch;
            try (
                DirectoryReader dr = LanceDirectoryReader.open(
                    new ByteBuffersDirectory(),
                    null,
                    dataset,
                    "id",
                    /* shardId */ 0,
                    /* numShards */ 1
                )
            ) {
                IndexSearcher searcher = new IndexSearcher(dr);
                InternalSum result = searchAndReduce(searcher, new MatchAllDocsQuery(), sumAgg, idType);
                sumViaOpenSearch = result.getValue();
            }

            // Analytical closed form for id = 0..rowCount-1
            double expectedAnalytical = rowCount * (rowCount - 1) / 2.0;

            assertEquals("analytical baseline", expectedAnalytical, sumViaLanceMetricAggregator, 0.0);
            assertEquals(
                "OpenSearch SumAggregator via per-fragment LanceFragmentLeafReader",
                expectedAnalytical,
                sumViaOpenSearch,
                0.0
            );
        }
    }
}

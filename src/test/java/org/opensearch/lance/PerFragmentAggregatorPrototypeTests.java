/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.lance.dispatch.LanceMetricAggregator;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;

/**
 * Direction 1 prototype tests: verify per-fragment
 * {@link org.opensearch.lance.engine.LanceFragmentLeafReader}s can drive
 * OpenSearch's stock aggregators (Sum, Terms, Terms + sub-Sum) and Lucene
 * sort with the same results as the current fragment-path
 * {@link LanceMetricAggregator} (metric family) or arithmetic reasoning
 * about the fixture (bucket / sort).
 *
 * <p>If green through Phases A + B + C, this is proof that fragment path
 * can be a thin orchestration layer over shard path's execution machinery
 * per-fragment, instead of maintaining a parallel implementation. Kill
 * criteria are documented in
 * {@code research/opensearch/lance-integration/direction1-prototype.md}.
 *
 * <p>Each test opens fragments via
 * {@link LanceDirectoryReader#open(org.apache.lucene.store.Directory, org.apache.lucene.index.IndexCommit, Dataset, String, int, int)}
 * with {@code shardId=0, numShards=1} which packs every fragment as a
 * Lucene leaf under one DirectoryReader. That mirrors what a fragment-path
 * per-node handler would give the shard aggregator machinery in production.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class PerFragmentAggregatorPrototypeTests extends AggregatorTestCase {

    /** Phase A: metric aggregation (SumAggregator) via per-fragment reader. */
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

    /** Phase B-1: bucket aggregation (TermsAggregator) on a keyword column. */
    public void testTermsOnKeywordColumnReturnsBucketPerUniqueValue() throws Exception {
        Path scratch = Files.createTempDirectory("lance-phaseB-terms-");
        int rowCount = 10;
        String uri = LanceTableFactory.writeKeywordOnlyTable(scratch, "phaseB_terms", rowCount);

        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            MappedFieldType labelType = new KeywordFieldMapper.KeywordFieldType("label");
            TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("labels").field("label").size(rowCount);

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
                StringTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), termsAgg, labelType);

                assertEquals("bucket count matches unique label count", rowCount, result.getBuckets().size());
                Set<String> keys = new HashSet<>();
                for (Terms.Bucket bucket : result.getBuckets()) {
                    keys.add(bucket.getKeyAsString());
                    assertEquals("every bucket carries exactly one doc", 1L, bucket.getDocCount());
                }
                Set<String> expected = new HashSet<>();
                for (int i = 0; i < rowCount; i++) {
                    expected.add("row-" + i);
                }
                assertEquals("bucket keys match every row's label", expected, keys);
            }
        }
    }

    /**
     * Phase B-1 (bis): bucket aggregation with a sub-aggregation. Proves the
     * per-fragment reader also feeds child aggregators, not just top-level
     * metrics, which is what real bucket-agg workloads use in practice.
     */
    public void testTermsOnKeywordWithSubSumOnIntColumn() throws Exception {
        Path scratch = Files.createTempDirectory("lance-phaseB-terms-sub-");
        int rowCount = 10;
        String uri = LanceTableFactory.writeKeywordOnlyTable(scratch, "phaseB_terms_sub", rowCount);

        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            MappedFieldType labelType = new KeywordFieldMapper.KeywordFieldType("label");
            MappedFieldType idType = new NumberFieldMapper.NumberFieldType("id", NumberFieldMapper.NumberType.INTEGER);
            TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("labels").field("label")
                .size(rowCount)
                .subAggregation(new SumAggregationBuilder("id_sum").field("id"));

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
                StringTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), termsAgg, labelType, idType);

                assertEquals(rowCount, result.getBuckets().size());
                for (Terms.Bucket bucket : result.getBuckets()) {
                    String key = bucket.getKeyAsString();
                    int expectedId = Integer.parseInt(key.substring("row-".length()));
                    InternalSum sub = (InternalSum) bucket.getAggregations().get("id_sum");
                    assertEquals(
                        "each label has exactly one row and its id_sum is that row's id",
                        (double) expectedId,
                        sub.getValue(),
                        0.0
                    );
                }
            }
        }
    }

    /**
     * Phase B-2: Lucene sort via IndexSearcher.search(query, n, sort). The
     * shard path already relies on this to serve stock {@code sort} clauses
     * for search hits over Lance-backed indexes. Prototype the same call
     * against a per-fragment LanceDirectoryReader.
     */
    public void testSortByIntColumnDescendingReturnsRowsInReverseOrder() throws Exception {
        Path scratch = Files.createTempDirectory("lance-phaseB-sort-");
        int rowCount = 20;
        String uri = LanceTableFactory.writeTable(scratch, "phaseB_sort", rowCount);

        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
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
                // Sort by id descending. LanceFragmentLeafReader exposes id
                // as NUMERIC doc values; SortField uses that for its comparator.
                Sort sort = new Sort(new SortField("id", SortField.Type.LONG, /* reverse */ true));
                TopFieldDocs top = searcher.search(new MatchAllDocsQuery(), rowCount, sort);

                assertEquals("hit count matches rowCount", rowCount, top.totalHits.value());
                assertEquals("returned hits fills the requested top", rowCount, top.scoreDocs.length);
                for (int i = 0; i < rowCount; i++) {
                    ScoreDoc hit = top.scoreDocs[i];
                    long value = (long) ((org.apache.lucene.search.FieldDoc) hit).fields[0];
                    assertEquals("descending order at position " + i, rowCount - 1 - i, value);
                }
            }
        }
    }
}

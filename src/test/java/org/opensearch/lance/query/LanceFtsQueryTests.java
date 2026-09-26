/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.FullTextQuery;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceFragmentLeafReader;
import org.opensearch.test.OpenSearchTestCase;

// The fragment coverage tests open a real Lance dataset; Lance JNI
// spins up native worker threads that outlive the test method.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFtsQueryTests extends OpenSearchTestCase {

    public void testEqualsAndHashCode() {
        LanceFtsQuery a = new LanceFtsQuery("body", "camera");
        LanceFtsQuery b = new LanceFtsQuery("body", "camera");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        assertNotEquals(a, new LanceFtsQuery("title", "camera"));
        assertNotEquals(a, new LanceFtsQuery("body", "phone"));
    }

    public void testToStringIsHumanReadable() {
        LanceFtsQuery query = new LanceFtsQuery("body", "camera");
        String text = query.toString("body");
        assertTrue("toString should mention the column, saw: " + text, text.contains("body"));
        assertTrue("toString should mention the query text, saw: " + text, text.contains("camera"));
    }

    public void testVisitorReceivesLeaf() {
        LanceFtsQuery query = new LanceFtsQuery("body", "camera");
        AtomicInteger leafCalls = new AtomicInteger();
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(org.apache.lucene.search.Query q) {
                leafCalls.incrementAndGet();
                assertSame(query, q);
            }
        });
        assertEquals("expected exactly one visitLeaf call", 1, leafCalls.get());
    }

    public void testPhraseQueryDiffersFromMatchQuery() {
        // phrase / non-phrase are distinct queries even when column and
        // text agree, because Lance routes them to different FullTextQuery
        // constructors. Making equals reflect that keeps query cache keys
        // and _explain output honest.
        LanceFtsQuery match = new LanceFtsQuery("body", "quick brown");
        LanceFtsQuery phrase = new LanceFtsQuery("body", "quick brown", true, 0);
        assertNotEquals(match, phrase);
        assertNotEquals(match.hashCode(), phrase.hashCode());
        String phraseText = phrase.toString("body");
        assertTrue("phrase toString should mention phrase, saw: " + phraseText, phraseText.contains("phrase"));
    }

    public void testPhraseSlopParticipatesInEqualsAndHashCode() {
        LanceFtsQuery slop0 = new LanceFtsQuery("body", "quick brown", true, 0);
        LanceFtsQuery slop2 = new LanceFtsQuery("body", "quick brown", true, 2);
        assertNotEquals(slop0, slop2);
        // Slop is clamped to a non-negative int; negative values collapse to 0.
        LanceFtsQuery slopNegative = new LanceFtsQuery("body", "quick brown", true, -5);
        assertEquals(slop0, slopNegative);
    }

    public void testOperatorParticipatesInEqualsAndHashCode() {
        LanceFtsQuery orQuery = new LanceFtsQuery("body", "quick brown");
        LanceFtsQuery andQuery = new LanceFtsQuery("body", "quick brown", FullTextQuery.Operator.AND);
        assertNotEquals(orQuery, andQuery);
        assertNotEquals(orQuery.hashCode(), andQuery.hashCode());
        String andText = andQuery.toString("body");
        assertTrue("AND toString should mention the operator, saw: " + andText, andText.contains("AND"));
    }

    public void testNullOperatorFallsBackToOr() {
        // The constructor tolerates a null operator so callers do not have
        // to guard when threading through code paths where OR is the
        // default. Equality with the explicit-OR form documents that.
        LanceFtsQuery a = new LanceFtsQuery("body", "camera", false, 0, null);
        LanceFtsQuery b = new LanceFtsQuery("body", "camera", false, 0, FullTextQuery.Operator.OR);
        assertEquals(a, b);
    }

    public void testScanFilterSqlParticipatesInEqualsHashCodeAndToString() {
        LanceFtsQuery plain = new LanceFtsQuery("body", "camera");
        LanceFtsQuery filtered = plain.withScanFilterSql("rating = 5");
        LanceFtsQuery filteredAgain = new LanceFtsQuery(FullTextQuery.match("camera", "body"), Set.of("body"), 0, "rating = 5");

        assertNotEquals(plain, filtered);
        assertNotEquals(plain.hashCode(), filtered.hashCode());
        assertEquals(filtered, filteredAgain);
        assertEquals(filtered.hashCode(), filteredAgain.hashCode());
        assertNotEquals(filtered, plain.withScanFilterSql("rating = 4"));

        assertNull(plain.scanFilterSql());
        assertEquals("rating = 5", filtered.scanFilterSql());
        assertSame("same prefilter returns the same instance", filtered, filtered.withScanFilterSql("rating = 5"));
        assertEquals("null removes the prefilter", plain, filtered.withScanFilterSql(null));

        // The scan limit copy keeps the prefilter, so the count-path
        // copy (withScanLimit(UNBOUNDED)) still filters the same rows.
        LanceFtsQuery limited = filtered.withScanLimit(10);
        assertEquals("rating = 5", limited.scanFilterSql());
        assertEquals(10, limited.scanLimit());
        assertEquals(filtered, limited.withScanLimit(LanceFtsQuery.SCAN_LIMIT_UNBOUNDED));

        String text = filtered.toString("body");
        assertTrue("toString should mention the prefilter, saw: " + text, text.contains("rating = 5"));
        assertFalse(plain.toString("body").contains("prefilter"));
    }

    public void testDirectFullTextQueryConstructorAndColumnsCollection() {
        // A LanceFtsQuery built from a MatchQuery with fuzziness must not
        // compare equal to one without it, because the parameter change
        // reaches Lance and can change the result set. Documenting this
        // through equality keeps _explain output honest for callers who
        // rely on canonical DSL representations.
        FullTextQuery plain = FullTextQuery.match("hello", "body");
        FullTextQuery fuzzy = FullTextQuery.match("hello", "body", 1f, Optional.of(1), 50, FullTextQuery.Operator.OR, 0);
        LanceFtsQuery plainQuery = new LanceFtsQuery(plain, Set.of("body"));
        LanceFtsQuery fuzzyQuery = new LanceFtsQuery(fuzzy, Set.of("body"));
        assertNotEquals(plainQuery, fuzzyQuery);
    }

    public void testCollectColumnsWalksBooleanTree() {
        FullTextQuery bodyMatch = FullTextQuery.match("hello", "body");
        FullTextQuery titleMatch = FullTextQuery.match("world", "title");
        FullTextQuery combined = FullTextQuery.booleanQuery(
            List.of(
                new FullTextQuery.BooleanClause(FullTextQuery.Occur.MUST, bodyMatch),
                new FullTextQuery.BooleanClause(FullTextQuery.Occur.SHOULD, titleMatch)
            )
        );
        Set<String> columns = LanceFtsQuery.collectColumns(combined);
        assertEquals(Set.of("body", "title"), columns);
    }

    public void testCollectColumnsWalksBoostTree() {
        FullTextQuery pos = FullTextQuery.match("hello", "body");
        FullTextQuery neg = FullTextQuery.match("stale", "title");
        FullTextQuery boosted = FullTextQuery.boost(pos, neg, 0.5f);
        Set<String> columns = LanceFtsQuery.collectColumns(boosted);
        assertEquals(Set.of("body", "title"), columns);
    }

    public void testCoversAllFragmentsWhenExecutorHoldsEveryFragment() throws Exception {
        // 12 rows, 4 per file: fragments 0, 1, 2. An executor that
        // holds exactly that set covers the dataset, and the scan
        // builder must stay free of a fragmentIds restriction.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "coverage-all", 12, 4);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<Integer> all = fragmentIdsOf(dataset);
            assertEquals("fixture must produce three fragments", 3, all.size());

            assertTrue(LanceFtsQuery.coversAllFragments(all, dataset));
            // Order and duplicates do not matter: coverage is a set check.
            List<Integer> shuffled = new ArrayList<>(all);
            Collections.reverse(shuffled);
            shuffled.add(all.get(0));
            assertTrue(LanceFtsQuery.coversAllFragments(shuffled, dataset));

            ScanOptions options = LanceFtsQuery.restrictToFragmentsUnlessAll(new ScanOptions.Builder(), all, dataset).build();
            assertFalse("full coverage must not set fragmentIds", options.getFragmentIds().isPresent());
        }
    }

    public void testCoversAllFragmentsIsFalseForProperSubset() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "coverage-subset", 12, 4);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<Integer> all = fragmentIdsOf(dataset);
            assertEquals(3, all.size());

            // Drop one fragment: the remaining two have the same size
            // as a two-node round-robin slice and must be reported as
            // a subset regardless of which fragment is missing.
            for (int missing = 0; missing < all.size(); missing++) {
                List<Integer> subset = new ArrayList<>(all);
                subset.remove(missing);
                assertFalse("subset missing fragment " + all.get(missing), LanceFtsQuery.coversAllFragments(subset, dataset));

                ScanOptions options = LanceFtsQuery.restrictToFragmentsUnlessAll(new ScanOptions.Builder(), subset, dataset).build();
                assertEquals("subset must be passed through as fragmentIds", Optional.of(subset), options.getFragmentIds());
            }
            assertFalse("empty executor set never covers a non-empty dataset", LanceFtsQuery.coversAllFragments(List.of(), dataset));
        }
    }

    public void testCoversAllFragmentsIgnoresIdsUnknownToDataset() throws Exception {
        // A superset that carries ids the manifest does not know (for
        // example a fragment id the coordinator saw before a
        // compaction rewrote the table) still covers every fragment
        // the dataset has, so the scan can run unrestricted. A size
        // comparison alone would call this a mismatch.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "coverage-superset", 12, 4);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<Integer> all = fragmentIdsOf(dataset);
            assertEquals(3, all.size());

            List<Integer> superset = new ArrayList<>(all);
            superset.add(1_000);
            superset.add(1_001);
            assertTrue(LanceFtsQuery.coversAllFragments(superset, dataset));
            ScanOptions options = LanceFtsQuery.restrictToFragmentsUnlessAll(new ScanOptions.Builder(), superset, dataset).build();
            assertFalse(options.getFragmentIds().isPresent());

            // Same size as the dataset but one real id swapped for an
            // unknown one: not a cover, because a real fragment is
            // missing. This is the case a size comparison gets wrong.
            List<Integer> sameSizeButMissingOne = new ArrayList<>(all.subList(1, all.size()));
            sameSizeButMissingOne.add(1_000);
            assertEquals(all.size(), sameSizeButMissingOne.size());
            assertFalse(LanceFtsQuery.coversAllFragments(sameSizeButMissingOne, dataset));
        }
    }

    public void testFtsScanReturnsOnlySubsetRowsWithRestrictionAndAllRowsWithout() throws Exception {
        // Drive Lance with the same ScanOptions shape ensureShardScan
        // builds (fullTextQuery + row address + limit) through both
        // branches of restrictToFragmentsUnlessAll. "hello" sits in
        // the even rows, two per fragment of four rows.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "coverage-scan", 12, 4);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<Integer> all = fragmentIdsOf(dataset);
            assertEquals(3, all.size());

            Map<Integer, Integer> unrestricted = hitsByFragment(dataset, all);
            assertEquals("two hello rows per fragment", Map.of(all.get(0), 2, all.get(1), 2, all.get(2), 2), unrestricted);

            List<Integer> subset = List.of(all.get(0), all.get(2));
            Map<Integer, Integer> restricted = hitsByFragment(dataset, subset);
            assertEquals("restricted scan must not return rows of the excluded fragment", Map.of(all.get(0), 2, all.get(2), 2), restricted);
        }
    }

    public void testFtsScanWithSqlPrefilterReturnsOnlyRowsPassingThePredicate() throws Exception {
        // The ScanOptions shape ensureShardScan builds for a collapsed
        // bool query: fullTextQuery + filter(sql) + prefilter(true) +
        // row address + limit. "hello" sits in the even rows; the
        // predicate keeps rows 4..11, so fragments 1 and 2 answer two
        // rows each and fragment 0 answers none. The count-only shape
        // (no columns, no row address) must agree on the total.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "prefilter-scan", 12, 4);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<Integer> all = fragmentIdsOf(dataset);
            assertEquals(3, all.size());

            ScanOptions hitsOptions = new ScanOptions.Builder().fullTextQuery(FullTextQuery.match("hello", "body"))
                .filter("(id >= 4 AND NOT (id = 6))")
                .prefilter(true)
                .withRowAddress(true)
                .limit(100L)
                .build();
            Map<Integer, Integer> counts = new HashMap<>();
            try (LanceScanner scanner = dataset.newScan(hitsOptions); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    assertNotNull("prefiltered FTS scan must still return _score", root.getVector("_score"));
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        counts.merge((int) (rowAddr.get(i) >>> 32), 1, Integer::sum);
                    }
                }
            }
            assertEquals("rows 4, 8, 10 pass the predicate", Map.of(all.get(1), 1, all.get(2), 2), counts);

            ScanOptions countOptions = new ScanOptions.Builder().fullTextQuery(FullTextQuery.match("hello", "body"))
                .filter("(id >= 4 AND NOT (id = 6))")
                .prefilter(true)
                .columns(Collections.emptyList())
                .withRowAddress(false)
                .withRowId(false)
                .build();
            long total = 0L;
            try (LanceScanner scanner = dataset.newScan(countOptions); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    total += reader.getVectorSchemaRoot().getRowCount();
                }
            }
            assertEquals(3L, total);
        }
    }

    public void testWeightHitCountReportsScannedRowsAcrossFragments() throws Exception {
        // The fragment executor reads hits.total from the Weight it
        // built for the request instead of counting in a second scan.
        // Before any leaf is scored the Weight has not scanned and
        // reports -1; after the first scorerSupplier call the shard
        // scan has run and hitCount covers every fragment of the
        // reader, bounded by scanLimit when one is set.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "weight-hit-count", 12, 4);
        // The reader takes ownership of the Dataset and closes it.
        Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        try (
            LanceDirectoryReader reader = LanceDirectoryReader.openForFragments(
                new ByteBuffersDirectory(),
                null,
                dataset,
                "",
                LanceEngineFactory.LancePrimaryKeyType.NONE,
                LanceOverrides.EMPTY,
                fragmentIdsOf(dataset)
            )
        ) {
            assertEquals("fixture must expose three leaves", 3, reader.leaves().size());
            IndexSearcher searcher = new IndexSearcher(reader);

            LanceFtsQuery unbounded = new LanceFtsQuery("body", "hello");
            LanceFtsQuery.LanceFtsWeight weight = (LanceFtsQuery.LanceFtsWeight) searcher.createWeight(
                searcher.rewrite(unbounded),
                ScoreMode.COMPLETE,
                1f
            );
            assertEquals("no leaf scored yet", -1L, weight.hitCount());
            weight.scorerSupplier(reader.leaves().get(0));
            assertEquals("two hello rows in each of three fragments", 6L, weight.hitCount());
            // Later leaves reuse the cached scan; the count does not change.
            weight.scorerSupplier(reader.leaves().get(2));
            assertEquals(6L, weight.hitCount());

            LanceFtsQuery bounded = unbounded.withScanLimit(2);
            LanceFtsQuery.LanceFtsWeight boundedWeight = (LanceFtsQuery.LanceFtsWeight) searcher.createWeight(
                searcher.rewrite(bounded),
                ScoreMode.COMPLETE,
                1f
            );
            boundedWeight.scorerSupplier(reader.leaves().get(0));
            assertEquals("scanLimit clips the scan, so the count stops at the limit", 2L, boundedWeight.hitCount());
            assertFalse("a scan that filled its limit is not complete", boundedWeight.complete());

            // A bound above the match count leaves the count exact,
            // which is what lets the executor skip its count scan when
            // a bounded hits scan comes back short of its limit.
            LanceFtsQuery wide = unbounded.withScanLimit(10);
            LanceFtsQuery.LanceFtsWeight wideWeight = (LanceFtsQuery.LanceFtsWeight) searcher.createWeight(
                searcher.rewrite(wide),
                ScoreMode.COMPLETE,
                1f
            );
            wideWeight.scorerSupplier(reader.leaves().get(1));
            assertEquals(6L, wideWeight.hitCount());
            assertTrue("a bounded scan short of its limit saw every match", wideWeight.complete());
            assertTrue(weight.complete());
        }
    }

    /**
     * Open a reader over {@code fragmentIds} of the table at
     * {@code uri} (every fragment when {@code null}); the reader takes
     * ownership of the Dataset.
     */
    private static LanceDirectoryReader openReader(String uri, List<Integer> fragmentIds) throws Exception {
        Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        return LanceDirectoryReader.openForFragments(
            new ByteBuffersDirectory(),
            null,
            dataset,
            "",
            LanceEngineFactory.LancePrimaryKeyType.NONE,
            LanceOverrides.EMPTY,
            fragmentIds == null ? fragmentIdsOf(dataset) : fragmentIds
        );
    }

    private static LanceFtsQuery.LanceFtsWeight weightOf(IndexSearcher searcher, LanceFtsQuery query) throws Exception {
        return (LanceFtsQuery.LanceFtsWeight) searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1f);
    }

    /** Doc ids the Weight's scorer yields on {@code leaf}, in iteration order. */
    private static List<Integer> docIdsOn(LanceFtsQuery.LanceFtsWeight weight, LeafReaderContext leaf) throws Exception {
        List<Integer> docIds = new ArrayList<>();
        ScorerSupplier supplier = weight.scorerSupplier(leaf);
        DocIdSetIterator iterator = supplier.get(Long.MAX_VALUE).iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docIds.add(doc);
        }
        return docIds;
    }

    private static LeafReaderContext leafOfFragment(LanceDirectoryReader reader, int fragmentId) {
        for (LeafReaderContext leaf : reader.leaves()) {
            LanceFragmentLeafReader fragmentLeaf = LanceFragmentLeafReader.unwrap(leaf.reader());
            if (fragmentLeaf != null && fragmentLeaf.fragmentId() == fragmentId) {
                return leaf;
            }
        }
        throw new AssertionError("no leaf for fragment " + fragmentId);
    }

    public void testBoundedScanOnSubsetReaderRunsUnrestrictedAndKeepsOwnRows() throws Exception {
        // Interleaved fixture: row i sits in fragment i % 3 at offset
        // i / 3, and "lance" matches every row with a score that grows
        // with i. A reader over fragments 0 and 2 holds ids 0, 3, 6, 9
        // and 2, 5, 8, 11. A page of 5 asks Lance for the top 20 (the
        // page times the two executors the fragment counts imply,
        // times the slack of two); the 12 matches come back short of
        // that, so every own row is kept and the count is exact.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeInterleavedTable(scratchDir, "subset-bounded", 3, 4);
        try (LanceDirectoryReader reader = openReader(uri, List.of(0, 2))) {
            assertEquals(2, reader.leaves().size());
            IndexSearcher searcher = new IndexSearcher(reader);
            LanceFtsQuery query = new LanceFtsQuery("body", "lance");

            LanceFtsQuery.LanceFtsWeight top5 = weightOf(searcher, query.withScanLimit(5));
            assertFalse("no scan before the first leaf is scored", top5.complete());
            assertEquals(List.of(0, 1, 2, 3), docIdsOn(top5, leafOfFragment(reader, 0)));
            assertEquals(List.of(0, 1, 2, 3), docIdsOn(top5, leafOfFragment(reader, 2)));
            assertEquals(8L, top5.hitCount());
            assertTrue("Lance returned fewer rows than the limit, so every match was seen", top5.complete());
            assertEquals("one scan", 1, top5.issuedScans().size());
            ScanOptions options = top5.issuedScans().get(0);
            assertFalse("subset reader must not restrict the scan: " + options, options.getFragmentIds().isPresent());
            assertEquals(Optional.of(20L), options.getLimit());
            assertTrue(options.isWithRowAddress());
        }
    }

    public void testBoundedScanOnSubsetReaderWidensUntilItHoldsThePage() throws Exception {
        // Contiguous fixture with the score falling with the id: the
        // 90 rows of fragments 0, 1 and 2 hold ids 0..29, 30..59 and
        // 60..89, and the global top list is filled fragment by
        // fragment from fragment 0. A reader over fragment 2 alone
        // asks for a page of 5: the first scan of 30 rows (5 × 3 × 2)
        // is fragment 0 only, the second of 60 is fragments 0 and 1,
        // and the third of 120 returns all 90 matches, so the reader's
        // 30 rows are kept and the count is exact. Keeping the
        // reader's share of one global top 5 would have returned no
        // row at all for a page five rows deep.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeContiguousScoreTable(scratchDir, "subset-widen", 3, 30, false);
        try (LanceDirectoryReader reader = openReader(uri, List.of(2))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            LanceFtsQuery.LanceFtsWeight weight = weightOf(searcher, new LanceFtsQuery("body", "lance").withScanLimit(5));
            List<Integer> docIds = docIdsOn(weight, leafOfFragment(reader, 2));
            assertEquals(30, docIds.size());
            assertEquals(30L, weight.hitCount());
            assertTrue(weight.complete());
            List<ScanOptions> scans = weight.issuedScans();
            assertEquals("three widening scans: " + scans, 3, scans.size());
            assertEquals(Optional.of(30L), scans.get(0).getLimit());
            assertEquals(Optional.of(60L), scans.get(1).getLimit());
            assertEquals(Optional.of(120L), scans.get(2).getLimit());
            for (ScanOptions scan : scans) {
                assertFalse("no scan carries the fragment restriction: " + scan, scan.getFragmentIds().isPresent());
            }
        }
    }

    public void testBoundedScanOnSubsetReaderFallsBackToRestrictedScanAtTheProbeLimit() throws Exception {
        // Same fixture and reader as above with the probe cap at one
        // row, so the first scan of 30 already reaches the cap: it
        // fills with fragment 0 and holds no row of fragment 2, and
        // the scan is repeated with the fragment restriction and the
        // page as limit, which returns the reader's own top 5 (its
        // lowest ids, 60..64) and fills, so the count is a bound.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeContiguousScoreTable(scratchDir, "subset-restricted", 3, 30, false);
        int before = LanceFtsQuery.subsetProbeLimit();
        LanceFtsQuery.setSubsetProbeLimit(1);
        try (LanceDirectoryReader reader = openReader(uri, List.of(2))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            LanceFtsQuery.LanceFtsWeight weight = weightOf(searcher, new LanceFtsQuery("body", "lance").withScanLimit(5));
            assertEquals(List.of(0, 1, 2, 3, 4), docIdsOn(weight, leafOfFragment(reader, 2)));
            assertEquals(5L, weight.hitCount());
            assertFalse("the restricted scan filled its limit", weight.complete());
            List<ScanOptions> scans = weight.issuedScans();
            assertEquals("probe then restricted scan: " + scans, 2, scans.size());
            assertEquals(Optional.of(30L), scans.get(0).getLimit());
            assertFalse(scans.get(0).getFragmentIds().isPresent());
            assertEquals(Optional.of(5L), scans.get(1).getLimit());
            assertEquals(Optional.of(List.of(2)), scans.get(1).getFragmentIds());
        } finally {
            LanceFtsQuery.setSubsetProbeLimit(before);
        }
    }

    public void testBoundedScanOnFullReaderAsksForThePageOnly() throws Exception {
        // A reader over every fragment has nothing to filter: one scan
        // with the page as limit, filled, so the count is a bound.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeContiguousScoreTable(scratchDir, "full-bounded", 3, 30, false);
        try (LanceDirectoryReader reader = openReader(uri, List.of(0, 1, 2))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            LanceFtsQuery.LanceFtsWeight weight = weightOf(searcher, new LanceFtsQuery("body", "lance").withScanLimit(5));
            assertEquals(List.of(0, 1, 2, 3, 4), docIdsOn(weight, leafOfFragment(reader, 0)));
            assertEquals(5L, weight.hitCount());
            assertFalse(weight.complete());
            assertEquals(1, weight.issuedScans().size());
            assertEquals(Optional.of(5L), weight.issuedScans().get(0).getLimit());
            assertFalse(weight.issuedScans().get(0).getFragmentIds().isPresent());
        }
    }

    public void testBoundedProbeLimitIsThePageTimesTheExecutorCountTimesTheSlack() {
        assertEquals("one of three fragments: three executors", 60L, LanceFtsQuery.boundedProbeLimit(10, 1, 3));
        assertEquals("two of three fragments: two executors", 40L, LanceFtsQuery.boundedProbeLimit(10, 2, 3));
        assertEquals("62 of 250 fragments: five executors", 100L, LanceFtsQuery.boundedProbeLimit(10, 62, 250));
        assertEquals("every fragment: the slack alone", 20L, LanceFtsQuery.boundedProbeLimit(10, 3, 3));
        assertEquals("a page of one on two executors", 4L, LanceFtsQuery.boundedProbeLimit(1, 1, 2));
        assertEquals(
            "a fragment count the dataset does not know is treated as the whole table",
            20L,
            LanceFtsQuery.boundedProbeLimit(10, 4, 3)
        );
    }

    public void testUnboundedScanOnSubsetReaderProbesWithoutFragmentIds() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeInterleavedTable(scratchDir, "subset-probe", 3, 4);
        try (LanceDirectoryReader reader = openReader(uri, List.of(0, 2))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            LanceFtsQuery.LanceFtsWeight weight = weightOf(searcher, new LanceFtsQuery("body", "lance"));
            assertEquals(List.of(0, 1, 2, 3), docIdsOn(weight, leafOfFragment(reader, 2)));
            assertEquals("every row of fragments 0 and 2", 8L, weight.hitCount());
            assertTrue("the probe came back short of its limit", weight.complete());
            assertEquals("one probe scan is enough", 1, weight.issuedScans().size());
            ScanOptions probe = weight.issuedScans().get(0);
            assertFalse(probe.getFragmentIds().isPresent());
            assertEquals(
                "eight rows covered: the floor of the effective probe limit",
                Optional.of((long) LanceFtsQuery.DEFAULT_SUBSET_PROBE_MIN_ROWS),
                probe.getLimit()
            );
        }
    }

    public void testUnboundedScanFallsBackToRestrictedScanWhenProbeFills() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeInterleavedTable(scratchDir, "subset-fallback", 3, 4);
        int before = LanceFtsQuery.subsetProbeLimit();
        LanceFtsQuery.setSubsetProbeLimit(2);
        try (LanceDirectoryReader reader = openReader(uri, List.of(0, 2))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            LanceFtsQuery.LanceFtsWeight weight = weightOf(searcher, new LanceFtsQuery("body", "lance"));
            // 12 matches fill a probe of 2, so the probe rows are
            // discarded and the restricted scan supplies every row of
            // the reader's fragments exactly once.
            assertEquals(List.of(0, 1, 2, 3), docIdsOn(weight, leafOfFragment(reader, 0)));
            assertEquals(List.of(0, 1, 2, 3), docIdsOn(weight, leafOfFragment(reader, 2)));
            assertEquals(8L, weight.hitCount());
            assertTrue(weight.complete());
            assertEquals("probe then restricted scan", 2, weight.issuedScans().size());
            ScanOptions probe = weight.issuedScans().get(0);
            assertFalse(probe.getFragmentIds().isPresent());
            assertEquals(Optional.of(2L), probe.getLimit());
            ScanOptions restricted = weight.issuedScans().get(1);
            assertEquals(Optional.of(List.of(0, 2)), restricted.getFragmentIds());
            assertFalse("the restricted scan is unbounded", restricted.getLimit().isPresent());
        } finally {
            LanceFtsQuery.setSubsetProbeLimit(before);
        }
    }

    public void testFullReaderKeepsSingleUnrestrictedScan() throws Exception {
        // A reader over every fragment neither filters nor probes: one
        // unbounded scan without fragmentIds, as before.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeInterleavedTable(scratchDir, "full-reader", 3, 4);
        int before = LanceFtsQuery.subsetProbeLimit();
        LanceFtsQuery.setSubsetProbeLimit(2);
        try (LanceDirectoryReader reader = openReader(uri, List.of(0, 1, 2))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            LanceFtsQuery.LanceFtsWeight weight = weightOf(searcher, new LanceFtsQuery("body", "lance"));
            assertEquals(List.of(0, 1, 2, 3), docIdsOn(weight, leafOfFragment(reader, 1)));
            assertEquals(12L, weight.hitCount());
            assertTrue(weight.complete());
            assertEquals(1, weight.issuedScans().size());
            ScanOptions options = weight.issuedScans().get(0);
            assertFalse(options.getFragmentIds().isPresent());
            assertFalse("no probe limit on a full reader", options.getLimit().isPresent());
        } finally {
            LanceFtsQuery.setSubsetProbeLimit(before);
        }
    }

    public void testSubsetProbeLimitRejectsValuesBelowOne() {
        int before = LanceFtsQuery.subsetProbeLimit();
        try {
            expectThrows(IllegalArgumentException.class, () -> LanceFtsQuery.setSubsetProbeLimit(0));
            LanceFtsQuery.setSubsetProbeLimit(1);
            assertEquals(1, LanceFtsQuery.subsetProbeLimit());
        } finally {
            LanceFtsQuery.setSubsetProbeLimit(before);
        }
    }

    public void testSubsetProbeRatioAndMinRowsRejectValuesOutOfRange() {
        double ratioBefore = LanceFtsQuery.subsetProbeRatio();
        int minRowsBefore = LanceFtsQuery.subsetProbeMinRows();
        try {
            expectThrows(IllegalArgumentException.class, () -> LanceFtsQuery.setSubsetProbeRatio(-0.1d));
            expectThrows(IllegalArgumentException.class, () -> LanceFtsQuery.setSubsetProbeRatio(1.1d));
            expectThrows(IllegalArgumentException.class, () -> LanceFtsQuery.setSubsetProbeRatio(Double.NaN));
            expectThrows(IllegalArgumentException.class, () -> LanceFtsQuery.setSubsetProbeMinRows(0));
            LanceFtsQuery.setSubsetProbeRatio(0d);
            LanceFtsQuery.setSubsetProbeRatio(1d);
            LanceFtsQuery.setSubsetProbeMinRows(1);
            assertEquals(1d, LanceFtsQuery.subsetProbeRatio(), 0d);
            assertEquals(1, LanceFtsQuery.subsetProbeMinRows());
        } finally {
            LanceFtsQuery.setSubsetProbeRatio(ratioBefore);
            LanceFtsQuery.setSubsetProbeMinRows(minRowsBefore);
        }
    }

    public void testEffectiveSubsetProbeLimitFollowsTheCoveredRows() {
        // Defaults: ratio 0.03, floor 10,000, cap 1,000,000.
        assertEquals("2M table, 3 of 8 fragments (750k rows): 3 percent", 22_500L, LanceFtsQuery.effectiveSubsetProbeLimit(750_000L));
        assertEquals("20M table on 3 nodes (6,666,667 rows): 3 percent", 200_000L, LanceFtsQuery.effectiveSubsetProbeLimit(6_666_667L));
        assertEquals("3 percent of 100k rows is below the floor", 10_000L, LanceFtsQuery.effectiveSubsetProbeLimit(100_000L));
        assertEquals("a reader over a handful of rows gets the floor", 10_000L, LanceFtsQuery.effectiveSubsetProbeLimit(8L));
        assertEquals("the cap holds at 100M rows", 1_000_000L, LanceFtsQuery.effectiveSubsetProbeLimit(100_000_000L));

        int limitBefore = LanceFtsQuery.subsetProbeLimit();
        double ratioBefore = LanceFtsQuery.subsetProbeRatio();
        int minRowsBefore = LanceFtsQuery.subsetProbeMinRows();
        try {
            LanceFtsQuery.setSubsetProbeRatio(0.1d);
            assertEquals(75_000L, LanceFtsQuery.effectiveSubsetProbeLimit(750_000L));
            LanceFtsQuery.setSubsetProbeMinRows(100_000);
            assertEquals("the floor wins over the ratio", 100_000L, LanceFtsQuery.effectiveSubsetProbeLimit(750_000L));
            LanceFtsQuery.setSubsetProbeLimit(50);
            assertEquals("the cap wins over the floor", 50L, LanceFtsQuery.effectiveSubsetProbeLimit(750_000L));
            LanceFtsQuery.setSubsetProbeRatio(0d);
            LanceFtsQuery.setSubsetProbeLimit(limitBefore);
            assertEquals("ratio 0 leaves the floor", 100_000L, LanceFtsQuery.effectiveSubsetProbeLimit(750_000L));
        } finally {
            LanceFtsQuery.setSubsetProbeLimit(limitBefore);
            LanceFtsQuery.setSubsetProbeRatio(ratioBefore);
            LanceFtsQuery.setSubsetProbeMinRows(minRowsBefore);
        }
    }

    public void testHitsScanReturnsRowAddressAndScoreOnly() throws Exception {
        // The Weight's scan projects _score and asks for the row
        // address; the batches Lance hands back must carry exactly
        // those two columns (no body, title, embedding), and the
        // Weight must still produce the same hits from them.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "projection", 12, 4);
        try (LanceDirectoryReader reader = openReader(uri, null)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            LanceFtsQuery.LanceFtsWeight weight = weightOf(searcher, new LanceFtsQuery("body", "hello"));
            assertEquals("hello sits in the even rows", List.of(0, 2), docIdsOn(weight, reader.leaves().get(0)));
            assertEquals(6L, weight.hitCount());

            ScanOptions issued = weight.issuedScans().get(0);
            assertEquals(Optional.of(LanceFtsQuery.HITS_SCAN_COLUMNS), issued.getColumns());
            assertTrue(issued.isWithRowAddress());
            Dataset dataset = LanceFragmentLeafReader.unwrap(reader.leaves().get(0).reader()).dataset();
            assertEquals(Set.of("_rowaddr", "_score"), columnsReturnedBy(dataset, issued));
        }
    }

    public void testHitBuffersAreReservedWithTheRequestBreakerAndReleasedOnClose() throws Exception {
        // Three fragments with two hello rows each. The first scored
        // leaf runs the scan: each fragment's buffer starts at the
        // initial capacity, and the leaf's sorted view is built on top.
        // Every other leaf adds only its sorted view. Closing the
        // accounting (what the executor does with its search context)
        // returns every byte.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "accounting", 12, 4);
        CircuitBreaker breaker = requestBreaker("1mb");
        try (LanceDirectoryReader reader = openReader(uri, null); AccountingSearcher searcher = new AccountingSearcher(reader, breaker)) {
            assertEquals(3, reader.leaves().size());
            LanceFtsQuery.LanceFtsWeight weight = weightOf(searcher, new LanceFtsQuery("body", "hello"));
            assertEquals("nothing reserved before the scan", 0L, breaker.getUsed());

            long bufferBytes = 3L * LanceFragmentHits.INITIAL_CAPACITY * LanceFragmentHits.BYTES_PER_HIT;
            long sortedBytesPerLeaf = 2L * LanceFragmentHits.BYTES_PER_HIT;
            assertEquals(List.of(0, 2), docIdsOn(weight, reader.leaves().get(0)));
            assertEquals(bufferBytes + sortedBytesPerLeaf, breaker.getUsed());
            assertEquals(breaker.getUsed(), searcher.accounting.reservedBytes());

            assertEquals(List.of(0, 2), docIdsOn(weight, reader.leaves().get(1)));
            assertEquals(List.of(0, 2), docIdsOn(weight, reader.leaves().get(2)));
            assertEquals(bufferBytes + 3L * sortedBytesPerLeaf, breaker.getUsed());
            // A second scorer on a leaf reuses the sorted arrays.
            assertEquals(List.of(0, 2), docIdsOn(weight, reader.leaves().get(1)));
            assertEquals(bufferBytes + 3L * sortedBytesPerLeaf, breaker.getUsed());

            searcher.accounting.close();
            assertEquals("close returns every reserved byte", 0L, breaker.getUsed());
            assertEquals(0L, searcher.accounting.reservedBytes());
        }
    }

    public void testBreakerRefusalSurfacesAsCircuitBreakingExceptionAndReleasesTheReservation() throws Exception {
        // A limit below the buffers of two fragments: the first
        // fragment's buffer fits, the second is refused. The exception
        // must reach the caller as CircuitBreakingException, not
        // wrapped in the IOException the scan uses for Lance failures,
        // and the bytes reserved before the refusal must be released
        // when the accounting closes.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "refusal", 12, 4);
        long oneBuffer = (long) LanceFragmentHits.INITIAL_CAPACITY * LanceFragmentHits.BYTES_PER_HIT;
        CircuitBreaker breaker = requestBreaker((oneBuffer + oneBuffer / 2) + "b");
        try (LanceDirectoryReader reader = openReader(uri, null); AccountingSearcher searcher = new AccountingSearcher(reader, breaker)) {
            LanceFtsQuery.LanceFtsWeight weight = weightOf(searcher, new LanceFtsQuery("body", "hello"));
            LeafReaderContext leaf = reader.leaves().get(0);
            CircuitBreakingException refused = expectThrows(CircuitBreakingException.class, () -> weight.scorerSupplier(leaf));
            assertTrue(refused.getMessage(), refused.getMessage().contains(LanceHitsAccounting.LABEL));
            assertEquals("the refused reservation is not counted", oneBuffer, breaker.getUsed());
            assertEquals(1L, breaker.getTrippedCount());
            assertEquals("the Weight installed no scan", -1L, weight.hitCount());

            searcher.accounting.close();
            assertEquals(0L, breaker.getUsed());
        }
    }

    public void testDiscardedProbeReturnsItsBytesBeforeTheRestrictedScan() throws Exception {
        // Subset reader over fragments 0 and 2 of the interleaved
        // fixture, probe limit 2: the probe returns two rows (at least
        // one of them from a kept fragment, so it allocated a buffer),
        // fills up, and is discarded. What stays reserved afterwards is
        // the restricted scan's buffers alone, one per fragment of the
        // reader, plus the sorted view of the scored leaf.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeInterleavedTable(scratchDir, "probe-release", 3, 4);
        int before = LanceFtsQuery.subsetProbeLimit();
        LanceFtsQuery.setSubsetProbeLimit(2);
        CircuitBreaker breaker = requestBreaker("1mb");
        try (
            LanceDirectoryReader reader = openReader(uri, List.of(0, 2));
            AccountingSearcher searcher = new AccountingSearcher(reader, breaker)
        ) {
            LanceFtsQuery.LanceFtsWeight weight = weightOf(searcher, new LanceFtsQuery("body", "lance"));
            assertEquals(List.of(0, 1, 2, 3), docIdsOn(weight, leafOfFragment(reader, 0)));
            assertEquals("probe then restricted scan", 2, weight.issuedScans().size());
            long restrictedBuffers = 2L * LanceFragmentHits.INITIAL_CAPACITY * LanceFragmentHits.BYTES_PER_HIT;
            long sortedLeaf = 4L * LanceFragmentHits.BYTES_PER_HIT;
            assertEquals(restrictedBuffers + sortedLeaf, breaker.getUsed());
            assertEquals(breaker.getUsed(), searcher.accounting.reservedBytes());

            searcher.accounting.close();
            assertEquals(0L, breaker.getUsed());
        } finally {
            LanceFtsQuery.setSubsetProbeLimit(before);
        }
    }

    /**
     * A {@code request} breaker with {@code limit} from the real
     * hierarchy service, so the message and the tripped count are
     * the ones a node produces. Real memory tracking is off: the
     * parent then sums the children instead of reading the heap.
     */
    static CircuitBreaker requestBreaker(String limit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new HierarchyCircuitBreakerService(settings, List.of(), clusterSettings).getBreaker(CircuitBreaker.REQUEST);
    }

    /** Names of the columns Lance returns for {@code options}, over every batch. */
    static Set<String> columnsReturnedBy(Dataset dataset, ScanOptions options) throws Exception {
        Set<String> names = new LinkedHashSet<>();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                for (Field field : reader.getVectorSchemaRoot().getSchema().getFields()) {
                    names.add(field.getName());
                }
            }
        }
        return names;
    }

    /**
     * The searcher shape the fragment executor uses, reduced to what
     * the Weights read from it: a plain {@link IndexSearcher} that
     * carries the request's {@link LanceHitsAccounting}. Closing it
     * closes the accounting the way the executor's search context does.
     */
    static final class AccountingSearcher extends IndexSearcher implements LanceHitsAccounting.Provider, AutoCloseable {
        final LanceHitsAccounting accounting;

        AccountingSearcher(IndexReader reader, CircuitBreaker breaker) {
            super(reader);
            this.accounting = new LanceHitsAccounting(breaker);
        }

        @Override
        public LanceHitsAccounting hitsAccounting() {
            return accounting;
        }

        @Override
        public void close() {
            accounting.close();
        }
    }

    private static Map<Integer, Integer> hitsByFragment(Dataset dataset, List<Integer> executorFragmentIds) throws Exception {
        ScanOptions options = LanceFtsQuery.restrictToFragmentsUnlessAll(new ScanOptions.Builder(), executorFragmentIds, dataset)
            .fullTextQuery(FullTextQuery.match("hello", "body"))
            .withRowAddress(true)
            .limit(100L)
            .build();
        Map<Integer, Integer> counts = new HashMap<>();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                for (int i = 0; i < root.getRowCount(); i++) {
                    int fragmentId = (int) (rowAddr.get(i) >>> 32);
                    counts.merge(fragmentId, 1, Integer::sum);
                }
            }
        }
        return counts;
    }

    private static List<Integer> fragmentIdsOf(Dataset dataset) {
        List<Integer> ids = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            ids.add(fragment.getId());
        }
        return ids;
    }
}

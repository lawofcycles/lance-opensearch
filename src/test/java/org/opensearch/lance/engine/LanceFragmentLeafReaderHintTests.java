/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceKnnQuery;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Doc values served for a hinted hit set: the reader takes only the
 * hinted rows of a column, answers docs outside the hint from the full
 * column, and keeps the ordinal space of keyword columns stable. The
 * fixture is {@link LanceTableFactory#writeHintFixtureTable} with three
 * fragments of 200 rows, so a hint of at most 10 rows per fragment
 * (5 percent) takes the sparse path.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFragmentLeafReaderHintTests extends OpenSearchTestCase {

    private static final int FRAGMENTS = 3;
    private static final int ROWS_PER_FRAGMENT = 200;

    private LanceDirectoryReader reader;
    private List<LanceFragmentLeafReader> leaves;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "hint-" + getTestName(), FRAGMENTS, ROWS_PER_FRAGMENT);
        Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        List<Integer> fragmentIds = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            fragmentIds.add(fragment.getId());
        }
        assertEquals(FRAGMENTS, fragmentIds.size());
        reader = LanceDirectoryReader.openForFragments(
            new ByteBuffersDirectory(),
            null,
            dataset,
            "",
            LanceEngineFactory.LancePrimaryKeyType.NONE,
            Collections.emptyMap(),
            fragmentIds
        );
        leaves = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(ctx.reader());
            assertNotNull(leaf);
            assertEquals(ROWS_PER_FRAGMENT, leaf.maxDoc());
            leaves.add(leaf);
        }
        // Fragments are written in id order and the reader lists them in
        // that order, so leaves.get(f) holds rows f * 200 .. f * 200 + 199.
        for (int f = 0; f < FRAGMENTS; f++) {
            assertEquals(f, leaves.get(f).fragmentId());
        }
    }

    @Override
    public void tearDown() throws Exception {
        if (reader != null) {
            reader.close();
        }
        super.tearDown();
    }

    // Fixture formulas, see LanceTableFactory.writeHintFixtureTable.

    private static int rowId(int fragment, int offset) {
        return fragment * ROWS_PER_FRAGMENT + offset;
    }

    private static Long rating(int i) {
        return i % 5 == 4 ? null : (long) ((i * 37) % 1000);
    }

    private static String category(int i) {
        return i % 4 == 3 ? null : "c" + (i % 3);
    }

    private static List<String> tags(int i) {
        if (i % 6 == 5) {
            return null;
        }
        return new ArrayList<>(new TreeSet<>(List.of("t" + (i % 2), "t" + (i % 5))));
    }

    private static Boolean flag(int i) {
        return i % 7 == 6 ? null : i % 2 == 0;
    }

    public void testHintedNumericAndBooleanValuesMatchTheFullColumn() throws Exception {
        LanceFragmentLeafReader leaf = leaves.get(1);
        int[] hint = { 3, 17, 44, 199 };
        leaf.hintMatchedOffsets(hint, false);

        NumericDocValues rating = leaf.getNumericDocValues("rating");
        NumericDocValues flag = leaf.getNumericDocValues("flag");
        for (int offset : hint) {
            int i = rowId(1, offset);
            Long expectedRating = rating(i);
            assertEquals("rating presence of row " + i, expectedRating != null, rating.advanceExact(offset));
            if (expectedRating != null) {
                assertEquals("rating of row " + i, expectedRating.longValue(), rating.longValue());
            }
            Boolean expectedFlag = flag(i);
            assertEquals("flag presence of row " + i, expectedFlag != null, flag.advanceExact(offset));
            if (expectedFlag != null) {
                assertEquals("flag of row " + i, expectedFlag ? 1L : 0L, flag.longValue());
            }
        }
        assertFalse("hinted rows only, no full column scan", leaf.isColumnFullyLoaded("rating"));
        assertFalse(leaf.isColumnFullyLoaded("flag"));
        assertEquals(hint.length, rating.cost());

        // A second instance for the same column reuses the taken rows.
        NumericDocValues again = leaf.getNumericDocValues("rating");
        assertTrue(again.advanceExact(3));
        assertEquals(rating(rowId(1, 3)).longValue(), again.longValue());
        assertFalse(leaf.isColumnFullyLoaded("rating"));
    }

    public void testDocOutsideHintSwitchesNumericToTheFullColumn() throws Exception {
        LanceFragmentLeafReader leaf = leaves.get(2);
        int[] hint = { 10, 20 };
        leaf.hintMatchedOffsets(hint, false);

        NumericDocValues rating = leaf.getNumericDocValues("rating");
        assertTrue(rating.advanceExact(10));
        assertEquals(rating(rowId(2, 10)).longValue(), rating.longValue());
        assertFalse(leaf.isColumnFullyLoaded("rating"));

        // Row 2 * 200 + 5 is outside the hint and has a value; row 2 * 200 + 9
        // is outside the hint and null. Both must be answered correctly, not
        // reported as missing.
        assertTrue(rating.advanceExact(5));
        assertEquals(rating(rowId(2, 5)).longValue(), rating.longValue());
        assertTrue("the miss loads the whole column", leaf.isColumnFullyLoaded("rating"));
        assertFalse(rating.advanceExact(9));
        // Hinted rows keep working from the full column.
        assertTrue(rating.advanceExact(20));
        assertEquals(rating(rowId(2, 20)).longValue(), rating.longValue());
        assertEquals(ROWS_PER_FRAGMENT, rating.cost());

        // advance() walks docs the hint cannot describe and also switches.
        LanceFragmentLeafReader other = leaves.get(0);
        other.hintMatchedOffsets(new int[] { 1, 2 }, false);
        NumericDocValues iterated = other.getNumericDocValues("rating");
        assertEquals(0, iterated.nextDoc());
        assertEquals(rating(0).longValue(), iterated.longValue());
        assertTrue(other.isColumnFullyLoaded("rating"));
    }

    public void testHintAboveSparseRatioLoadsTheFullColumnUpFront() throws Exception {
        LanceFragmentLeafReader sparseLeaf = leaves.get(1);
        int[] ten = new int[10];
        for (int i = 0; i < ten.length; i++) {
            ten[i] = i * 3;
        }
        sparseLeaf.hintMatchedOffsets(ten, true);
        NumericDocValues sparseRating = sparseLeaf.getNumericDocValues("rating");
        assertTrue(sparseRating.advanceExact(0));
        assertTrue("10 of 200 rows is exactly 5 percent and stays sparse", sparseLeaf.isServingSparse("rating"));
        assertFalse(sparseLeaf.isColumnFullyLoaded("rating"));
        // Rows 200, 203, ..., 227 are all c2 or null: one distinct term.
        assertEquals(1L, sparseLeaf.getSortedSetDocValues("category").getValueCount());
        assertTrue(sparseLeaf.isServingSparse("category"));

        LanceFragmentLeafReader denseLeaf = leaves.get(0);
        int[] eleven = new int[11];
        for (int i = 0; i < eleven.length; i++) {
            eleven[i] = i * 3;
        }
        denseLeaf.hintMatchedOffsets(eleven, true);
        NumericDocValues rating = denseLeaf.getNumericDocValues("rating");
        assertTrue(rating.advanceExact(0));
        assertFalse(denseLeaf.isServingSparse("rating"));
        assertTrue("11 of 200 rows is above 5 percent", denseLeaf.isColumnFullyLoaded("rating"));
        SortedSetDocValues category = denseLeaf.getSortedSetDocValues("category");
        assertEquals("full dictionary: c0, c1, c2", 3L, category.getValueCount());
        assertFalse(denseLeaf.isServingSparse("category"));
        assertTrue(denseLeaf.isColumnFullyLoaded("category"));

        // The dense leaf loaded through the shard cache, which publishes
        // the full column to every leaf of the reader. The sparse leaf
        // keeps serving its taken rows and its own dictionary anyway.
        assertTrue(sparseLeaf.isColumnFullyLoaded("rating"));
        assertTrue(sparseLeaf.isServingSparse("rating"));
        assertTrue(sparseLeaf.isServingSparse("category"));
        assertEquals("the ordinal space of the sparse leaf is unchanged", 1L, sparseLeaf.getSortedSetDocValues("category").getValueCount());
    }

    public void testExclusiveHintServesKeywordOrdinalsFromTheHintedRows() throws Exception {
        LanceFragmentLeafReader leaf = leaves.get(1);
        // Rows 201 (c0), 202 (c1), 203 (null), 208 (c1), 210 (c0), 212 (c2)
        int[] hint = { 1, 2, 3, 8, 10, 12 };
        leaf.hintMatchedOffsets(hint, true);

        SortedSetDocValues category = leaf.getSortedSetDocValues("category");
        assertEquals("distinct categories among the hinted rows", 3L, category.getValueCount());
        for (int offset : hint) {
            int i = rowId(1, offset);
            String expected = category(i);
            assertEquals("presence of row " + i, expected != null, category.advanceExact(offset));
            if (expected != null) {
                assertEquals(1, category.docValueCount());
                assertEquals("category of row " + i, expected, category.lookupOrd(category.nextOrd()).utf8ToString());
            }
        }
        assertFalse("the dictionary came from the take, not a column scan", leaf.isColumnFullyLoaded("category"));

        // Ordinals compare like the terms: c0 < c1 < c2.
        SortedDocValues single = leaf.getSortedDocValues("category");
        assertTrue(single.advanceExact(1));
        int c0 = single.ordValue();
        assertTrue(single.advanceExact(2));
        int c1 = single.ordValue();
        assertTrue(single.advanceExact(12));
        int c2 = single.ordValue();
        assertTrue(c0 < c1 && c1 < c2);
        assertEquals(c1, single.lookupTerm(new BytesRef("c1")));

        // Multi-valued keyword: each row's ordinals are ascending and
        // map back to the sorted distinct tags of the row.
        SortedSetDocValues tags = leaf.getSortedSetDocValues("tags");
        for (int offset : hint) {
            int i = rowId(1, offset);
            List<String> expected = tags(i);
            assertEquals("tags presence of row " + i, expected != null, tags.advanceExact(offset));
            if (expected != null) {
                assertEquals("tag count of row " + i, expected.size(), tags.docValueCount());
                long previous = -1;
                List<String> actual = new ArrayList<>();
                for (int n = 0; n < expected.size(); n++) {
                    long ord = tags.nextOrd();
                    assertTrue("ordinals ascend", ord > previous);
                    previous = ord;
                    actual.add(tags.lookupOrd(ord).utf8ToString());
                }
                assertEquals("tags of row " + i, expected, actual);
            }
        }
        assertFalse(leaf.isColumnFullyLoaded("tags"));

        // The same hint delivered again without the exclusive flag does
        // not downgrade the leaf; a fresh instance stays on the sparse
        // dictionary so its ordinal space matches the one above.
        leaf.hintMatchedOffsets(hint.clone(), false);
        assertTrue(leaf.hintExclusive());
        assertEquals(3L, leaf.getSortedSetDocValues("category").getValueCount());
        assertFalse(leaf.isColumnFullyLoaded("category"));
    }

    public void testNonExclusiveHintLoadsTheFullKeywordDictionary() throws Exception {
        LanceFragmentLeafReader leaf = leaves.get(2);
        leaf.hintMatchedOffsets(new int[] { 1, 2 }, false);
        SortedSetDocValues category = leaf.getSortedSetDocValues("category");
        assertEquals(3L, category.getValueCount());
        assertTrue("without exclusivity the ordinal space must be the full one", leaf.isColumnFullyLoaded("category"));
        assertTrue(category.advanceExact(5));
        assertEquals(category(rowId(2, 5)), category.lookupOrd(category.nextOrd()).utf8ToString());
    }

    public void testKeywordInstanceOutsideHintAnswersFromTheFullColumnWithinItsDictionary() throws Exception {
        LanceFragmentLeafReader leaf = leaves.get(1);
        // Rows 201, 204, 213 all carry c0, so the sparse dictionary is {c0}.
        int[] hint = { 1, 4, 13 };
        leaf.hintMatchedOffsets(hint, true);
        SortedSetDocValues category = leaf.getSortedSetDocValues("category");
        assertEquals(1L, category.getValueCount());
        assertFalse(leaf.isColumnFullyLoaded("category"));

        // Row 210 is outside the hint but its value c0 exists in the
        // dictionary: the instance loads the column and answers with the
        // sparse ordinal.
        assertTrue(category.advanceExact(10));
        assertEquals(0L, category.nextOrd());
        assertTrue(leaf.isColumnFullyLoaded("category"));
        // Row 207 is outside the hint and null.
        assertFalse(category.advanceExact(7));
        // Row 202 carries c1, which has no ordinal in the space this
        // instance has already exposed; reporting it as missing or as some
        // other term would be wrong, so the instance refuses.
        IllegalStateException failure = expectThrows(IllegalStateException.class, () -> category.advanceExact(2));
        assertTrue(failure.getMessage(), failure.getMessage().contains("category"));

        // Later instances of the column under the same hint keep the
        // sparse space (the decision sticks per column), even though the
        // full dictionary is now loaded.
        assertEquals(1L, leaf.getSortedSetDocValues("category").getValueCount());
    }

    public void testReplacingTheHintRebuildsTheSparseStructures() throws Exception {
        LanceFragmentLeafReader leaf = leaves.get(0);
        leaf.hintMatchedOffsets(new int[] { 0, 1, 2 }, true);
        assertEquals("c0, c1, c2", 3L, leaf.getSortedSetDocValues("category").getValueCount());
        NumericDocValues rating = leaf.getNumericDocValues("rating");
        assertTrue(rating.advanceExact(1));
        assertEquals(rating(1).longValue(), rating.longValue());

        // Rows 4 (rating null, c1) and 6 (c0): the new hint replaces the
        // old one and the structures are rebuilt for it. The exclusive
        // flag belongs to the new hint.
        leaf.hintMatchedOffsets(new int[] { 4, 6 }, false);
        assertFalse(leaf.hintExclusive());
        assertArrayEquals(new int[] { 4, 6 }, leaf.hintedOffsets());
        NumericDocValues replaced = leaf.getNumericDocValues("rating");
        assertFalse(replaced.advanceExact(4));
        assertTrue(replaced.advanceExact(6));
        assertEquals(rating(6).longValue(), replaced.longValue());
        assertFalse(leaf.isColumnFullyLoaded("rating"));

        // Same rows reported again, this time exclusively: the flag is
        // upgraded and the keyword dictionary covers the two rows only.
        leaf.hintMatchedOffsets(new int[] { 4, 6 }, true);
        assertTrue(leaf.hintExclusive());
        assertEquals("c0 and c1", 2L, leaf.getSortedSetDocValues("category").getValueCount());
        assertFalse(leaf.isColumnFullyLoaded("category"));
    }

    public void testFtsWeightHintsTheLeafAndSortReadsOnlyTheHits() throws Exception {
        IndexSearcher searcher = new IndexSearcher(reader);
        Sort byRatingDesc = new Sort(new SortField("rating", SortField.Type.LONG, true));

        // tok250 matches row 250 only: fragment 1, offset 50.
        TopFieldDocs single = searcher.search(new LanceFtsQuery("body", "tok250"), 10, byRatingDesc);
        assertEquals(1, single.totalHits.value());
        assertEquals(rating(250).longValue(), ((Long) ((FieldDoc) single.scoreDocs[0]).fields[0]).longValue());
        assertArrayEquals(new int[] { 50 }, leaves.get(1).hintedOffsets());
        assertTrue("the searcher drove collection from the FTS scorer alone", leaves.get(1).hintExclusive());
        assertNull("no hits, no hint", leaves.get(0).hintedOffsets());
        assertNull(leaves.get(2).hintedOffsets());
        for (LanceFragmentLeafReader leaf : leaves) {
            assertFalse("sort read the hit row only", leaf.isColumnFullyLoaded("rating"));
        }

        // grp7 matches 8 rows per fragment (4 percent): still sparse.
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < FRAGMENTS * ROWS_PER_FRAGMENT; i++) {
            if (i % 25 == 7) {
                expected.add(i);
            }
        }
        // Lucene treats a missing long as 0, so null ratings sort last in
        // descending order; ties break on doc id.
        expected.sort(Comparator.comparingLong((Integer i) -> rating(i) == null ? 0L : rating(i)).reversed().thenComparing(i -> i));
        TopFieldDocs group = searcher.search(new LanceFtsQuery("body", "grp7"), 30, byRatingDesc);
        assertEquals(expected.size(), group.totalHits.value());
        assertEquals(expected, globalIds(group.scoreDocs));
        for (LanceFragmentLeafReader leaf : leaves) {
            assertEquals(8, leaf.hintedOffsets().length);
            assertTrue(leaf.hintExclusive());
            assertFalse(leaf.isColumnFullyLoaded("rating"));
        }
    }

    public void testFtsWeightKeywordSortUsesTheSparseDictionaryOnEveryLeaf() throws Exception {
        IndexSearcher searcher = new IndexSearcher(reader);
        Sort byCategory = new Sort(new SortField("category", SortField.Type.STRING));
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < FRAGMENTS * ROWS_PER_FRAGMENT; i++) {
            if (i % 25 == 7) {
                expected.add(i);
            }
        }
        // Missing terms sort first for SortField.Type.STRING; ties break on doc id.
        expected.sort(Comparator.comparing((Integer i) -> category(i) == null ? "" : category(i)).thenComparing(i -> i));
        TopFieldDocs docs = searcher.search(new LanceFtsQuery("body", "grp7"), 30, byCategory);
        assertEquals(expected, globalIds(docs.scoreDocs));
        for (LanceFragmentLeafReader leaf : leaves) {
            assertTrue(leaf.hintExclusive());
            assertFalse("keyword sort built its dictionary from the 8 hits", leaf.isColumnFullyLoaded("category"));
        }
    }

    public void testKeywordSortLoadsTheFullDictionaryOnLeavesVisitedWithAFullQueue() throws Exception {
        // With a page smaller than the hits of the first leaf, Lucene's
        // TermOrdValComparator looks the current bottom term up in every
        // later leaf's dictionary while the leaf collector is created,
        // which is before the FTS Weight has shown that it drives
        // collection there. That leaf has to answer from the full
        // dictionary; the first leaf, whose comparator asks nothing
        // before scoring starts, stays sparse. Results are unaffected.
        IndexSearcher searcher = new IndexSearcher(reader);
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < FRAGMENTS * ROWS_PER_FRAGMENT; i++) {
            if (i % 25 == 7) {
                expected.add(i);
            }
        }
        expected.sort(Comparator.comparing((Integer i) -> category(i) == null ? "" : category(i)).thenComparing(i -> i));
        TopFieldDocs docs = searcher.search(
            new LanceFtsQuery("body", "grp7"),
            5,
            new Sort(new SortField("category", SortField.Type.STRING))
        );
        assertEquals(expected.subList(0, 5), globalIds(docs.scoreDocs));
        assertTrue(leaves.get(0).isServingSparse("category"));
        assertFalse(leaves.get(1).isServingSparse("category"));
        assertFalse(leaves.get(2).isServingSparse("category"));
        assertTrue(leaves.get(1).isColumnFullyLoaded("category"));
    }

    public void testKnnWeightHintsTheLeaf() throws Exception {
        IndexSearcher searcher = new IndexSearcher(reader);
        float[] vector = new float[8];
        vector[0] = 250.4f;
        // Nearest to 250.4: rows 250, 251, 249, all in fragment 1.
        TopFieldDocs docs = searcher.search(
            new LanceKnnQuery("embedding", vector, 3),
            10,
            new Sort(new SortField("rating", SortField.Type.LONG, true))
        );
        assertEquals(3, docs.totalHits.value());
        List<Integer> expected = new ArrayList<>(List.of(249, 250, 251));
        expected.sort(Comparator.comparingLong((Integer i) -> rating(i) == null ? 0L : rating(i)).reversed());
        assertEquals(expected, globalIds(docs.scoreDocs));
        assertArrayEquals(new int[] { 49, 50, 51 }, leaves.get(1).hintedOffsets());
        assertTrue(leaves.get(1).hintExclusive());
        assertNull(leaves.get(0).hintedOffsets());
        for (LanceFragmentLeafReader leaf : leaves) {
            assertFalse(leaf.isColumnFullyLoaded("rating"));
        }
    }

    public void testDisjunctionOfTwoLanceClausesStaysCorrect() throws Exception {
        // grp7 (8 rows per fragment) OR tok251 (row 251 only). On fragment
        // 1 both clauses have hits, so Lucene builds a disjunction and
        // obtains plain scorers: the leaf is hinted with tok251's row
        // (the last Weight to report) but not exclusively, so the keyword
        // sort loads the full dictionary and the numeric sort falls back
        // to the full column when a grp7 row is requested. On fragments
        // 0 and 2 only grp7 has hits, so the boolean forwards bulk
        // scoring to that clause and the leaves stay sparse.
        IndexSearcher searcher = new IndexSearcher(reader);
        Query union = new BooleanQuery.Builder().add(new LanceFtsQuery("body", "grp7"), BooleanClause.Occur.SHOULD)
            .add(new LanceFtsQuery("body", "tok251"), BooleanClause.Occur.SHOULD)
            .build();
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < FRAGMENTS * ROWS_PER_FRAGMENT; i++) {
            if (i % 25 == 7 || i == 251) {
                expected.add(i);
            }
        }
        List<Integer> byCategory = new ArrayList<>(expected);
        byCategory.sort(Comparator.comparing((Integer i) -> category(i) == null ? "" : category(i)).thenComparing(i -> i));
        TopFieldDocs categoryDocs = searcher.search(union, 40, new Sort(new SortField("category", SortField.Type.STRING)));
        assertEquals(byCategory, globalIds(categoryDocs.scoreDocs));
        assertFalse(leaves.get(1).hintExclusive());
        assertFalse(leaves.get(1).isServingSparse("category"));
        assertTrue("disjunction on fragment 1 reads the full dictionary", leaves.get(1).isColumnFullyLoaded("category"));
        assertTrue(leaves.get(0).hintExclusive());
        assertTrue("single clause on fragment 0 stays sparse", leaves.get(0).isServingSparse("category"));

        List<Integer> byRating = new ArrayList<>(expected);
        byRating.sort(Comparator.comparingLong((Integer i) -> rating(i) == null ? 0L : rating(i)).reversed().thenComparing(i -> i));
        TopFieldDocs ratingDocs = searcher.search(union, 40, new Sort(new SortField("rating", SortField.Type.LONG, true)));
        assertEquals(byRating, globalIds(ratingDocs.scoreDocs));
        assertTrue(
            "a grp7 row outside the tok251 hint switched fragment 1 to the full column",
            leaves.get(1).isColumnFullyLoaded("rating")
        );
        assertFalse(leaves.get(1).isServingSparse("rating"));
        assertTrue("the fallback scanned fragment 1 alone", leaves.get(2).isServingSparse("rating"));
        assertFalse(leaves.get(2).isColumnFullyLoaded("rating"));
    }

    public void testForeignReaderWrapperKeepsKeywordDictionariesOnTheFullColumn() throws Exception {
        // A reader wrapper the plugin does not know (the shape of the
        // security plugin's DLS / FLS reader) may hide rows the Lance
        // scan returned. The Weight still hints the leaf, so numeric
        // values are read for the visited docs only, but it never marks
        // the hint exclusive, so no keyword dictionary is built from the
        // hit rows and getValueCount() reflects the whole fragment.
        try (DirectoryReader wrapped = new ForeignWrapper(reader)) {
            IndexSearcher searcher = new IndexSearcher(wrapped);
            for (LeafReaderContext ctx : wrapped.leaves()) {
                assertFalse(LanceFragmentLeafReader.wrappedOnlyByOwnReaders(ctx.reader()));
                assertNotNull(LanceFragmentLeafReader.unwrap(ctx.reader()));
            }
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < FRAGMENTS * ROWS_PER_FRAGMENT; i++) {
                if (i % 25 == 7) {
                    expected.add(i);
                }
            }
            expected.sort(Comparator.comparing((Integer i) -> category(i) == null ? "" : category(i)).thenComparing(i -> i));
            TopFieldDocs docs = searcher.search(
                new LanceFtsQuery("body", "grp7"),
                30,
                new Sort(new SortField("category", SortField.Type.STRING))
            );
            assertEquals(expected, globalIds(docs.scoreDocs));
            for (LanceFragmentLeafReader leaf : leaves) {
                assertEquals("the hint is still delivered", 8, leaf.hintedOffsets().length);
                assertFalse("but never marked exclusive under a foreign wrapper", leaf.hintExclusive());
                assertFalse(leaf.isServingSparse("category"));
                assertTrue(leaf.isColumnFullyLoaded("category"));
                assertEquals("full dictionary of the fragment", 3L, leaf.getSortedSetDocValues("category").getValueCount());
            }

            List<Integer> byRating = new ArrayList<>(expected);
            byRating.sort(Comparator.comparingLong((Integer i) -> rating(i) == null ? 0L : rating(i)).reversed().thenComparing(i -> i));
            TopFieldDocs ratingDocs = searcher.search(
                new LanceFtsQuery("body", "grp7"),
                30,
                new Sort(new SortField("rating", SortField.Type.LONG, true))
            );
            assertEquals(byRating, globalIds(ratingDocs.scoreDocs));
            for (LanceFragmentLeafReader leaf : leaves) {
                assertTrue("numeric values are read per visited doc and stay sparse", leaf.isServingSparse("rating"));
                assertFalse(leaf.isColumnFullyLoaded("rating"));
            }
        }
    }

    public void testOpenSearchReaderWrapperKeepsTheExclusiveHint() throws Exception {
        // OpenSearchDirectoryReader.wrap is the wrapper every fragment
        // path reader carries; it hides nothing, so the chain stays
        // trusted and keyword dictionaries come from the hit rows.
        try (OpenSearchDirectoryReader wrapped = OpenSearchDirectoryReader.wrap(reader, new ShardId("hint", "uuid", 0))) {
            // Closing the wrapper closes the reader underneath; tearDown
            // must not close it a second time.
            reader = null;
            for (LeafReaderContext ctx : wrapped.leaves()) {
                assertTrue(LanceFragmentLeafReader.wrappedOnlyByOwnReaders(ctx.reader()));
            }
            IndexSearcher searcher = new IndexSearcher(wrapped);
            TopFieldDocs docs = searcher.search(
                new LanceFtsQuery("body", "grp7"),
                30,
                new Sort(new SortField("category", SortField.Type.STRING))
            );
            assertEquals(24, docs.totalHits.value());
            for (LanceFragmentLeafReader leaf : leaves) {
                assertTrue(leaf.hintExclusive());
                assertTrue(leaf.isServingSparse("category"));
            }
        }
    }

    public void testScorerSupplierHandsItsScorerOutOnce() throws Exception {
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = searcher.createWeight(new LanceFtsQuery("body", "grp7"), ScoreMode.COMPLETE_NO_SCORES, 1f);
        LeafReaderContext ctx = reader.leaves().get(0);
        ScorerSupplier viaGet = weight.scorerSupplier(ctx);
        assertNotNull(viaGet.get(Long.MAX_VALUE));
        IllegalStateException twice = expectThrows(IllegalStateException.class, () -> viaGet.get(Long.MAX_VALUE));
        assertTrue(twice.getMessage(), twice.getMessage().contains("already handed out"));

        ScorerSupplier viaBulk = weight.scorerSupplier(ctx);
        assertNotNull(viaBulk.bulkScorer());
        expectThrows(IllegalStateException.class, () -> viaBulk.get(Long.MAX_VALUE));
        expectThrows(IllegalStateException.class, viaBulk::bulkScorer);
    }

    /**
     * Stand-in for a reader wrapper installed by another plugin: a
     * {@link FilterDirectoryReader} whose leaves are plain
     * {@link FilterLeafReader}s, which is what the security plugin's
     * DLS / FLS reader is from the fragment reader's point of view.
     */
    private static final class ForeignWrapper extends FilterDirectoryReader {
        ForeignWrapper(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader leaf) {
                    return new FilterLeafReader(leaf) {
                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return leaf.getCoreCacheHelper();
                        }

                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return leaf.getReaderCacheHelper();
                        }
                    };
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new ForeignWrapper(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        @Override
        protected void doClose() {
            // The test fixture owns the wrapped reader and closes it in
            // tearDown; FilterDirectoryReader.doClose would close it here.
        }
    }

    private List<Integer> globalIds(ScoreDoc[] scoreDocs) throws IOException {
        List<Integer> ids = new ArrayList<>(scoreDocs.length);
        List<LeafReaderContext> contexts = reader.leaves();
        for (ScoreDoc scoreDoc : scoreDocs) {
            int leafIndex = ReaderUtil.subIndex(scoreDoc.doc, contexts);
            LeafReaderContext ctx = contexts.get(leafIndex);
            ids.add(rowId(leaves.get(leafIndex).fragmentId(), scoreDoc.doc - ctx.docBase));
        }
        return ids;
    }
}

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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldCollectorManager;
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
import org.opensearch.lance.query.LanceHintingWeight;
import org.opensearch.lance.query.LanceKnnQuery;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Doc values served for a hinted hit set: the reader takes only the
 * hinted rows of a column, answers docs outside the hint from the full
 * column, and keeps the ordinal space of keyword columns stable. The
 * fixture is {@link LanceTableFactory#writeHintFixtureTable} with three
 * fragments of 10,000 rows, so a hint of at most 25 rows per fragment
 * (0.25 percent, {@link LanceFragmentLeafReader#SPARSE_RATIO}) takes the
 * sparse path. The full-text token {@code sp7} matches one row in 625
 * (16 per fragment, 0.16 percent) and drives the sparse path from a
 * Lance scorer; {@code grp7} matches one row in 25 (4 percent) and
 * loads the full column.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFragmentLeafReaderHintTests extends OpenSearchTestCase {

    private static final int FRAGMENTS = 3;
    private static final int ROWS_PER_FRAGMENT = 10_000;
    /** Largest hint that stays sparse on a fragment of {@link #ROWS_PER_FRAGMENT} rows. */
    private static final int SPARSE_ROWS = (int) (ROWS_PER_FRAGMENT * LanceFragmentLeafReader.SPARSE_RATIO);
    /** Rows of one fragment the token {@code sp7} matches ({@code i % 625 == 7}). */
    private static final int SP_HITS_PER_FRAGMENT = ROWS_PER_FRAGMENT / 625;

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
        // that order, so leaves.get(f) holds rows f * 10,000 .. f * 10,000 + 9,999.
        for (int f = 0; f < FRAGMENTS; f++) {
            assertEquals(f, leaves.get(f).fragmentId());
        }
        assertEquals(25, SPARSE_ROWS);
        assertEquals(16, SP_HITS_PER_FRAGMENT);
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

    /** Global ids of the rows the token {@code sp7} matches, in id order. */
    private static List<Integer> sp7Rows() {
        List<Integer> rows = new ArrayList<>();
        for (int i = 0; i < FRAGMENTS * ROWS_PER_FRAGMENT; i++) {
            if (i % 625 == 7) {
                rows.add(i);
            }
        }
        return rows;
    }

    /** Rating descending, Lucene's missing value 0 for null, ties on id. */
    private static final Comparator<Integer> BY_RATING_DESC = Comparator.comparingLong((Integer i) -> rating(i) == null ? 0L : rating(i))
        .reversed()
        .thenComparing(i -> i);

    /** Category ascending, missing terms first as for SortField.Type.STRING, ties on id. */
    private static final Comparator<Integer> BY_CATEGORY = Comparator.comparing((Integer i) -> category(i) == null ? "" : category(i))
        .thenComparing(i -> i);

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

        // Row 2 * 10,000 + 5 is outside the hint and has a value; row
        // 2 * 10,000 + 9 is outside the hint and null. Both must be
        // answered correctly, not reported as missing.
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
        int[] atRatio = new int[SPARSE_ROWS];
        for (int i = 0; i < atRatio.length; i++) {
            atRatio[i] = i * 3;
        }
        sparseLeaf.hintMatchedOffsets(atRatio, true);
        NumericDocValues sparseRating = sparseLeaf.getNumericDocValues("rating");
        assertTrue(sparseRating.advanceExact(0));
        assertTrue("25 of 10,000 rows is exactly 0.25 percent and stays sparse", sparseLeaf.isServingSparse("rating"));
        assertFalse(sparseLeaf.isColumnFullyLoaded("rating"));
        // Rows 10,000, 10,003, ..., 10,072 share i % 3 == 1: c1 or null,
        // one distinct term.
        assertEquals(1L, sparseLeaf.getSortedSetDocValues("category").getValueCount());
        assertTrue(sparseLeaf.isServingSparse("category"));

        LanceFragmentLeafReader denseLeaf = leaves.get(0);
        int[] aboveRatio = new int[SPARSE_ROWS + 1];
        for (int i = 0; i < aboveRatio.length; i++) {
            aboveRatio[i] = i * 3;
        }
        denseLeaf.hintMatchedOffsets(aboveRatio, true);
        NumericDocValues rating = denseLeaf.getNumericDocValues("rating");
        assertTrue(rating.advanceExact(0));
        assertFalse(denseLeaf.isServingSparse("rating"));
        assertTrue("26 of 10,000 rows is above 0.25 percent", denseLeaf.isColumnFullyLoaded("rating"));
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
        // Rows 10,001 (c2), 10,002 (c0), 10,003 (null), 10,008 (c0),
        // 10,010 (c2), 10,012 (c1)
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
        assertEquals("c0", category(rowId(1, 2)));
        assertTrue(single.advanceExact(2));
        int c0 = single.ordValue();
        assertEquals("c1", category(rowId(1, 12)));
        assertTrue(single.advanceExact(12));
        int c1 = single.ordValue();
        assertEquals("c2", category(rowId(1, 1)));
        assertTrue(single.advanceExact(1));
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
        // Rows 10,002, 10,005, 10,008 all carry c0, so the sparse dictionary is {c0}.
        int[] hint = { 2, 5, 8 };
        for (int offset : hint) {
            assertEquals("c0", category(rowId(1, offset)));
        }
        leaf.hintMatchedOffsets(hint, true);
        SortedSetDocValues category = leaf.getSortedSetDocValues("category");
        assertEquals(1L, category.getValueCount());
        assertFalse(leaf.isColumnFullyLoaded("category"));

        // Row 10,014 is outside the hint but its value c0 exists in the
        // dictionary: the instance loads the column and answers with the
        // sparse ordinal.
        assertEquals("c0", category(rowId(1, 14)));
        assertTrue(category.advanceExact(14));
        assertEquals(0L, category.nextOrd());
        assertTrue(leaf.isColumnFullyLoaded("category"));
        // Row 10,007 is outside the hint and null.
        assertNull(category(rowId(1, 7)));
        assertFalse(category.advanceExact(7));
        // Row 10,001 carries c2, which has no ordinal in the space this
        // instance has already exposed; reporting it as missing or as some
        // other term would be wrong, so the instance refuses.
        assertEquals("c2", category(rowId(1, 1)));
        IllegalStateException failure = expectThrows(IllegalStateException.class, () -> category.advanceExact(1));
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

        // tok10050 matches row 10,050 only: fragment 1, offset 50.
        int single = rowId(1, 50);
        TopFieldDocs singleDocs = searcher.search(new LanceFtsQuery("body", "tok" + single), 10, byRatingDesc);
        assertEquals(1, singleDocs.totalHits.value());
        assertEquals(rating(single).longValue(), ((Long) ((FieldDoc) singleDocs.scoreDocs[0]).fields[0]).longValue());
        assertArrayEquals(new int[] { 50 }, leaves.get(1).hintedOffsets());
        assertTrue("the searcher drove collection from the FTS scorer alone", leaves.get(1).hintExclusive());
        // Leaves without hits are told so, exclusively: the searcher asked
        // their (empty) supplier for a BulkScorer as well.
        assertEquals(0, leaves.get(0).hintedOffsets().length);
        assertTrue(leaves.get(0).hintExclusive());
        assertEquals(0, leaves.get(2).hintedOffsets().length);
        for (LanceFragmentLeafReader leaf : leaves) {
            assertFalse("sort read the hit row only", leaf.isColumnFullyLoaded("rating"));
        }
        // A keyword dictionary requested on a hit-less leaf under its
        // empty exclusive hint is empty and costs no Lance scan; this is
        // what a global ordinal map built after the hits phase sees.
        assertEquals(0L, leaves.get(0).getSortedSetDocValues("category").getValueCount());
        assertTrue(leaves.get(0).isServingSparse("category"));
        assertFalse(leaves.get(0).isColumnFullyLoaded("category"));

        // sp7 matches 16 rows per fragment (0.16 percent): still sparse.
        List<Integer> expected = sp7Rows();
        // Lucene treats a missing long as 0, so null ratings sort last in
        // descending order; ties break on doc id.
        expected.sort(BY_RATING_DESC);
        TopFieldDocs group = searcher.search(new LanceFtsQuery("body", "sp7"), 100, byRatingDesc);
        assertEquals(expected.size(), group.totalHits.value());
        assertEquals(expected, globalIds(group.scoreDocs));
        for (LanceFragmentLeafReader leaf : leaves) {
            assertEquals(SP_HITS_PER_FRAGMENT, leaf.hintedOffsets().length);
            assertTrue(leaf.hintExclusive());
            assertFalse(leaf.isColumnFullyLoaded("rating"));
        }

        // grp7 matches 400 rows per fragment (4 percent): above the
        // ratio, so the sort loads the column instead of taking 400 rows.
        List<Integer> dense = new ArrayList<>();
        for (int i = 0; i < FRAGMENTS * ROWS_PER_FRAGMENT; i++) {
            if (i % 25 == 7) {
                dense.add(i);
            }
        }
        dense.sort(BY_RATING_DESC);
        TopFieldDocs denseDocs = searcher.search(new LanceFtsQuery("body", "grp7"), 50, byRatingDesc);
        assertEquals(dense.size(), denseDocs.totalHits.value());
        assertEquals(dense.subList(0, 50), globalIds(denseDocs.scoreDocs));
        for (LanceFragmentLeafReader leaf : leaves) {
            assertEquals(400, leaf.hintedOffsets().length);
            assertFalse(leaf.isServingSparse("rating"));
            assertTrue(leaf.isColumnFullyLoaded("rating"));
        }
    }

    public void testFtsWeightKeywordSortUsesTheSparseDictionaryOnEveryLeaf() throws Exception {
        IndexSearcher searcher = new IndexSearcher(reader);
        Sort byCategory = new Sort(new SortField("category", SortField.Type.STRING));
        List<Integer> expected = sp7Rows();
        // Missing terms sort first for SortField.Type.STRING; ties break on doc id.
        expected.sort(BY_CATEGORY);
        TopFieldDocs docs = searcher.search(new LanceFtsQuery("body", "sp7"), 100, byCategory);
        assertEquals(expected, globalIds(docs.scoreDocs));
        for (LanceFragmentLeafReader leaf : leaves) {
            assertTrue(leaf.hintExclusive());
            assertFalse("keyword sort built its dictionary from the 16 hits", leaf.isColumnFullyLoaded("category"));
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
        List<Integer> expected = sp7Rows();
        expected.sort(BY_CATEGORY);
        TopFieldDocs docs = searcher.search(
            new LanceFtsQuery("body", "sp7"),
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
        vector[0] = ROWS_PER_FRAGMENT + 50.4f;
        // Nearest to 10,050.4: rows 10,050, 10,051, 10,049, all in fragment 1.
        TopFieldDocs docs = searcher.search(
            new LanceKnnQuery("embedding", vector, 3),
            10,
            new Sort(new SortField("rating", SortField.Type.LONG, true))
        );
        assertEquals(3, docs.totalHits.value());
        List<Integer> expected = new ArrayList<>(List.of(rowId(1, 49), rowId(1, 50), rowId(1, 51)));
        expected.sort(BY_RATING_DESC);
        assertEquals(expected, globalIds(docs.scoreDocs));
        assertArrayEquals(new int[] { 49, 50, 51 }, leaves.get(1).hintedOffsets());
        assertTrue(leaves.get(1).hintExclusive());
        assertEquals(0, leaves.get(0).hintedOffsets().length);
        assertTrue(leaves.get(0).hintExclusive());
        for (LanceFragmentLeafReader leaf : leaves) {
            assertFalse(leaf.isColumnFullyLoaded("rating"));
        }
    }

    public void testDisjunctionOfTwoLanceClausesStaysCorrect() throws Exception {
        // sp7 (16 rows per fragment) OR tok10051 (row 10,051 only).
        // Lucene builds a disjunction on every fragment (both clauses
        // hand out a supplier, tok10051's over an empty set outside
        // fragment 1) and obtains plain scorers, so no leaf is hinted
        // exclusively and the keyword sort loads the full dictionary. On
        // fragment 1 the leaf is hinted with tok10051's row (the last
        // Weight to report) and the numeric sort falls back to the full
        // column when an sp7 row is requested; on fragments 0 and 2 the
        // empty tok10051 hint does not displace sp7's, so the numeric
        // sort stays sparse there.
        IndexSearcher searcher = new IndexSearcher(reader);
        int single = rowId(1, 51);
        Query union = new BooleanQuery.Builder().add(new LanceFtsQuery("body", "sp7"), BooleanClause.Occur.SHOULD)
            .add(new LanceFtsQuery("body", "tok" + single), BooleanClause.Occur.SHOULD)
            .build();
        List<Integer> expected = sp7Rows();
        expected.add(single);
        List<Integer> byCategory = new ArrayList<>(expected);
        byCategory.sort(BY_CATEGORY);
        TopFieldDocs categoryDocs = searcher.search(union, 100, new Sort(new SortField("category", SortField.Type.STRING)));
        assertEquals(byCategory, globalIds(categoryDocs.scoreDocs));
        assertFalse(leaves.get(1).hintExclusive());
        assertFalse(leaves.get(1).isServingSparse("category"));
        assertTrue("disjunction on fragment 1 reads the full dictionary", leaves.get(1).isColumnFullyLoaded("category"));
        assertFalse(leaves.get(0).hintExclusive());
        assertEquals("sp7's rows stay hinted on fragment 0", SP_HITS_PER_FRAGMENT, leaves.get(0).hintedOffsets().length);
        assertFalse(leaves.get(0).isServingSparse("category"));

        List<Integer> byRating = new ArrayList<>(expected);
        byRating.sort(BY_RATING_DESC);
        TopFieldDocs ratingDocs = searcher.search(union, 100, new Sort(new SortField("rating", SortField.Type.LONG, true)));
        assertEquals(byRating, globalIds(ratingDocs.scoreDocs));
        assertTrue(
            "an sp7 row outside the tok10051 hint switched fragment 1 to the full column",
            leaves.get(1).isColumnFullyLoaded("rating")
        );
        assertFalse(leaves.get(1).isServingSparse("rating"));
        assertTrue("the fallback scanned fragment 1 alone", leaves.get(2).isServingSparse("rating"));
        assertFalse(leaves.get(2).isColumnFullyLoaded("rating"));
        assertTrue(leaves.get(0).isServingSparse("rating"));
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
            List<Integer> expected = sp7Rows();
            expected.sort(BY_CATEGORY);
            TopFieldDocs docs = searcher.search(
                new LanceFtsQuery("body", "sp7"),
                100,
                new Sort(new SortField("category", SortField.Type.STRING))
            );
            assertEquals(expected, globalIds(docs.scoreDocs));
            for (LanceFragmentLeafReader leaf : leaves) {
                assertEquals("the hint is still delivered", SP_HITS_PER_FRAGMENT, leaf.hintedOffsets().length);
                assertFalse("but never marked exclusive under a foreign wrapper", leaf.hintExclusive());
                assertFalse(leaf.isServingSparse("category"));
                assertTrue(leaf.isColumnFullyLoaded("category"));
                assertEquals("full dictionary of the fragment", 3L, leaf.getSortedSetDocValues("category").getValueCount());
            }

            List<Integer> byRating = new ArrayList<>(expected);
            byRating.sort(BY_RATING_DESC);
            TopFieldDocs ratingDocs = searcher.search(
                new LanceFtsQuery("body", "sp7"),
                100,
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
                new LanceFtsQuery("body", "sp7"),
                100,
                new Sort(new SortField("category", SortField.Type.STRING))
            );
            assertEquals(FRAGMENTS * SP_HITS_PER_FRAGMENT, docs.totalHits.value());
            for (LanceFragmentLeafReader leaf : leaves) {
                assertTrue(leaf.hintExclusive());
                assertTrue(leaf.isServingSparse("category"));
            }
        }
    }

    public void testEarlyExclusiveHintServesKeywordOrdinalsBeforeCollection() throws Exception {
        // The fragment executor creates the Weight of a bare Lance
        // clause first and hints every leaf exclusively through it,
        // then builds aggregators and comparators. A keyword terms
        // aggregation reads every leaf's value count while it is
        // created (TermsAggregatorFactory.getMaxOrd) and a keyword sort
        // reads the bottom term on every later leaf while the leaf
        // comparator is created; both happen before any scorer exists.
        // Under the early hint those reads see the sparse dictionary.
        WeightSearcher searcher = new WeightSearcher(reader);
        Weight weight = searcher.createWeight(new LanceFtsQuery("body", "sp7"), ScoreMode.COMPLETE, 1f);
        assertTrue(weight instanceof LanceHintingWeight);
        for (LeafReaderContext ctx : reader.leaves()) {
            ((LanceHintingWeight) weight).hintExclusive(ctx);
        }
        List<int[]> hinted = new ArrayList<>();
        for (int f = 0; f < FRAGMENTS; f++) {
            LanceFragmentLeafReader leaf = leaves.get(f);
            assertTrue(leaf.hintExclusive());
            assertEquals(SP_HITS_PER_FRAGMENT, leaf.hintedOffsets().length);
            hinted.add(leaf.hintedOffsets());
            // What getMaxOrd does before collection: read the value count.
            SortedSetDocValues values = leaf.getSortedSetDocValues("category");
            // sp7 rows are i % 625 == 7 (16 per fragment); category is
            // null for i % 4 == 3, else "c" + (i % 3).
            TreeSet<String> distinct = new TreeSet<>();
            for (int offset : leaf.hintedOffsets()) {
                String category = category(rowId(f, offset));
                if (category != null) {
                    distinct.add(category);
                }
            }
            assertEquals("fragment " + f, distinct.size(), values.getValueCount());
            assertTrue("fragment " + f, leaf.isServingSparse("category"));
            assertFalse("fragment " + f, leaf.isColumnFullyLoaded("category"));
        }

        // A keyword sort with a page smaller than one fragment's hits,
        // driven through the same Weight: the later leaves' comparators
        // look the bottom term up in the sparse dictionary now (without
        // the early hint they would have loaded the full one, see
        // testKeywordSortLoadsTheFullDictionaryOnLeavesVisitedWithAFullQueue).
        List<Integer> expected = sp7Rows();
        expected.sort(BY_CATEGORY);
        Sort byCategory = new Sort(new SortField("category", SortField.Type.STRING));
        TopFieldDocs docs = searcher.search(weight, new TopFieldCollectorManager(byCategory, 5, null, 1000));
        assertEquals(expected.subList(0, 5), globalIds(docs.scoreDocs));
        for (int f = 0; f < FRAGMENTS; f++) {
            LanceFragmentLeafReader leaf = leaves.get(f);
            assertSame("the scorer handed the reader the array it was hinted with", hinted.get(f), leaf.hintedOffsets());
            assertTrue(leaf.hintExclusive());
            assertTrue("fragment " + f, leaf.isServingSparse("category"));
            assertFalse("fragment " + f, leaf.isColumnFullyLoaded("category"));
        }
        // The same Weight serves a second collection (the executor's
        // aggregation phase after the hits phase) without touching the
        // dictionaries again.
        TopFieldDocs again = searcher.search(weight, new TopFieldCollectorManager(byCategory, 100, null, 1000));
        assertEquals(expected, globalIds(again.scoreDocs));
        for (LanceFragmentLeafReader leaf : leaves) {
            assertFalse(leaf.isColumnFullyLoaded("category"));
        }
    }

    public void testEarlyExclusiveHintFromTheKnnWeight() throws Exception {
        WeightSearcher searcher = new WeightSearcher(reader);
        float[] vector = new float[8];
        vector[0] = ROWS_PER_FRAGMENT + 50.4f;
        Weight weight = searcher.createWeight(new LanceKnnQuery("embedding", vector, 3), ScoreMode.COMPLETE, 1f);
        for (LeafReaderContext ctx : reader.leaves()) {
            ((LanceHintingWeight) weight).hintExclusive(ctx);
        }
        // Rows 10,049, 10,050, 10,051 are the three nearest, all in fragment 1.
        assertArrayEquals(new int[] { 49, 50, 51 }, leaves.get(1).hintedOffsets());
        assertTrue(leaves.get(1).hintExclusive());
        assertEquals(0, leaves.get(0).hintedOffsets().length);
        assertTrue(leaves.get(0).hintExclusive());
        assertEquals(0, leaves.get(2).hintedOffsets().length);
        assertTrue(leaves.get(2).hintExclusive());
        // 10,049 -> c2, 10,050 -> c0, 10,051 -> null.
        assertEquals("c2", category(rowId(1, 49)));
        assertEquals("c0", category(rowId(1, 50)));
        assertNull(category(rowId(1, 51)));
        assertEquals(2L, leaves.get(1).getSortedSetDocValues("category").getValueCount());
        assertEquals(0L, leaves.get(0).getSortedSetDocValues("category").getValueCount());
        assertEquals(0L, leaves.get(2).getSortedSetDocValues("category").getValueCount());
        for (LanceFragmentLeafReader leaf : leaves) {
            assertTrue(leaf.isServingSparse("category"));
            assertFalse(leaf.isColumnFullyLoaded("category"));
        }
        TopFieldDocs docs = searcher.search(
            weight,
            new TopFieldCollectorManager(new Sort(new SortField("category", SortField.Type.STRING)), 10, null, 1000)
        );
        // Missing terms sort first: 10,051 (null), then 10,050 (c0), 10,049 (c2).
        assertEquals(List.of(rowId(1, 51), rowId(1, 50), rowId(1, 49)), globalIds(docs.scoreDocs));
        for (LanceFragmentLeafReader leaf : leaves) {
            assertFalse(leaf.isColumnFullyLoaded("category"));
        }
    }

    public void testEarlyHintUnderAForeignWrapperIsDeliveredButNotExclusive() throws Exception {
        try (DirectoryReader wrapped = new ForeignWrapper(reader)) {
            IndexSearcher searcher = new IndexSearcher(wrapped);
            Weight weight = searcher.createWeight(new LanceFtsQuery("body", "sp7"), ScoreMode.COMPLETE, 1f);
            for (LeafReaderContext ctx : wrapped.leaves()) {
                ((LanceHintingWeight) weight).hintExclusive(ctx);
            }
            for (LanceFragmentLeafReader leaf : leaves) {
                assertEquals(SP_HITS_PER_FRAGMENT, leaf.hintedOffsets().length);
                assertFalse(leaf.hintExclusive());
                assertEquals("full dictionary of the fragment", 3L, leaf.getSortedSetDocValues("category").getValueCount());
                assertFalse(leaf.isServingSparse("category"));
            }
        }
    }

    /**
     * {@link IndexSearcher} that runs a caller-built {@link Weight}
     * through a {@link CollectorManager}, the way the fragment executor
     * drives its phases off the one Weight it created up front.
     */
    private static final class WeightSearcher extends IndexSearcher {
        WeightSearcher(DirectoryReader reader) {
            super(reader);
        }

        <C extends Collector, T> T search(Weight weight, CollectorManager<C, T> manager) throws IOException {
            C collector = manager.newCollector();
            LeafReaderContextPartition[] partitions = getLeafContexts().stream()
                .map(LeafReaderContextPartition::createForEntireSegment)
                .toArray(LeafReaderContextPartition[]::new);
            search(partitions, weight, collector);
            return manager.reduce(Collections.singletonList(collector));
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

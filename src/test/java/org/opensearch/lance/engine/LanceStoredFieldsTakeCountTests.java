/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.mapper.Uid;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.dispatch.HitProjection;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceFragmentSchema.TakeProjection;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.stats.LanceNodeStats;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.test.OpenSearchTestCase;

/**
 * How many take scans a page of hits costs and which columns they
 * project, as {@link FetchTakeStats.Accumulator} counts them on the
 * leaves of a request: a score ordered page takes once per leaf that
 * holds a hit and addresses every hit of the leaf in that take, a sort
 * column the doc values answer from the loaded column is never part of
 * the stored fields take, and {@code _source} includes, {@code fields}
 * and {@code docvalue_fields} on one request project the union of the
 * columns the first two name, once each, with the primary key.
 *
 * <p>Fixture: {@link LanceTableFactory#writeContiguousScoreTable} with
 * three fragments of 100 rows, {@code tied} false, so the columns are
 * {@code id, body, category} in schema order and the BM25 score of the
 * token {@code lance} falls strictly with the row id: the ten best
 * hits are rows 0 to 9 of fragment 0, the best 250 span the three
 * fragments as 100, 100 and 50.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceStoredFieldsTakeCountTests extends OpenSearchTestCase {

    private static final int FRAGMENTS = 3;
    private static final int ROWS_PER_FRAGMENT = 100;

    private Dataset dataset;
    private List<Integer> fragmentIds;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeContiguousScoreTable(scratchDir, "take-" + getTestName(), FRAGMENTS, ROWS_PER_FRAGMENT, false);
        dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        fragmentIds = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            fragmentIds.add(fragment.getId());
        }
        assertEquals(FRAGMENTS, fragmentIds.size());
    }

    @Override
    public void tearDown() throws Exception {
        if (dataset != null) {
            dataset.close();
        }
        super.tearDown();
    }

    /** A reader over the three fragments, {@code id} as the primary key or none. */
    private LanceDirectoryReader open(boolean idIsPrimaryKey) throws Exception {
        return LanceDirectoryReader.openForFragments(
            new ByteBuffersDirectory(),
            null,
            dataset,
            idIsPrimaryKey ? "id" : "",
            idIsPrimaryKey ? LancePrimaryKeyType.LONG : LancePrimaryKeyType.NONE,
            LanceOverrides.EMPTY,
            fragmentIds
        );
    }

    private static List<LanceFragmentLeafReader> leavesOf(LanceDirectoryReader reader) {
        List<LanceFragmentLeafReader> leaves = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(ctx.reader());
            assertNotNull(leaf);
            leaves.add(leaf);
        }
        assertEquals(FRAGMENTS, leaves.size());
        return leaves;
    }

    private static HitProjection projection(FetchSourceContext source, List<String> docValueFields, String... fields) {
        List<FieldAndFormat> fetchFields = new ArrayList<>();
        for (String field : fields) {
            fetchFields.add(new FieldAndFormat(field, null));
        }
        List<FieldAndFormat> docValues = new ArrayList<>();
        for (String field : docValueFields) {
            docValues.add(new FieldAndFormat(field, null));
        }
        return new HitProjection(source, null, docValues, fetchFields, false);
    }

    /**
     * The page's doc ids grouped by leaf index, leaf local, in leaf
     * order: the slices the fragment executor hands each leaf's
     * {@link LanceFragmentLeafReader#prefetchRows} before the fetch
     * phase renders the page.
     */
    private static Map<Integer, int[]> docsByLeaf(LanceDirectoryReader reader, ScoreDoc[] scoreDocs) {
        List<LeafReaderContext> leaves = reader.leaves();
        Map<Integer, List<Integer>> grouped = new TreeMap<>();
        for (ScoreDoc scoreDoc : scoreDocs) {
            int leafIndex = ReaderUtil.subIndex(scoreDoc.doc, leaves);
            grouped.computeIfAbsent(leafIndex, k -> new ArrayList<>()).add(scoreDoc.doc - leaves.get(leafIndex).docBase);
        }
        Map<Integer, int[]> out = new TreeMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : grouped.entrySet()) {
            int[] docs = new int[entry.getValue().size()];
            for (int i = 0; i < docs.length; i++) {
                docs[i] = entry.getValue().get(i);
            }
            out.put(entry.getKey(), docs);
        }
        return out;
    }

    /**
     * Prepare the leaves for one request as the fragment executor does
     * (one accumulator, one projection, rows of an earlier request
     * dropped) and take the page's rows once per leaf with hits.
     */
    private static FetchTakeStats.Accumulator takePage(
        LanceDirectoryReader reader,
        List<LanceFragmentLeafReader> leaves,
        TakeProjection projection,
        ScoreDoc[] scoreDocs
    ) throws Exception {
        FetchTakeStats.Accumulator takes = new FetchTakeStats.Accumulator();
        for (LanceFragmentLeafReader leaf : leaves) {
            leaf.setTakeAccumulator(takes);
            leaf.setTakeProjection(projection);
        }
        for (Map.Entry<Integer, int[]> slice : docsByLeaf(reader, scoreDocs).entrySet()) {
            leaves.get(slice.getKey()).prefetchRows(slice.getValue());
        }
        return takes;
    }

    /**
     * Render {@code _id} and, with {@code renderSource}, {@code _source}
     * of every hit of the page through the leaves, as the fetch phase
     * does for a body with and without {@code _source}.
     */
    private static List<Collected> render(
        LanceDirectoryReader reader,
        List<LanceFragmentLeafReader> leaves,
        ScoreDoc[] scoreDocs,
        boolean renderSource
    ) throws Exception {
        List<Collected> out = new ArrayList<>(scoreDocs.length);
        List<LeafReaderContext> contexts = reader.leaves();
        for (ScoreDoc scoreDoc : scoreDocs) {
            int leafIndex = ReaderUtil.subIndex(scoreDoc.doc, contexts);
            Collected collected = new Collected(renderSource);
            leaves.get(leafIndex).materialiseStoredFields(scoreDoc.doc - contexts.get(leafIndex).docBase, collected);
            out.add(collected);
        }
        return out;
    }

    public void testScoreOrderedPageTakesOncePerLeafWithHits() throws Exception {
        try (LanceDirectoryReader reader = open(true)) {
            List<LanceFragmentLeafReader> leaves = leavesOf(reader);
            LanceFragmentSchema schema = leaves.get(0).schema();
            TakeProjection full = HitProjection.NONE.takeProjection(schema);
            assertEquals(List.of("id", "body", "category"), full.columns());
            IndexSearcher searcher = new IndexSearcher(reader);
            LanceFtsQuery lance = new LanceFtsQuery("body", "lance");

            // Ten hits by score: rows 0 to 9, all in fragment 0. One
            // take, addressing the ten rows, projecting the three
            // columns.
            TopDocs ten = searcher.search(lance, 10);
            assertEquals(10, ten.scoreDocs.length);
            Map<Integer, int[]> tenByLeaf = docsByLeaf(reader, ten.scoreDocs);
            assertEquals("the ten best scores sit in fragment 0: " + tenByLeaf.keySet(), List.of(0), new ArrayList<>(tenByLeaf.keySet()));
            LanceNodeStats.FetchStats before = FetchTakeStats.snapshot();
            FetchTakeStats.Accumulator tenTakes = takePage(reader, leaves, full, ten.scoreDocs);
            assertEquals("one take for the one leaf with hits", 1L, tenTakes.takeCount());
            assertEquals("the take addressed every hit of the leaf", 10L, tenTakes.takeRows());
            assertEquals("the take projected the three columns", 3L, tenTakes.takeColumns());
            LanceNodeStats.FetchStats after = FetchTakeStats.snapshot();
            assertEquals(before.storedFieldsTakes() + 1, after.storedFieldsTakes());
            assertEquals("no column take: the score is the order", before.columnTakes(), after.columnTakes());

            // Rendering the page reads the taken rows: no take per row.
            List<Collected> tenHits = render(reader, leaves, ten.scoreDocs, true);
            for (int i = 0; i < 10; i++) {
                assertEquals(Integer.toString(i), tenHits.get(i).id());
                assertEquals("c" + (i % 3), tenHits.get(i).source().get("category"));
            }
            assertEquals("the fetch phase issued no further take", 1L, tenTakes.takeCount());
            assertEquals(10L, tenTakes.takeRows());

            // A page of 250 hits spans the three fragments as 100, 100
            // and 50: one take per leaf, each addressing that leaf's
            // hits, each projecting the three columns.
            TopDocs wide = searcher.search(lance, 250);
            assertEquals(250, wide.scoreDocs.length);
            Map<Integer, int[]> wideByLeaf = docsByLeaf(reader, wide.scoreDocs);
            assertEquals(List.of(0, 1, 2), new ArrayList<>(wideByLeaf.keySet()));
            assertEquals(100, wideByLeaf.get(0).length);
            assertEquals(100, wideByLeaf.get(1).length);
            assertEquals(50, wideByLeaf.get(2).length);
            FetchTakeStats.Accumulator wideTakes = takePage(reader, leaves, full, wide.scoreDocs);
            assertEquals("one take per leaf with hits", (long) FRAGMENTS, wideTakes.takeCount());
            assertEquals("the takes addressed every hit once", 250L, wideTakes.takeRows());
            assertEquals("every take projected the three columns", 3L * FRAGMENTS, wideTakes.takeColumns());
            List<Collected> wideHits = render(reader, leaves, wide.scoreDocs, true);
            for (int i = 0; i < 250; i++) {
                assertEquals(Integer.toString(i), wideHits.get(i).id());
            }
            assertEquals((long) FRAGMENTS, wideTakes.takeCount());
        }
    }

    public void testSortColumnAnsweredByTheLoadedColumnIsNotTaken() throws Exception {
        // No primary key: _id is synthesised from the row address, so
        // the take carries a column only when _source or fields ask for
        // it, and the sort column joins it on no other ground.
        try (LanceDirectoryReader reader = open(false)) {
            List<LanceFragmentLeafReader> leaves = leavesOf(reader);
            LanceFragmentSchema schema = leaves.get(0).schema();
            IndexSearcher searcher = new IndexSearcher(reader);
            Sort byIdDesc = new Sort(new SortField("id", SortField.Type.LONG, true));

            // The sort reads id through the doc values: the whole column
            // is loaded on every leaf and no take runs for it.
            LanceNodeStats.FetchStats before = FetchTakeStats.snapshot();
            TopDocs top = searcher.search(MatchAllDocsQuery.INSTANCE, 10, byIdDesc);
            assertEquals(10, top.scoreDocs.length);
            Map<Integer, int[]> byLeaf = docsByLeaf(reader, top.scoreDocs);
            assertEquals("the ten largest ids sit in the last fragment", List.of(2), new ArrayList<>(byLeaf.keySet()));
            for (LanceFragmentLeafReader leaf : leaves) {
                assertTrue("the sort loaded id on fragment " + leaf.fragmentId(), leaf.isColumnFullyLoaded("id"));
            }
            LanceNodeStats.FetchStats sorted = FetchTakeStats.snapshot();
            assertEquals("the sort took no column", before.columnTakes(), sorted.columnTakes());
            assertEquals(before.takeCount(), sorted.takeCount());

            // _source: false: nothing to project, so no take at all, and
            // _id still renders from the row address.
            TakeProjection noSource = projection(FetchSourceContext.DO_NOT_FETCH_SOURCE, List.of()).takeProjection(schema);
            assertEquals(List.of(), noSource.columns());
            FetchTakeStats.Accumulator noSourceTakes = takePage(reader, leaves, noSource, top.scoreDocs);
            assertEquals("nothing to take", 0L, noSourceTakes.takeCount());
            List<Collected> noSourceHits = render(reader, leaves, top.scoreDocs, false);
            assertEquals("2-99", noSourceHits.get(0).id());
            assertNull(noSourceHits.get(0).source());
            assertEquals(0L, noSourceTakes.takeCount());

            // An includes filter on another column: the take projects
            // that column alone; the sort column stays out.
            TakeProjection categoryOnly = projection(new FetchSourceContext(true, new String[] { "category" }, null), List.of())
                .takeProjection(schema);
            assertEquals(List.of("category"), categoryOnly.columns());
            FetchTakeStats.Accumulator categoryTakes = takePage(reader, leaves, categoryOnly, top.scoreDocs);
            assertEquals(1L, categoryTakes.takeCount());
            assertEquals(10L, categoryTakes.takeRows());
            assertEquals("category alone", 1L, categoryTakes.takeColumns());
            List<Collected> categoryHits = render(reader, leaves, top.scoreDocs, true);
            assertEquals(Map.of("category", "c" + (299 % 3)), categoryHits.get(0).source());

            // docvalue_fields on the sort column: the doc values answer
            // it, the take still projects nothing.
            TakeProjection docValues = projection(FetchSourceContext.DO_NOT_FETCH_SOURCE, List.of("id")).takeProjection(schema);
            assertEquals(List.of(), docValues.columns());
            assertEquals(0L, takePage(reader, leaves, docValues, top.scoreDocs).takeCount());

            // fields naming the sort column: now the take projects it,
            // because the fields phase reads it from the source.
            TakeProjection idField = projection(FetchSourceContext.DO_NOT_FETCH_SOURCE, List.of(), "id").takeProjection(schema);
            assertEquals(List.of("id"), idField.columns());
            FetchTakeStats.Accumulator idTakes = takePage(reader, leaves, idField, top.scoreDocs);
            assertEquals(1L, idTakes.takeCount());
            assertEquals("id alone", 1L, idTakes.takeColumns());

            LanceNodeStats.FetchStats after = FetchTakeStats.snapshot();
            assertEquals(
                "two stored fields takes, one per projection that names a column",
                sorted.storedFieldsTakes() + 2,
                after.storedFieldsTakes()
            );
            assertEquals("still no column take", before.columnTakes(), after.columnTakes());
        }
    }

    public void testSourceIncludesFieldsAndDocValueFieldsProjectTheUnionOnce() throws Exception {
        try (LanceDirectoryReader reader = open(true)) {
            List<LanceFragmentLeafReader> leaves = leavesOf(reader);
            LanceFragmentSchema schema = leaves.get(0).schema();

            // includes category, fields body and cat*, docvalue_fields
            // category and id: body and category once each, in schema
            // order, then the key; nothing for docvalue_fields.
            TakeProjection combined = projection(
                new FetchSourceContext(true, new String[] { "category" }, null),
                List.of("category", "id"),
                "body",
                "cat*"
            ).takeProjection(schema);
            assertEquals(new TakeProjection(List.of("body", "category", "id"), 2, 2), combined);

            // The key named by the filter and by fields is taken once, at
            // its place among the source columns.
            TakeProjection keyTwice = projection(new FetchSourceContext(true, new String[] { "id" }, null), List.of("id"), "id")
                .takeProjection(schema);
            assertEquals(new TakeProjection(List.of("id"), 1, 0), keyTwice);

            // docvalue_fields alone: the key for _id, nothing else.
            TakeProjection docValuesOnly = projection(FetchSourceContext.DO_NOT_FETCH_SOURCE, List.of("category", "body")).takeProjection(
                schema
            );
            assertEquals(new TakeProjection(List.of("id"), 0, 0), docValuesOnly);

            // fields on a column the filter excludes: taken for fields,
            // with the rest of the source.
            TakeProjection excludedButNamed = projection(new FetchSourceContext(true, null, new String[] { "body" }), List.of(), "body")
                .takeProjection(schema);
            assertEquals(new TakeProjection(List.of("id", "body", "category"), 3, 0), excludedButNamed);

            // The combined projection through a leaf: one take of the
            // three columns for a page in fragment 0, _source with body
            // and category in schema order, _id from the key.
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs ten = searcher.search(new LanceFtsQuery("body", "lance"), 10);
            FetchTakeStats.Accumulator takes = takePage(reader, leaves, combined, ten.scoreDocs);
            assertEquals(1L, takes.takeCount());
            assertEquals(10L, takes.takeRows());
            assertEquals("body, category and the key", 3L, takes.takeColumns());
            List<Collected> hits = render(reader, leaves, ten.scoreDocs, true);
            Collected fifth = hits.get(5);
            assertEquals("5", fifth.id());
            assertEquals(List.of("body", "category"), new ArrayList<>(fifth.source().keySet()));
            assertEquals("c2", fifth.source().get("category"));
            assertTrue(String.valueOf(fifth.source().get("body")), ((String) fifth.source().get("body")).startsWith("lance lance"));
            assertEquals("rendering took nothing more", 1L, takes.takeCount());
        }
    }

    /** A visitor that keeps the {@code _id} and, when the request renders it, the {@code _source} a leaf renders. */
    private static final class Collected extends StoredFieldVisitor {
        private final boolean needsSource;
        private String id;
        private byte[] source;

        Collected(boolean needsSource) {
            this.needsSource = needsSource;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            if ("_id".equals(fieldInfo.name)) {
                return Status.YES;
            }
            if ("_source".equals(fieldInfo.name)) {
                return needsSource ? Status.YES : Status.NO;
            }
            return Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            if ("_id".equals(fieldInfo.name)) {
                id = Uid.decodeId(value);
            } else if ("_source".equals(fieldInfo.name)) {
                source = value;
            }
        }

        String id() {
            return id;
        }

        Map<String, Object> source() {
            if (source == null) {
                return null;
            }
            assertTrue(new String(source, StandardCharsets.UTF_8), source.length > 0);
            return XContentHelper.convertToMap(new BytesArray(source), true, XContentType.JSON).v2();
        }
    }
}

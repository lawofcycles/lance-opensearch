/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.cluster.service.ClusterService;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.CheckedFunction;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * {@code hits.total.value} under a reader wrapper has to be the number
 * of rows the wrapper lets through, for every way of spelling
 * {@code match_all} and whatever {@code size} the request carries.
 *
 * <p>{@link DlsLikeReaderWrapperPlugin} installs a wrapper shaped like
 * the security plugin's DLS leaf reader: {@code getLiveDocs()} hides the
 * odd offset of every fragment leaf and {@code hasDeletions()} is true,
 * while {@code numDocs()} stays at the unfiltered value of the wrapped
 * leaf. That last detail is what a {@code MatchAllDocsQuery} Weight
 * answers {@code IndexSearcher.count} with, so a count that reaches
 * that shortcut reports the hidden rows too. The tests drive the
 * executor directly for the per node {@code matched} value and the
 * search API for the merged {@code hits.total.value}; the search API
 * cases also cover a scalar filter and a bare FTS clause, whose Lance
 * scan must not be clipped to {@code size} before the wrapper hides
 * rows, or the page comes back short.
 *
 * <p>Thread leak checks are off as in the other tests that load the Lance
 * native library.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ReaderWrapperMatchAllTotalTests extends OpenSearchSingleNodeTestCase {

    private static final int ROWS = 12;
    private static final int ROWS_PER_FRAGMENT = 4;
    private static final int VISIBLE_ROWS = ROWS / 2;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class, DlsLikeReaderWrapperPlugin.class);
    }

    private String attach(String indexName) throws Exception {
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeMultiFragmentTable(dir, indexName, ROWS, ROWS_PER_FRAGMENT);
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        assertEquals(ROWS / ROWS_PER_FRAGMENT, attached.fragments());
        ensureGreen(indexName);
        return tableUri;
    }

    /**
     * Every {@code match_all} spelling the coordinator ships as a
     * {@link QueryBuilder} (it translates none of them to Lance SQL),
     * plus the absent query.
     */
    private static Map<String, QueryBuilder> matchAllSpellings() {
        Map<String, QueryBuilder> spellings = new LinkedHashMap<>();
        spellings.put("no query", null);
        spellings.put("match_all", new MatchAllQueryBuilder());
        spellings.put("match_all boost 2", new MatchAllQueryBuilder().boost(2f));
        spellings.put("bool must match_all", QueryBuilders.boolQuery().must(new MatchAllQueryBuilder()));
        spellings.put("bool filter match_all", QueryBuilders.boolQuery().filter(new MatchAllQueryBuilder()));
        spellings.put("constant_score match_all", QueryBuilders.constantScoreQuery(new MatchAllQueryBuilder()));
        return spellings;
    }

    public void testExecutorMatchedCountsOnlyVisibleRowsForEveryMatchAllSpelling() throws Exception {
        String indexName = "wrapped-matched";
        String tableUri = attach(indexName);
        TransportLanceFragmentQueryAction executor = getInstanceFromNode(TransportLanceFragmentQueryAction.class);
        // The coordinator folds `from` into the per node size, so
        // `from 3, size 5` arrives here as size 8.
        int[] sizes = { 0, 1, 5, 8 };
        for (Map.Entry<String, QueryBuilder> spelling : matchAllSpellings().entrySet()) {
            for (int size : sizes) {
                for (int trackTotalHitsUpTo : new int[] {
                    SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO,
                    SearchContext.TRACK_TOTAL_HITS_ACCURATE }) {
                    String label = spelling.getKey() + " size " + size + " track_total_hits " + trackTotalHitsUpTo;
                    LanceFragmentQueryRequest request = FragmentRequests.planned(
                        getInstanceFromNode(ClusterService.class),
                        getInstanceFromNode(LanceWarmCache.class),
                        tableUri,
                        indexName,
                        spelling.getValue(),
                        /* postFilter */ null,
                        List.of(),
                        /* searchAfter */ null,
                        size,
                        /* aggregations */ null,
                        List.of(),
                        false,
                        trackTotalHitsUpTo
                    );
                    LanceFragmentQueryResponse response = executor.execute(request);
                    assertEquals(label + ": matched", VISIBLE_ROWS, response.matched());
                    assertFalse(label + ": matched is a lower bound", response.matchedIsLowerBound());
                    assertEquals(label + ": hits", Math.min(size, VISIBLE_ROWS), response.hits().size());
                    assertOnlyVisibleRows(label, response.hits());
                }
            }
        }
    }

    public void testSearchTotalCountsOnlyVisibleRowsForEveryMatchAllSpelling() throws Exception {
        String indexName = "wrapped-total";
        attach(indexName);
        Map<String, SearchSourceBuilder> requests = new LinkedHashMap<>();
        for (Map.Entry<String, QueryBuilder> spelling : matchAllSpellings().entrySet()) {
            for (int size : new int[] { 0, 1, 5 }) {
                requests.put(spelling.getKey() + " size " + size, new SearchSourceBuilder().query(spelling.getValue()).size(size));
            }
        }
        requests.put("match_all from 3 size 5", new SearchSourceBuilder().query(new MatchAllQueryBuilder()).from(3).size(5));
        requests.put(
            "match_all size 5 _source false",
            new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5).fetchSource(false)
        );
        requests.put(
            "match_all size 5 track_total_hits true",
            new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5).trackTotalHits(true)
        );
        // Shapes whose Lance scan would be clipped to `size` rows
        // before the wrapper hides any of them: a scalar filter the
        // coordinator translates to Lance SQL, and a bare FTS clause.
        // Every row of the fixture matches both.
        for (int size : new int[] { 0, 5 }) {
            requests.put("range id gte 0 size " + size, new SearchSourceBuilder().query(QueryBuilders.rangeQuery("id").gte(0)).size(size));
            requests.put(
                "lance_match title morning size " + size,
                new SearchSourceBuilder().query(new LanceMatchQueryBuilder("title", "morning")).size(size)
            );
        }
        for (Map.Entry<String, SearchSourceBuilder> entry : requests.entrySet()) {
            SearchSourceBuilder source = entry.getValue();
            SearchResponse response = client().prepareSearch(indexName).setSource(source).get();
            String label = entry.getKey();
            assertNotNull(label + ": hits.total", response.getHits().getTotalHits());
            assertEquals(label + ": hits.total.value", VISIBLE_ROWS, response.getHits().getTotalHits().value());
            assertEquals(label + ": hits.total.relation", TotalHits.Relation.EQUAL_TO, response.getHits().getTotalHits().relation());
            if (source.size() == 0) {
                // No hits phase runs for size 0, so the count is the
                // only observable output here; pin it on its own so a
                // regression on the count-only path fails by itself.
                assertEquals(label + ": size 0 count", VISIBLE_ROWS, response.getHits().getTotalHits().value());
                assertEquals(label + ": size 0 returns no hits", 0, response.getHits().getHits().length);
                continue;
            }
            int from = Math.max(source.from(), 0);
            int expectedHits = Math.max(0, Math.min(source.size(), VISIBLE_ROWS - from));
            assertEquals(label + ": hits", expectedHits, response.getHits().getHits().length);
            assertOnlyVisibleRows(label, List.of(response.getHits().getHits()));
        }
    }

    private static void assertOnlyVisibleRows(String label, List<SearchHit> hits) {
        for (SearchHit hit : hits) {
            // Ids are "<fragment>-<offset>"; the wrapper hides odd
            // offsets inside every fragment leaf.
            int offset = Integer.parseInt(hit.getId().substring(hit.getId().indexOf('-') + 1));
            assertEquals(label + ": hit " + hit.getId() + " should have been hidden", 0, offset % 2);
        }
    }

    /**
     * Installs a reader wrapper on every Lance-backed index that hides
     * the odd documents of each leaf the way the security plugin's DLS
     * leaf reader does: filtered {@code getLiveDocs()},
     * {@code hasDeletions() == true}, and {@code numDocs()} left at the
     * wrapped leaf's unfiltered value.
     */
    public static class DlsLikeReaderWrapperPlugin extends Plugin {

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (indexModule.getSettings().get(LanceEngineFactory.TABLE_SETTING) != null) {
                indexModule.setReaderWrapper(indexService -> new DlsLikeWrapperFactory());
            }
        }
    }

    static final class DlsLikeWrapperFactory implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {
        @Override
        public DirectoryReader apply(DirectoryReader reader) throws IOException {
            return new DlsLikeDirectoryReader(reader);
        }
    }

    static final class DlsLikeDirectoryReader extends FilterDirectoryReader {

        DlsLikeDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new DlsLikeLeafReader(reader);
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new DlsLikeDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    static final class DlsLikeLeafReader extends FilterLeafReader {

        private final FixedBitSet liveDocs;

        DlsLikeLeafReader(LeafReader in) {
            super(in);
            Bits inner = in.getLiveDocs();
            liveDocs = new FixedBitSet(in.maxDoc());
            for (int doc = 0; doc < in.maxDoc(); doc += 2) {
                if (inner == null || inner.get(doc)) {
                    liveDocs.set(doc);
                }
            }
        }

        @Override
        public Bits getLiveDocs() {
            return liveDocs;
        }

        @Override
        public boolean hasDeletions() {
            return true;
        }

        @Override
        public int numDocs() {
            // Deliberately not the cardinality of liveDocs: the security
            // plugin's DlsGetEvaluator keeps in.numDocs() here.
            return in.numDocs();
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }
}

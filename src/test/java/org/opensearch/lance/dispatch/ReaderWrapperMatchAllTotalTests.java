/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.cluster.service.ClusterService;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceSettings;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
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
 * <p>The node runs with {@code plugins.lance.test.hiding_wrapper_index_prefix}
 * set, so the plugin installs its {@code HidingReaderWrapper} on every
 * Lance backed index whose name starts with {@code wrapped}. The wrapper
 * is shaped like the security plugin's DLS leaf reader:
 * {@code getLiveDocs()} hides the rows whose {@code id} is below 6 and
 * {@code hasDeletions()} is true, while {@code numDocs()} stays at the
 * unfiltered value of the wrapped leaf. That last detail is what a
 * {@code MatchAllDocsQuery} Weight answers {@code IndexSearcher.count}
 * with, so a count that reaches that shortcut reports the hidden rows
 * too. With four rows per fragment the first leaf is hidden whole, the
 * second in half and the third not at all. The tests drive the executor
 * directly for the per node {@code matched} value and the search API for
 * the merged {@code hits.total.value}; the search API cases also cover a
 * scalar filter and a bare FTS clause, whose Lance scan must not be
 * clipped to {@code size} before the wrapper hides rows, or the page
 * comes back short.
 *
 * <p>Thread leak checks are off as in the other tests that load the Lance
 * native library.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ReaderWrapperMatchAllTotalTests extends OpenSearchSingleNodeTestCase {

    private static final int ROWS = 12;
    private static final int ROWS_PER_FRAGMENT = 4;
    /** Rows whose {@code id} is below this are hidden. */
    private static final int FIRST_VISIBLE_ID = 6;
    private static final int VISIBLE_ROWS = ROWS - FIRST_VISIBLE_ID;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(LanceSettings.TEST_HIDING_WRAPPER_INDEX_PREFIX_SETTING.getKey(), "wrapped:body:id:" + FIRST_VISIBLE_ID)
            .build();
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
            // Ids are "<fragment>-<offset>" and row i sits at fragment
            // i / 4, offset i % 4; the wrapper hides the rows below id 6.
            int dash = hit.getId().indexOf('-');
            int fragment = Integer.parseInt(hit.getId().substring(0, dash));
            int offset = Integer.parseInt(hit.getId().substring(dash + 1));
            int row = fragment * ROWS_PER_FRAGMENT + offset;
            assertTrue(label + ": hit " + hit.getId() + " should have been hidden", row >= FIRST_VISIBLE_ID);
        }
    }
}

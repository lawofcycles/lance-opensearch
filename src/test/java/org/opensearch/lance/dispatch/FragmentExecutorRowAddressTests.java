/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.lance.plan.execute.MergeReducer;
import org.opensearch.lance.plan.execute.MergeReducer.RankedHit;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * The row address the executor reports for every hit, on the three hits
 * paths (scored Lucene page, sorted Lucene page, Lance sort pushdown),
 * and the order in which one executor returns hits with equal sort
 * values. The fixture has no primary key, so a hit's {@code _id} is
 * {@code <fragment>-<offset>}, which is the row address spelled out.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class FragmentExecutorRowAddressTests extends OpenSearchSingleNodeTestCase {

    private static final int FRAGMENTS = 3;
    private static final int ROWS_PER_FRAGMENT = 100;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
    }

    private String attach(String indexName) throws Exception {
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeHintFixtureTable(dir, indexName, FRAGMENTS, ROWS_PER_FRAGMENT);
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        assertEquals(FRAGMENTS, attached.fragments());
        ensureGreen(indexName);
        return tableUri;
    }

    private LanceFragmentQueryResponse run(
        String tableUri,
        String indexName,
        QueryBuilder query,
        String filterSql,
        List<SortBuilder<?>> sorts,
        int size,
        List<Integer> fragmentIds
    ) throws Exception {
        TransportLanceFragmentQueryAction executor = getInstanceFromNode(TransportLanceFragmentQueryAction.class);
        return executor.execute(
            new LanceFragmentQueryRequest(
                tableUri,
                indexName,
                StorageOptions.empty(),
                /* pinnedVersion */ -1L,
                filterSql,
                query,
                /* postFilter */ null,
                sorts,
                /* searchAfter */ null,
                size,
                /* aggregations */ null,
                fragmentIds,
                /* trackScores */ false,
                SearchContext.TRACK_TOTAL_HITS_ACCURATE
            )
        );
    }

    public void testScoredPageReportsRowAddressesAndOrdersTiesByThem() throws Exception {
        String indexName = "row-address-scored";
        String tableUri = attach(indexName);
        // lance is repeated (i % 5) + 1 times, so the 60 rows with
        // i % 5 == 4 share the top score: 20 in fragment 0, 20 in
        // fragment 1, 20 in fragment 2. A page of 30 is all ties. The
        // explicit _score sort keeps the Lance scan unbounded, so
        // Lucene's collector sees every match and orders the ties by
        // doc id.
        List<SortBuilder<?>> sorts = List.of(new ScoreSortBuilder().order(SortOrder.DESC));
        LanceFragmentQueryResponse response = run(
            tableUri,
            indexName,
            new LanceMatchQueryBuilder("body", "lance"),
            null,
            sorts,
            30,
            List.of()
        );
        assertEquals(30, response.hits().size());
        assertRowAddressesMatchIds(response);
        float top = sortScore(response.hits().get(0));
        for (int i = 1; i < response.hits().size(); i++) {
            assertEquals("hit " + i + " is a tie", top, sortScore(response.hits().get(i)), 0f);
            assertTrue("ties ascend by row address at " + i, response.rowAddrs()[i - 1] < response.rowAddrs()[i]);
        }
        // Rows 4, 9, ..., 99 of fragment 0, then 104, ..., 149 of fragment 1.
        List<String> expected = new ArrayList<>();
        for (int i = 4; i < 150; i += 5) {
            expected.add((i / ROWS_PER_FRAGMENT) + "-" + (i % ROWS_PER_FRAGMENT));
        }
        assertEquals(expected, ids(response.hits()));
    }

    public void testBoundedScanPageReportsRowAddresses() throws Exception {
        String indexName = "row-address-bounded";
        String tableUri = attach(indexName);
        // Without a sort the executor clips the Lance FTS scan to the
        // page size and Lance decides which of the tied rows survive
        // the clip; whichever they are, every hit carries its address
        // and the executor orders them by it.
        LanceFragmentQueryResponse response = run(
            tableUri,
            indexName,
            new LanceMatchQueryBuilder("body", "lance"),
            null,
            List.of(),
            30,
            List.of()
        );
        assertEquals(30, response.hits().size());
        assertRowAddressesMatchIds(response);
        for (int i = 1; i < response.hits().size(); i++) {
            assertEquals("hit " + i + " is a tie", response.hits().get(0).getScore(), response.hits().get(i).getScore(), 0f);
            assertTrue("ties ascend by row address at " + i, response.rowAddrs()[i - 1] < response.rowAddrs()[i]);
        }
    }

    /**
     * The score of a hit of a request sorted by {@code _score} alone:
     * OpenSearch treats that sort as the default order, so the hits
     * carry no sort values and the score is in {@link SearchHit#getScore()}.
     */
    private static float sortScore(SearchHit hit) {
        Object[] sortValues = hit.getSortValues();
        if (sortValues != null && sortValues.length > 0) {
            return ((Number) sortValues[0]).floatValue();
        }
        return hit.getScore();
    }

    public void testSortedPageReportsRowAddressesAndOrdersTiesByThem() throws Exception {
        String indexName = "row-address-sorted";
        String tableUri = attach(indexName);
        // flag is true on even rows; a descending sort on it puts every
        // even row first, all tied, so the page is ordered by doc id.
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("flag").order(SortOrder.DESC));
        LanceFragmentQueryResponse response = run(
            tableUri,
            indexName,
            new LanceMatchQueryBuilder("body", "hello"),
            null,
            sorts,
            40,
            List.of()
        );
        assertEquals(40, response.hits().size());
        assertRowAddressesMatchIds(response);
        for (int i = 1; i < response.hits().size(); i++) {
            assertTrue("ties ascend by row address at " + i, response.rowAddrs()[i - 1] < response.rowAddrs()[i]);
        }
        // flag is null on rows with i % 7 == 6, and a null sorts last
        // under desc, so the page is the even rows that are not 6 mod 7.
        List<String> expected = new ArrayList<>();
        for (int i = 0; expected.size() < 40; i += 2) {
            if (i % 7 != 6) {
                expected.add((i / ROWS_PER_FRAGMENT) + "-" + (i % ROWS_PER_FRAGMENT));
            }
        }
        assertEquals(expected, ids(response.hits()));
    }

    public void testLanceSortPushdownReportsRowAddresses() throws Exception {
        String indexName = "row-address-pushdown";
        String tableUri = attach(indexName);
        // match_all with a plain field sort and no search_after takes
        // the Lance sort pushdown; rating is distinct per row.
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("rating").order(SortOrder.DESC));
        LanceFragmentQueryResponse response = run(tableUri, indexName, new MatchAllQueryBuilder(), null, sorts, 10, List.of());
        assertEquals(10, response.hits().size());
        assertRowAddressesMatchIds(response);
        // rating is (i * 37) % 1000 and null on i % 5 == 4; 999 is row
        // 27 (27 * 37 = 999), the only preimage below 300.
        assertEquals("0-27", response.hits().get(0).getId());
    }

    public void testPerFragmentPagesMergeToTheWholeTablePage() throws Exception {
        String indexName = "row-address-merge";
        String tableUri = attach(indexName);
        QueryBuilder query = new LanceMatchQueryBuilder("body", "lance");
        int size = 10;
        // With the explicit _score sort the scan is unbounded and every
        // executor orders the ties by doc id, so the merged page is the
        // first ten rows of the top score group, all in fragment 0.
        List<SortBuilder<?>> sorts = List.of(new ScoreSortBuilder().order(SortOrder.DESC));
        List<String> expected = new ArrayList<>();
        for (int i = 4; i < 54; i += 5) {
            expected.add("0-" + i);
        }
        // The reference: one executor over every fragment, the page a
        // single node returns.
        List<String> whole = ids(run(tableUri, indexName, query, null, sorts, size, List.of()).hits());
        assertEquals(expected, whole);
        // One executor per fragment, as on a three node cluster with
        // one fragment each, merged in every node order.
        List<List<RankedHit>> perFragment = new ArrayList<>();
        for (int fragment = 0; fragment < FRAGMENTS; fragment++) {
            LanceFragmentQueryResponse response = run(tableUri, indexName, query, null, sorts, size, List.of(fragment));
            assertEquals(size, response.hits().size());
            perFragment.add(ranked(response));
        }
        List<List<RankedHit>> order = new ArrayList<>(perFragment);
        for (int attempt = 0; attempt < 6; attempt++) {
            Collections.shuffle(order, random());
            List<SearchHit> merged = MergeReducer.mergeHits(order, sorts);
            assertEquals(whole, ids(merged.subList(0, size)));
        }
        // Two nodes, fragments 0 and 2 on one and fragment 1 on the other.
        LanceFragmentQueryResponse first = run(tableUri, indexName, query, null, sorts, size, List.of(0, 2));
        LanceFragmentQueryResponse second = run(tableUri, indexName, query, null, sorts, size, List.of(1));
        List<SearchHit> merged = MergeReducer.mergeHits(List.of(ranked(second), ranked(first)), sorts);
        assertEquals(whole, ids(merged.subList(0, size)));
    }

    private static List<RankedHit> ranked(LanceFragmentQueryResponse response) {
        List<RankedHit> ranked = new ArrayList<>();
        for (int i = 0; i < response.hits().size(); i++) {
            ranked.add(new RankedHit(response.hits().get(i), 0, response.rowAddrs()[i]));
        }
        return ranked;
    }

    /** Every row address is {@code fragment << 32 | offset} for the hit's {@code <fragment>-<offset>} id. */
    private static void assertRowAddressesMatchIds(LanceFragmentQueryResponse response) {
        assertEquals(response.hits().size(), response.rowAddrs().length);
        for (int i = 0; i < response.hits().size(); i++) {
            String id = response.hits().get(i).getId();
            int dash = id.indexOf('-');
            long fragment = Long.parseLong(id.substring(0, dash));
            long offset = Long.parseLong(id.substring(dash + 1));
            assertEquals(id, (fragment << 32) | offset, response.rowAddrs()[i]);
        }
    }

    private static List<String> ids(List<SearchHit> hits) {
        List<String> ids = new ArrayList<>(hits.size());
        for (SearchHit hit : hits) {
            ids.add(hit.getId());
        }
        return ids;
    }
}

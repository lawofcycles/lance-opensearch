/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link TransportLanceCoordinatorAction#mergeHits} over per-node hit
 * lists. Each inner list stands for one node's response: already sorted
 * by that node's executor, carrying the raw sort values Lucene's
 * {@code FieldDoc.fields} produced.
 */
public class CoordinatorHitMergeTests extends OpenSearchTestCase {

    public void testScoreOnlyMergesDescendingAcrossNodes() {
        List<SearchHit> nodeA = List.of(scored("a1", 3.0f), scored("a2", 1.0f));
        List<SearchHit> nodeB = List.of(scored("b1", 2.5f), scored("b2", 0.5f));

        List<SearchHit> merged = TransportLanceCoordinatorAction.mergeHits(List.of(nodeA, nodeB), Collections.emptyList());

        assertEquals(List.of("a1", "b1", "a2", "b2"), ids(merged));
    }

    public void testSingleFieldAscendingInterleavesNodes() {
        // Fragment round-robin puts ids 0,3,6 on one node and 1,4,7 on
        // the other, so a correct merge must interleave them.
        List<SearchHit> nodeA = List.of(sorted("0", 0L), sorted("3", 3L), sorted("6", 6L));
        List<SearchHit> nodeB = List.of(sorted("1", 1L), sorted("4", 4L), sorted("7", 7L));
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("id").order(SortOrder.ASC));

        List<SearchHit> merged = TransportLanceCoordinatorAction.mergeHits(List.of(nodeA, nodeB), sorts);

        assertEquals(List.of("0", "1", "3", "4", "6", "7"), ids(merged));
    }

    public void testTwoFieldsAscendingThenDescending() {
        // Primary key category asc (keyword, BytesRef), secondary id desc
        // (long). Ties on category must fall back to id descending.
        List<SearchHit> nodeA = List.of(sorted("a-even-2", new BytesRef("even"), 2L), sorted("a-odd-1", new BytesRef("odd"), 1L));
        List<SearchHit> nodeB = List.of(sorted("b-even-4", new BytesRef("even"), 4L), sorted("b-odd-3", new BytesRef("odd"), 3L));
        List<SortBuilder<?>> sorts = List.of(
            new FieldSortBuilder("category").order(SortOrder.ASC),
            new FieldSortBuilder("id").order(SortOrder.DESC)
        );

        List<SearchHit> merged = TransportLanceCoordinatorAction.mergeHits(List.of(nodeA, nodeB), sorts);

        assertEquals(List.of("b-even-4", "a-even-2", "b-odd-3", "a-odd-1"), ids(merged));
    }

    public void testNullSortValuesFollowMissingPlacement() {
        // Keyword sorts report a null raw value for a missing term. The
        // default and "_last" put the null at the end regardless of
        // direction; "_first" puts it at the front.
        List<SearchHit> nodeA = List.of(sorted("a-b", new BytesRef("b")), sorted("a-null", new Object[] { null }));
        List<SearchHit> nodeB = List.of(sorted("b-a", new BytesRef("a")), sorted("b-c", new BytesRef("c")));

        List<SearchHit> defaultAsc = TransportLanceCoordinatorAction.mergeHits(
            List.of(nodeA, nodeB),
            List.of(new FieldSortBuilder("category").order(SortOrder.ASC))
        );
        assertEquals(List.of("b-a", "a-b", "b-c", "a-null"), ids(defaultAsc));

        List<SearchHit> lastDesc = TransportLanceCoordinatorAction.mergeHits(
            List.of(nodeA, nodeB),
            List.of(new FieldSortBuilder("category").order(SortOrder.DESC).missing("_last"))
        );
        assertEquals(List.of("b-c", "a-b", "b-a", "a-null"), ids(lastDesc));

        List<SearchHit> firstAsc = TransportLanceCoordinatorAction.mergeHits(
            List.of(nodeA, nodeB),
            List.of(new FieldSortBuilder("category").order(SortOrder.ASC).missing("_first"))
        );
        assertEquals(List.of("a-null", "b-a", "a-b", "b-c"), ids(firstAsc));
    }

    public void testTiesKeepNodeOrderThenPerNodeOrder() {
        // Equal sort values on every hit: the result must be node A's
        // list followed by node B's list, each in its own order, and
        // the same on repeated calls.
        List<SearchHit> nodeA = List.of(sorted("a1", 5L), sorted("a2", 5L));
        List<SearchHit> nodeB = List.of(sorted("b1", 5L), sorted("b2", 5L));
        List<SearchHit> nodeC = List.of(sorted("c1", 5L));
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("id").order(SortOrder.DESC));

        for (int i = 0; i < 3; i++) {
            List<SearchHit> merged = TransportLanceCoordinatorAction.mergeHits(List.of(nodeA, nodeB, nodeC), sorts);
            assertEquals(List.of("a1", "a2", "b1", "b2", "c1"), ids(merged));
        }

        // Equal scores behave the same way on the score-only path.
        List<SearchHit> scoredA = List.of(scored("a1", 1.0f), scored("a2", 1.0f));
        List<SearchHit> scoredB = List.of(scored("b1", 1.0f));
        assertEquals(
            List.of("a1", "a2", "b1"),
            ids(TransportLanceCoordinatorAction.mergeHits(List.of(scoredA, scoredB), Collections.emptyList()))
        );
    }

    public void testScoreClauseInsideSortUsesRawSortValue() {
        // sort: [{"_score": "desc"}, {"id": "asc"}] with track_scores
        // off: SearchHit.getScore() is NaN and the score lives in the
        // raw sort values.
        SearchHit a1 = sorted("a1", 2.0f, 10L);
        SearchHit a2 = sorted("a2", 1.0f, 11L);
        SearchHit b1 = sorted("b1", 2.0f, 9L);
        SearchHit b2 = sorted("b2", 3.0f, 12L);
        List<SortBuilder<?>> sorts = List.of(new ScoreSortBuilder(), new FieldSortBuilder("id").order(SortOrder.ASC));

        List<SearchHit> merged = TransportLanceCoordinatorAction.mergeHits(List.of(List.of(a1, a2), List.of(b2, b1)), sorts);

        assertEquals(List.of("b2", "b1", "a1", "a2"), ids(merged));
    }

    public void testDocClauseFallsBackToNodeOrder() {
        // _doc ids are per-node Lucene ids and carry no meaning across
        // nodes, so a _doc sort yields node order then per-node order.
        List<SearchHit> nodeA = List.of(sorted("a1", 5), sorted("a2", 9));
        List<SearchHit> nodeB = List.of(sorted("b1", 0), sorted("b2", 1));
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder(FieldSortBuilder.DOC_FIELD_NAME));

        List<SearchHit> merged = TransportLanceCoordinatorAction.mergeHits(List.of(nodeA, nodeB), sorts);

        assertEquals(List.of("a1", "a2", "b1", "b2"), ids(merged));
    }

    public void testEmptyAndSingleNodeInputs() {
        assertTrue(TransportLanceCoordinatorAction.mergeHits(Collections.emptyList(), Collections.emptyList()).isEmpty());
        assertTrue(TransportLanceCoordinatorAction.mergeHits(List.of(Collections.emptyList()), Collections.emptyList()).isEmpty());
        List<SearchHit> only = List.of(sorted("x", 1L), sorted("y", 2L));
        assertEquals(List.of("x", "y"), ids(TransportLanceCoordinatorAction.mergeHits(List.of(only), List.of(new FieldSortBuilder("id")))));
    }

    private static SearchHit scored(String id, float score) {
        SearchHit hit = new SearchHit(0, id, Map.of(), Map.of());
        hit.score(score);
        return hit;
    }

    private static SearchHit sorted(String id, Object... rawSortValues) {
        SearchHit hit = new SearchHit(0, id, Map.of(), Map.of());
        hit.score(Float.NaN);
        DocValueFormat[] formats = new DocValueFormat[rawSortValues.length];
        for (int i = 0; i < formats.length; i++) {
            formats[i] = DocValueFormat.RAW;
        }
        hit.sortValues(rawSortValues, formats);
        return hit;
    }

    private static List<String> ids(List<SearchHit> hits) {
        List<String> ids = new ArrayList<>(hits.size());
        for (SearchHit hit : hits) {
            ids.add(hit.getId());
        }
        return ids;
    }
}

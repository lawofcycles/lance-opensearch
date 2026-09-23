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
import org.opensearch.lance.plan.execute.MergeReducer;
import org.opensearch.lance.plan.execute.MergeReducer.RankedHit;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link MergeReducer#mergeHits} over per-node hit
 * lists. Each inner list stands for one node's response: already sorted
 * by that node's executor, carrying the raw sort values Lucene's
 * {@code FieldDoc.fields} produced and the row address of every hit.
 * The helpers below give every hit a distinct row address unless a test
 * says otherwise, in the order the hits are declared.
 */
public class CoordinatorHitMergeTests extends OpenSearchTestCase {

    private static final int TARGET = 0;

    public void testScoreOnlyMergesDescendingAcrossNodes() {
        List<RankedHit> nodeA = List.of(scored("a1", 3.0f, 0L), scored("a2", 1.0f, 1L));
        List<RankedHit> nodeB = List.of(scored("b1", 2.5f, 2L), scored("b2", 0.5f, 3L));

        List<SearchHit> merged = MergeReducer.mergeHits(List.of(nodeA, nodeB), Collections.emptyList());

        assertEquals(List.of("a1", "b1", "a2", "b2"), ids(merged));
    }

    public void testSingleFieldAscendingInterleavesNodes() {
        // Fragment round-robin puts ids 0,3,6 on one node and 1,4,7 on
        // the other, so a correct merge must interleave them.
        List<RankedHit> nodeA = List.of(sorted("0", 0L, 0L), sorted("3", 3L, 3L), sorted("6", 6L, 6L));
        List<RankedHit> nodeB = List.of(sorted("1", 1L, 1L), sorted("4", 4L, 4L), sorted("7", 7L, 7L));
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("id").order(SortOrder.ASC));

        List<SearchHit> merged = MergeReducer.mergeHits(List.of(nodeA, nodeB), sorts);

        assertEquals(List.of("0", "1", "3", "4", "6", "7"), ids(merged));
    }

    public void testTwoFieldsAscendingThenDescending() {
        // Primary key category asc (keyword, BytesRef), secondary id desc
        // (long). Ties on category must fall back to id descending.
        List<RankedHit> nodeA = List.of(sorted("a-even-2", 0L, new BytesRef("even"), 2L), sorted("a-odd-1", 1L, new BytesRef("odd"), 1L));
        List<RankedHit> nodeB = List.of(sorted("b-even-4", 2L, new BytesRef("even"), 4L), sorted("b-odd-3", 3L, new BytesRef("odd"), 3L));
        List<SortBuilder<?>> sorts = List.of(
            new FieldSortBuilder("category").order(SortOrder.ASC),
            new FieldSortBuilder("id").order(SortOrder.DESC)
        );

        List<SearchHit> merged = MergeReducer.mergeHits(List.of(nodeA, nodeB), sorts);

        assertEquals(List.of("b-even-4", "a-even-2", "b-odd-3", "a-odd-1"), ids(merged));
    }

    public void testNullSortValuesFollowMissingPlacement() {
        // Keyword sorts report a null raw value for a missing term. The
        // default and "_last" put the null at the end regardless of
        // direction; "_first" puts it at the front.
        List<RankedHit> nodeA = List.of(sorted("a-b", 0L, new BytesRef("b")), sorted("a-null", 1L, new Object[] { null }));
        List<RankedHit> nodeB = List.of(sorted("b-a", 2L, new BytesRef("a")), sorted("b-c", 3L, new BytesRef("c")));

        List<SearchHit> defaultAsc = MergeReducer.mergeHits(
            List.of(nodeA, nodeB),
            List.of(new FieldSortBuilder("category").order(SortOrder.ASC))
        );
        assertEquals(List.of("b-a", "a-b", "b-c", "a-null"), ids(defaultAsc));

        List<SearchHit> lastDesc = MergeReducer.mergeHits(
            List.of(nodeA, nodeB),
            List.of(new FieldSortBuilder("category").order(SortOrder.DESC).missing("_last"))
        );
        assertEquals(List.of("b-c", "a-b", "b-a", "a-null"), ids(lastDesc));

        List<SearchHit> firstAsc = MergeReducer.mergeHits(
            List.of(nodeA, nodeB),
            List.of(new FieldSortBuilder("category").order(SortOrder.ASC).missing("_first"))
        );
        assertEquals(List.of("a-null", "b-a", "a-b", "b-c"), ids(firstAsc));
    }

    public void testTiesFollowRowAddressNotNodeOrder() {
        // Equal sort values on every hit. Fragment 0 (row addresses
        // 0 << 32 | n) is on node B and fragment 1 on node A, so a merge
        // that kept node order would list node A first; the row
        // address puts fragment 0 first whatever the node order.
        List<RankedHit> nodeA = List.of(sorted("1-0", rowAddr(1, 0), 5L), sorted("1-1", rowAddr(1, 1), 5L));
        List<RankedHit> nodeB = List.of(sorted("0-0", rowAddr(0, 0), 5L), sorted("0-1", rowAddr(0, 1), 5L));
        List<RankedHit> nodeC = List.of(sorted("2-0", rowAddr(2, 0), 5L));
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("id").order(SortOrder.DESC));

        List<String> expected = List.of("0-0", "0-1", "1-0", "1-1", "2-0");
        assertEquals(expected, ids(MergeReducer.mergeHits(List.of(nodeA, nodeB, nodeC), sorts)));
        assertEquals(expected, ids(MergeReducer.mergeHits(List.of(nodeC, nodeA, nodeB), sorts)));
        assertEquals(expected, ids(MergeReducer.mergeHits(List.of(nodeB, nodeC, nodeA), sorts)));

        // Equal scores behave the same way on the score-only path.
        List<RankedHit> scoredA = List.of(scored("1-0", 1.0f, rowAddr(1, 0)), scored("1-1", 1.0f, rowAddr(1, 1)));
        List<RankedHit> scoredB = List.of(scored("0-7", 1.0f, rowAddr(0, 7)));
        assertEquals(List.of("0-7", "1-0", "1-1"), ids(MergeReducer.mergeHits(List.of(scoredA, scoredB), Collections.emptyList())));
        assertEquals(List.of("0-7", "1-0", "1-1"), ids(MergeReducer.mergeHits(List.of(scoredB, scoredA), Collections.emptyList())));
    }

    public void testTiedTopPageIsTheSameForAnyFragmentToNodeAssignment() {
        // Twelve rows over three fragments of four, every row scored
        // 1.0, page size 4. Whichever way the fragments are dealt out
        // over one, two or three nodes, each executor returns the
        // first four rows of its fragments in row address order and
        // the merged page must be the first four rows of fragment 0,
        // which is what one executor over the whole table returns.
        List<RankedHit> all = new ArrayList<>();
        for (int fragment = 0; fragment < 3; fragment++) {
            for (int offset = 0; offset < 4; offset++) {
                all.add(scored(fragment + "-" + offset, 1.0f, rowAddr(fragment, offset)));
            }
        }
        List<String> expected = List.of("0-0", "0-1", "0-2", "0-3");
        List<List<Integer>> assignments = List.of(
            List.of(0, 1, 2),
            List.of(0, 1, 2, 0, 1, 2),
            List.of(1, 0, 2),
            List.of(2, 2, 0),
            List.of(0, 0, 0)
        );
        for (List<Integer> assignment : assignments) {
            List<List<RankedHit>> perNode = new ArrayList<>();
            int nodes = Collections.max(assignment) + 1;
            for (int node = 0; node < nodes; node++) {
                List<RankedHit> nodeHits = new ArrayList<>();
                for (int fragment = 0; fragment < 3; fragment++) {
                    if (assignment.get(fragment) == node) {
                        nodeHits.addAll(all.subList(fragment * 4, fragment * 4 + 4));
                    }
                }
                perNode.add(nodeHits.subList(0, Math.min(4, nodeHits.size())));
            }
            List<SearchHit> merged = MergeReducer.mergeHits(perNode, Collections.emptyList());
            assertEquals(assignment.toString(), expected, ids(merged.subList(0, 4)));
        }
    }

    public void testScoreClauseInsideSortUsesRawSortValue() {
        // sort: [{"_score": "desc"}, {"id": "asc"}] with track_scores
        // off: SearchHit.getScore() is NaN and the score lives in the
        // raw sort values.
        RankedHit a1 = sorted("a1", 0L, 2.0f, 10L);
        RankedHit a2 = sorted("a2", 1L, 1.0f, 11L);
        RankedHit b1 = sorted("b1", 2L, 2.0f, 9L);
        RankedHit b2 = sorted("b2", 3L, 3.0f, 12L);
        List<SortBuilder<?>> sorts = List.of(new ScoreSortBuilder(), new FieldSortBuilder("id").order(SortOrder.ASC));

        List<SearchHit> merged = MergeReducer.mergeHits(List.of(List.of(a1, a2), List.of(b2, b1)), sorts);

        assertEquals(List.of("b2", "b1", "a1", "a2"), ids(merged));
    }

    public void testDocClauseOrdersByRowAddress() {
        // The _doc sort values are per-node Lucene doc ids (0 and 1 on
        // both nodes here) and carry no meaning across nodes; the
        // clause orders by row address, the doc id order one reader
        // over the whole table would have.
        List<RankedHit> nodeA = List.of(sorted("1-0", rowAddr(1, 0), 0), sorted("1-1", rowAddr(1, 1), 1));
        List<RankedHit> nodeB = List.of(sorted("0-0", rowAddr(0, 0), 0), sorted("0-1", rowAddr(0, 1), 1));
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder(FieldSortBuilder.DOC_FIELD_NAME));

        List<SearchHit> merged = MergeReducer.mergeHits(List.of(nodeA, nodeB), sorts);

        assertEquals(List.of("0-0", "0-1", "1-0", "1-1"), ids(merged));

        List<SortBuilder<?>> descending = List.of(new FieldSortBuilder(FieldSortBuilder.DOC_FIELD_NAME).order(SortOrder.DESC));
        assertEquals(List.of("1-1", "1-0", "0-1", "0-0"), ids(MergeReducer.mergeHits(List.of(nodeA, nodeB), descending)));
    }

    public void testTiesAcrossIndexesFollowTargetOrder() {
        // Two indexes in one request can hold the same row address.
        // The index that comes first in the request wins the tie, in
        // either arrival order.
        RankedHit first = new RankedHit(hit("first", 1.0f), 0, rowAddr(0, 0));
        RankedHit second = new RankedHit(hit("second", 1.0f), 1, rowAddr(0, 0));
        assertEquals(
            List.of("first", "second"),
            ids(MergeReducer.mergeHits(List.of(List.of(second), List.of(first)), Collections.emptyList()))
        );
        assertEquals(
            List.of("first", "second"),
            ids(MergeReducer.mergeHits(List.of(List.of(first), List.of(second)), Collections.emptyList()))
        );
    }

    public void testUnrelatedSortValueTypesAreRefused() {
        // A Long against a BytesRef in the same clause has no defined
        // order; refusing beats a silently scrambled page.
        List<RankedHit> nodeA = List.of(sorted("a1", 0L, 1L));
        List<RankedHit> nodeB = List.of(sorted("b1", 1L, new BytesRef("x")));
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("id"));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> MergeReducer.mergeHits(List.of(nodeA, nodeB), sorts));
        assertTrue(e.getMessage(), e.getMessage().contains("java.lang.Long") && e.getMessage().contains("BytesRef"));

        // Mixed numeric widths are still compared by value.
        assertEquals(
            List.of("i", "l"),
            ids(MergeReducer.mergeHits(List.of(List.of(sorted("l", 0L, 5L)), List.of(sorted("i", 1L, 3))), sorts))
        );
    }

    public void testEmptyAndSingleNodeInputs() {
        assertTrue(MergeReducer.mergeHits(Collections.emptyList(), Collections.emptyList()).isEmpty());
        assertTrue(MergeReducer.mergeHits(List.of(Collections.emptyList()), Collections.emptyList()).isEmpty());
        List<RankedHit> only = List.of(sorted("x", 0L, 1L), sorted("y", 1L, 2L));
        assertEquals(List.of("x", "y"), ids(MergeReducer.mergeHits(List.of(only), List.of(new FieldSortBuilder("id")))));
    }

    private static long rowAddr(int fragment, int offset) {
        return ((long) fragment << 32) | offset;
    }

    private static SearchHit hit(String id, float score) {
        SearchHit hit = new SearchHit(0, id, Map.of(), Map.of());
        hit.score(score);
        return hit;
    }

    private static RankedHit scored(String id, float score, long rowAddr) {
        return new RankedHit(hit(id, score), TARGET, rowAddr);
    }

    private static RankedHit sorted(String id, long rowAddr, Object... rawSortValues) {
        SearchHit hit = new SearchHit(0, id, Map.of(), Map.of());
        hit.score(Float.NaN);
        DocValueFormat[] formats = new DocValueFormat[rawSortValues.length];
        for (int i = 0; i < formats.length; i++) {
            formats[i] = DocValueFormat.RAW;
        }
        hit.sortValues(rawSortValues, formats);
        return new RankedHit(hit, TARGET, rowAddr);
    }

    private static List<String> ids(List<SearchHit> hits) {
        List<String> ids = new ArrayList<>(hits.size());
        for (SearchHit hit : hits) {
            ids.add(hit.getId());
        }
        return ids;
    }
}

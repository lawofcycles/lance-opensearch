/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import org.apache.calcite.plan.Convention;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFts;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceMultiMatchQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The pushed full text match names whether each searched column is
 * answered from an inverted index: the word is read off the
 * {@code LanceTable} the scan was built over, rendered as one
 * {@code index=} term in the plan text and summarised as one word for
 * the explain endpoint's {@code fts_index}, and left out when the model
 * does not know the table's indexed columns.
 */
public class PushedOperationTests extends OpenSearchTestCase {

    /** A bare scan over {@link PlanTestFixtures#SCHEMA} whose table knows {@code indexedColumns}, or nothing when null. */
    private static LanceTableScan scan(Set<String> indexedColumns) {
        LanceSchemas.IndexModel model = LanceSchemas.model(
            "idx",
            PlanTestFixtures.SCHEMA,
            Map.of(),
            Map.of(),
            "",
            Set.of(),
            () -> 512L,
            indexedColumns
        );
        return (LanceTableScan) PlanTestFixtures.factory().relBuilder(model.schema()).scan(LancePlannerFactory.SCHEMA_NAME, "idx").build();
    }

    private static LanceFtsMatch match(LanceTableScan scan) {
        return new LanceFtsMatch(
            scan.getCluster(),
            scan.getCluster().traitSetOf(Convention.NONE),
            scan,
            LanceFtsMatch.Kind.MATCH,
            List.of("body"),
            new LanceMatchQueryBuilder("body", "hello")
        );
    }

    private static LanceFtsMatch multiMatch(LanceTableScan scan) {
        return new LanceFtsMatch(
            scan.getCluster(),
            scan.getCluster().traitSetOf(Convention.NONE),
            scan,
            LanceFtsMatch.Kind.MULTI_MATCH,
            List.of("body", "category"),
            new LanceMultiMatchQueryBuilder(List.of("body", "category"), "hello")
        );
    }

    public void testIndexedColumnRendersInverted() {
        LanceTableScan scan = scan(Set.of("body"));
        PushedFts pushed = scan.withPushedFts(match(scan), null).pushedFts().orElseThrow();
        assertTrue(pushed.indexKnown());
        assertTrue(pushed.indexed("body"));
        assertEquals(PushedFts.INDEX_INVERTED, pushed.indexSummary());
        assertTrue(pushed.toString(), pushed.toString().startsWith("fts{kind=MATCH, columns=[body], index=inverted, query="));
    }

    public void testUnindexedColumnRendersNone() {
        LanceTableScan scan = scan(Set.of());
        PushedFts pushed = scan.withPushedFts(match(scan), "rating >= 3").pushedFts().orElseThrow();
        assertTrue(pushed.indexKnown());
        assertFalse(pushed.indexed("body"));
        assertEquals(PushedFts.INDEX_NONE, pushed.indexSummary());
        String text = pushed.toString();
        assertTrue(text, text.startsWith("fts{kind=MATCH, columns=[body], index=none, query="));
        assertTrue(text, text.endsWith(", filter=rating >= 3}"));
    }

    public void testMixedColumnsRenderEachColumnsWord() {
        LanceTableScan scan = scan(Set.of("body"));
        PushedFts pushed = scan.withPushedFts(multiMatch(scan), null).pushedFts().orElseThrow();
        assertEquals(PushedFts.INDEX_MIXED, pushed.indexSummary());
        assertTrue(
            pushed.toString(),
            pushed.toString().startsWith("fts{kind=MULTI_MATCH, columns=[body, category], index=[body=inverted, category=none], query=")
        );
    }

    public void testUnknownIndexRendersNoTerm() {
        LanceTableScan scan = scan(null);
        PushedFts pushed = scan.withPushedFts(match(scan), null).pushedFts().orElseThrow();
        assertFalse(pushed.indexKnown());
        assertFalse(pushed.indexed("body"));
        assertNull(pushed.indexSummary());
        assertTrue(pushed.toString(), pushed.toString().startsWith("fts{kind=MATCH, columns=[body], query="));
        assertEquals("the two argument form stands for an unknown index", pushed, new PushedFts(pushed.fts(), null));
    }

    public void testIndexIsPartOfTheDigest() {
        LanceTableScan indexed = scan(Set.of("body"));
        LanceTableScan flat = scan(Set.of());
        String indexedDigest = indexed.withPushedFts(match(indexed), null).getDigest();
        String flatDigest = flat.withPushedFts(match(flat), null).getDigest();
        assertTrue(indexedDigest, indexedDigest.contains("index=inverted"));
        assertTrue(flatDigest, flatDigest.contains("index=none"));
    }
}

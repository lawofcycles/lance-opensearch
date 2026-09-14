/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.apache.lucene.search.QueryVisitor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link LanceKnnQuery}. The Lance-native scan path inside
 * {@code createWeight} needs a live Dataset and cannot be exercised here,
 * but the surrounding Lucene {@code Query} contract (equals, hashCode,
 * toString, visitor) is pure Java and worth locking in.
 */
public class LanceKnnQueryTests extends OpenSearchTestCase {

    private static final String COLUMN = "embedding";
    private static final float[] VECTOR = new float[] { 0.1f, 0.2f, 0.3f, 0.4f };
    private static final int K = 5;

    public void testEqualsAndHashCodeAcrossEveryDimension() {
        LanceKnnQuery a = new LanceKnnQuery(COLUMN, VECTOR, K);
        LanceKnnQuery b = new LanceKnnQuery(COLUMN, new float[] { 0.1f, 0.2f, 0.3f, 0.4f }, K);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        // Column differs
        assertNotEquals(a, new LanceKnnQuery("other", VECTOR, K));
        // Vector differs
        assertNotEquals(a, new LanceKnnQuery(COLUMN, new float[] { 1.0f, 2.0f, 3.0f, 4.0f }, K));
        // k differs
        assertNotEquals(a, new LanceKnnQuery(COLUMN, VECTOR, K + 1));
        // Vector length differs
        assertNotEquals(a, new LanceKnnQuery(COLUMN, new float[] { 0.1f, 0.2f }, K));
    }

    public void testToStringShowsColumnAndK() {
        String text = new LanceKnnQuery(COLUMN, VECTOR, K).toString(COLUMN);
        assertTrue("toString should mention the column, saw: " + text, text.contains(COLUMN));
        assertTrue("toString should mention k, saw: " + text, text.contains("k=" + K));
    }

    public void testVisitorReceivesLeaf() {
        LanceKnnQuery query = new LanceKnnQuery(COLUMN, VECTOR, K);
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

    public void testFragmentHitsGrowGeometricallyFromEightEntries() {
        LanceKnnQuery.FragmentHits hits = new LanceKnnQuery.FragmentHits();
        // Locked in at eight entries per the initial capacity so a per-shard
        // scan that returns at most k rows per fragment does not reallocate
        // for a typical top-10 query.
        assertEquals(8, hits.offsets.length);
        assertEquals(8, hits.distances.length);
        for (int i = 0; i < 8; i++) {
            hits.add(i, (float) i);
        }
        assertEquals(8, hits.size);
        // Ninth add forces a geometric grow.
        hits.add(8, 8.0f);
        assertEquals(9, hits.size);
        assertEquals(16, hits.offsets.length);
        assertEquals(16, hits.distances.length);
        assertEquals(8, hits.offsets[8]);
        assertEquals(8.0f, hits.distances[8], 0.0f);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.apache.lucene.search.QueryVisitor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;

public class LanceFtsQueryTests extends OpenSearchTestCase {

    public void testEqualsAndHashCode() {
        LanceFtsQuery a = new LanceFtsQuery("body", "camera");
        LanceFtsQuery b = new LanceFtsQuery("body", "camera");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        assertNotEquals(a, new LanceFtsQuery("title", "camera"));
        assertNotEquals(a, new LanceFtsQuery("body", "phone"));
    }

    public void testToStringIsHumanReadable() {
        LanceFtsQuery query = new LanceFtsQuery("body", "camera");
        String text = query.toString("body");
        assertTrue("toString should mention the column, saw: " + text, text.contains("body"));
        assertTrue("toString should mention the query text, saw: " + text, text.contains("camera"));
    }

    public void testVisitorReceivesLeaf() {
        LanceFtsQuery query = new LanceFtsQuery("body", "camera");
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
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.search.QueryVisitor;
import org.opensearch.test.OpenSearchTestCase;
import org.lance.ipc.FullTextQuery;

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

    public void testPhraseQueryDiffersFromMatchQuery() {
        // phrase / non-phrase are distinct queries even when column and
        // text agree, because Lance routes them to different FullTextQuery
        // constructors. Making equals reflect that keeps query cache keys
        // and _explain output honest.
        LanceFtsQuery match = new LanceFtsQuery("body", "quick brown");
        LanceFtsQuery phrase = new LanceFtsQuery("body", "quick brown", true, 0);
        assertNotEquals(match, phrase);
        assertNotEquals(match.hashCode(), phrase.hashCode());
        String phraseText = phrase.toString("body");
        assertTrue("phrase toString should mention phrase, saw: " + phraseText, phraseText.contains("phrase"));
    }

    public void testPhraseSlopParticipatesInEqualsAndHashCode() {
        LanceFtsQuery slop0 = new LanceFtsQuery("body", "quick brown", true, 0);
        LanceFtsQuery slop2 = new LanceFtsQuery("body", "quick brown", true, 2);
        assertNotEquals(slop0, slop2);
        // Slop is clamped to a non-negative int; negative values collapse to 0.
        LanceFtsQuery slopNegative = new LanceFtsQuery("body", "quick brown", true, -5);
        assertEquals(slop0, slopNegative);
    }

    public void testOperatorParticipatesInEqualsAndHashCode() {
        LanceFtsQuery orQuery = new LanceFtsQuery("body", "quick brown");
        LanceFtsQuery andQuery = new LanceFtsQuery("body", "quick brown", FullTextQuery.Operator.AND);
        assertNotEquals(orQuery, andQuery);
        assertNotEquals(orQuery.hashCode(), andQuery.hashCode());
        String andText = andQuery.toString("body");
        assertTrue("AND toString should mention the operator, saw: " + andText, andText.contains("and"));
    }

    public void testNullOperatorFallsBackToOr() {
        // The constructor tolerates a null operator so callers do not have
        // to guard when threading through code paths where OR is the
        // default. Equality with the explicit-OR form documents that.
        LanceFtsQuery a = new LanceFtsQuery("body", "camera", false, 0, null);
        LanceFtsQuery b = new LanceFtsQuery("body", "camera", false, 0, FullTextQuery.Operator.OR);
        assertEquals(a, b);
    }
}

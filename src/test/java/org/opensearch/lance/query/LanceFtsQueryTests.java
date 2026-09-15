/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.List;
import java.util.Optional;
import java.util.Set;
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
        assertTrue("AND toString should mention the operator, saw: " + andText, andText.contains("AND"));
    }

    public void testNullOperatorFallsBackToOr() {
        // The constructor tolerates a null operator so callers do not have
        // to guard when threading through code paths where OR is the
        // default. Equality with the explicit-OR form documents that.
        LanceFtsQuery a = new LanceFtsQuery("body", "camera", false, 0, null);
        LanceFtsQuery b = new LanceFtsQuery("body", "camera", false, 0, FullTextQuery.Operator.OR);
        assertEquals(a, b);
    }

    public void testDirectFullTextQueryConstructorAndColumnsCollection() {
        // A LanceFtsQuery built from a MatchQuery with fuzziness must not
        // compare equal to one without it, because the parameter change
        // reaches Lance and can change the result set. Documenting this
        // through equality keeps _explain output honest for callers who
        // rely on canonical DSL representations.
        FullTextQuery plain = FullTextQuery.match("hello", "body");
        FullTextQuery fuzzy = FullTextQuery.match("hello", "body", 1f, Optional.of(1), 50, FullTextQuery.Operator.OR, 0);
        LanceFtsQuery plainQuery = new LanceFtsQuery(plain, Set.of("body"));
        LanceFtsQuery fuzzyQuery = new LanceFtsQuery(fuzzy, Set.of("body"));
        assertNotEquals(plainQuery, fuzzyQuery);
    }

    public void testCollectColumnsWalksBooleanTree() {
        FullTextQuery bodyMatch = FullTextQuery.match("hello", "body");
        FullTextQuery titleMatch = FullTextQuery.match("world", "title");
        FullTextQuery combined = FullTextQuery.booleanQuery(
            List.of(
                new FullTextQuery.BooleanClause(FullTextQuery.Occur.MUST, bodyMatch),
                new FullTextQuery.BooleanClause(FullTextQuery.Occur.SHOULD, titleMatch)
            )
        );
        Set<String> columns = LanceFtsQuery.collectColumns(combined);
        assertEquals(Set.of("body", "title"), columns);
    }

    public void testCollectColumnsWalksBoostTree() {
        FullTextQuery pos = FullTextQuery.match("hello", "body");
        FullTextQuery neg = FullTextQuery.match("stale", "title");
        FullTextQuery boosted = FullTextQuery.boost(pos, neg, 0.5f);
        Set<String> columns = LanceFtsQuery.collectColumns(boosted);
        assertEquals(Set.of("body", "title"), columns);
    }
}

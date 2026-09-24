/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.lance.ipc.FullTextQuery;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.lance.query.LanceMatchPhraseQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceMultiMatchQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * Pins what {@link StockTextQueryRewriter} turns each stock full text
 * clause into, and which clauses it leaves alone, by comparing against
 * the {@code lance_*} builder a caller would have written.
 */
public class StockTextQueryRewriterTests extends OpenSearchTestCase {

    private static final Set<String> TEXT_FIELDS = Set.of("body", "title");

    private static QueryBuilder rewrite(QueryBuilder query) {
        return StockTextQueryRewriter.rewrite(query, TEXT_FIELDS);
    }

    public void testMatchBecomesLanceMatch() {
        assertEquals(new LanceMatchQueryBuilder("body", "hello lance"), rewrite(QueryBuilders.matchQuery("body", "hello lance")));
    }

    public void testMatchCarriesOperatorPrefixLengthMaxExpansionsBoostAndName() {
        MatchQueryBuilder match = QueryBuilders.matchQuery("body", "hello lance")
            .operator(Operator.AND)
            .prefixLength(2)
            .maxExpansions(20)
            .boost(1.5f)
            .queryName("hit");
        LanceMatchQueryBuilder expected = new LanceMatchQueryBuilder("body", "hello lance").operator(FullTextQuery.Operator.AND)
            .prefixLength(2)
            .maxExpansions(20);
        expected.boost(1.5f).queryName("hit");
        assertEquals(expected, rewrite(match));
    }

    public void testMatchFuzzinessBecomesTheEditDistance() {
        assertEquals(
            new LanceMatchQueryBuilder("body", "helo").fuzziness(1),
            rewrite(QueryBuilders.matchQuery("body", "helo").fuzziness(Fuzziness.ONE))
        );
        // AUTO resolves on the whole text: "helo" has four characters, one edit.
        assertEquals(
            new LanceMatchQueryBuilder("body", "helo").fuzziness(1),
            rewrite(QueryBuilders.matchQuery("body", "helo").fuzziness(Fuzziness.AUTO))
        );
        assertEquals(
            new LanceMatchQueryBuilder("body", "hello lance").fuzziness(2),
            rewrite(QueryBuilders.matchQuery("body", "hello lance").fuzziness(Fuzziness.AUTO))
        );
    }

    public void testMatchOnAFieldThatIsNotLanceTextIsLeftAlone() {
        MatchQueryBuilder match = QueryBuilders.matchQuery("category", "c0");
        assertSame(match, rewrite(match));
    }

    public void testMatchWithAnExplicitAnalyzerIsLeftAlone() {
        MatchQueryBuilder match = QueryBuilders.matchQuery("body", "hello").analyzer("whitespace");
        assertSame(match, rewrite(match));
    }

    public void testMatchWithAnEmptyTextIsLeftAlone() {
        MatchQueryBuilder match = QueryBuilders.matchQuery("body", "");
        assertSame(match, rewrite(match));
    }

    public void testMatchPhraseBecomesLanceMatchPhraseWithSlop() {
        MatchPhraseQueryBuilder phrase = QueryBuilders.matchPhraseQuery("body", "quick fox").slop(1).boost(2f).queryName("p");
        LanceMatchPhraseQueryBuilder expected = new LanceMatchPhraseQueryBuilder("body", "quick fox").slop(1);
        expected.boost(2f).queryName("p");
        assertEquals(expected, rewrite(phrase));
    }

    public void testMultiMatchBestFieldsBecomesLanceMultiMatchWithBoosts() {
        MultiMatchQueryBuilder multi = QueryBuilders.multiMatchQuery("hello cloudy")
            .field("body")
            .field("title", 2f)
            .operator(Operator.AND);
        LanceMultiMatchQueryBuilder expected = new LanceMultiMatchQueryBuilder(List.of("body", "title"), "hello cloudy").operator(
            FullTextQuery.Operator.AND
        ).boosts(List.of(1f, 2f));
        assertEquals(expected, rewrite(multi));
    }

    public void testMultiMatchWithoutBoostsOmitsTheBoostList() {
        LanceMultiMatchQueryBuilder rewritten = (LanceMultiMatchQueryBuilder) rewrite(
            QueryBuilders.multiMatchQuery("hello", "body", "title")
        );
        assertEquals(new LanceMultiMatchQueryBuilder(List.of("body", "title"), "hello"), rewritten);
    }

    public void testMultiMatchOfAnotherTypeOrWithFuzzinessOrTieBreakerIsLeftAlone() {
        MultiMatchQueryBuilder mostFields = QueryBuilders.multiMatchQuery("hello", "body", "title")
            .type(MultiMatchQueryBuilder.Type.MOST_FIELDS);
        assertSame(mostFields, rewrite(mostFields));
        MultiMatchQueryBuilder phrase = QueryBuilders.multiMatchQuery("hello", "body").type(MultiMatchQueryBuilder.Type.PHRASE);
        assertSame(phrase, rewrite(phrase));
        MultiMatchQueryBuilder fuzzy = QueryBuilders.multiMatchQuery("hello", "body", "title").fuzziness(Fuzziness.ONE);
        assertSame(fuzzy, rewrite(fuzzy));
        MultiMatchQueryBuilder tie = QueryBuilders.multiMatchQuery("hello", "body", "title").tieBreaker(0.3f);
        assertSame(tie, rewrite(tie));
    }

    public void testMultiMatchNamingAFieldOutsideLanceTextIsLeftAlone() {
        MultiMatchQueryBuilder mixed = QueryBuilders.multiMatchQuery("hello", "body", "category");
        assertSame(mixed, rewrite(mixed));
        MultiMatchQueryBuilder pattern = QueryBuilders.multiMatchQuery("hello", "bo*");
        assertSame(pattern, rewrite(pattern));
    }

    public void testBoolIsRebuiltAroundRewrittenClausesKeepingEveryOtherParameter() {
        BoolQueryBuilder bool = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("body", "hello"))
            .should(QueryBuilders.matchQuery("title", "sunny"))
            .mustNot(QueryBuilders.matchPhraseQuery("body", "quick fox"))
            .filter(QueryBuilders.termQuery("id", 4))
            .minimumShouldMatch("1")
            .adjustPureNegative(false)
            .boost(0.5f)
            .queryName("b");
        BoolQueryBuilder expected = QueryBuilders.boolQuery()
            .must(new LanceMatchQueryBuilder("body", "hello"))
            .should(new LanceMatchQueryBuilder("title", "sunny"))
            .mustNot(new LanceMatchPhraseQueryBuilder("body", "quick fox"))
            .filter(QueryBuilders.termQuery("id", 4))
            .minimumShouldMatch("1")
            .adjustPureNegative(false)
            .boost(0.5f)
            .queryName("b");
        assertEquals(expected, rewrite(bool));
    }

    public void testBoolWithoutAStockTextClauseIsTheSameInstance() {
        BoolQueryBuilder bool = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("id", 4)).filter(QueryBuilders.existsQuery("body"));
        assertSame(bool, rewrite(bool));
    }

    public void testNestedBoolsAreRewrittenThrough() {
        BoolQueryBuilder inner = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("body", "hello"));
        BoolQueryBuilder outer = QueryBuilders.boolQuery().must(inner).filter(QueryBuilders.termQuery("id", 1));
        BoolQueryBuilder expected = QueryBuilders.boolQuery()
            .must(QueryBuilders.boolQuery().should(new LanceMatchQueryBuilder("body", "hello")))
            .filter(QueryBuilders.termQuery("id", 1));
        assertEquals(expected, rewrite(outer));
    }

    public void testDisMaxIsRebuiltAroundRewrittenClauses() {
        DisMaxQueryBuilder disMax = QueryBuilders.disMaxQuery()
            .add(QueryBuilders.matchQuery("body", "hello"))
            .add(QueryBuilders.termQuery("id", 1))
            .tieBreaker(0.2f)
            .boost(3f);
        DisMaxQueryBuilder expected = QueryBuilders.disMaxQuery()
            .add(new LanceMatchQueryBuilder("body", "hello"))
            .add(QueryBuilders.termQuery("id", 1))
            .tieBreaker(0.2f)
            .boost(3f);
        assertEquals(expected, rewrite(disMax));
    }

    public void testOtherCompoundsAndScalarQueriesAreLeftAlone() {
        QueryBuilder constantScore = QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("body", "hello"));
        assertSame(constantScore, rewrite(constantScore));
        QueryBuilder term = QueryBuilders.termQuery("body", "hello");
        assertSame(term, rewrite(term));
        assertNull(rewrite(null));
    }

    public void testNoLanceTextFieldsMeansNoRewrite() {
        MatchQueryBuilder match = QueryBuilders.matchQuery("body", "hello");
        assertSame(match, StockTextQueryRewriter.rewrite(match, Set.of()));
    }
}

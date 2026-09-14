/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.List;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchTestCase;

public class LanceKnnFilterTranslatorTests extends OpenSearchTestCase {

    public void testMatchAllTranslatesToTrue() {
        assertEquals("true", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.matchAllQuery()));
    }

    public void testTermNumeric() {
        assertEquals("rating = 5", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termQuery("rating", 5)));
    }

    public void testTermString() {
        assertEquals("body = 'hello'", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termQuery("body", "hello")));
    }

    public void testTermBoolean() {
        assertEquals("flag = true", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termQuery("flag", true)));
    }

    public void testTermStringEscapesSingleQuote() {
        assertEquals("name = 'O''Brien'", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termQuery("name", "O'Brien")));
    }

    public void testTermsNumericIn() {
        assertEquals("rating IN (3, 4, 5)", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termsQuery("rating", List.of(3, 4, 5))));
    }

    public void testEmptyTermsCollapsesToFalse() {
        assertEquals("false", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termsQuery("rating", java.util.List.of())));
    }

    public void testExistsIsNotNull() {
        assertEquals("rating IS NOT NULL", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.existsQuery("rating")));
    }

    public void testRangeGteLt() {
        assertEquals(
            "(rating >= 4 AND rating < 10)",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.rangeQuery("rating").gte(4).lt(10))
        );
    }

    public void testRangeGteOnly() {
        assertEquals("(rating >= 4)", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.rangeQuery("rating").gte(4)));
    }

    public void testRangeLtOnly() {
        assertEquals("(rating < 10)", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.rangeQuery("rating").lt(10)));
    }

    public void testRangeWithoutBoundsRejected() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceKnnFilterTranslator.toLanceSql(QueryBuilders.rangeQuery("rating"))
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("rating"));
    }

    public void testBoolFilterMustCombined() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .filter(QueryBuilders.rangeQuery("rating").gte(4))
            .must(QueryBuilders.termQuery("flag", true));
        assertEquals("((rating >= 4) AND flag = true)", LanceKnnFilterTranslator.toLanceSql(b));
    }

    public void testBoolMustNotNegates() {
        BoolQueryBuilder b = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("flag", false));
        assertEquals("(NOT (flag = false))", LanceKnnFilterTranslator.toLanceSql(b));
    }

    public void testBoolShouldGoesInsideOr() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("rating", 3))
            .should(QueryBuilders.termQuery("rating", 5));
        assertEquals("((rating = 3 OR rating = 5))", LanceKnnFilterTranslator.toLanceSql(b));
    }

    public void testBoolMustPlusShould() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("rating").gte(4))
            .should(QueryBuilders.termQuery("flag", true))
            .should(QueryBuilders.termQuery("flag", false));
        assertEquals("((rating >= 4) AND (flag = true OR flag = false))", LanceKnnFilterTranslator.toLanceSql(b));
    }

    public void testEmptyBoolTranslatesToTrue() {
        assertEquals("true", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.boolQuery()));
    }

    public void testUnsupportedBuilderRejected() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceKnnFilterTranslator.toLanceSql(QueryBuilders.matchQuery("body", "hello"))
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("MatchQueryBuilder"));
    }

    public void testNullValueRejected() {
        // OpenSearch's TermQueryBuilder rejects null values before they
        // reach the translator ("value cannot be null"), so we exercise
        // the translator's own defensive branch by feeding it a
        // TermsQueryBuilder whose value list contains a null element.
        java.util.List<Object> values = new java.util.ArrayList<>();
        values.add("a");
        values.add(null);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termsQuery("tag", values))
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("must not be null"));
    }

    public void testNestedBool() {
        BoolQueryBuilder inner = QueryBuilders.boolQuery().should(QueryBuilders.termQuery("a", 1)).should(QueryBuilders.termQuery("a", 2));
        BoolQueryBuilder outer = QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("rating").gte(4)).filter(inner);
        assertEquals("((rating >= 4) AND ((a = 1 OR a = 2)))", LanceKnnFilterTranslator.toLanceSql(outer));
    }

    public void testTermsListValue() {
        assertEquals("tag IN ('a', 'b')", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termsQuery("tag", List.of("a", "b"))));
    }
}

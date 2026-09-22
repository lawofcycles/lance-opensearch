/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.List;
import java.util.function.Function;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RegexpFlag;
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

    public void testWildcardTranslatesToLike() {
        assertEquals(
            "body LIKE '%w0001%' ESCAPE '\\'",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.wildcardQuery("body", "*w0001*"))
        );
        assertEquals(
            "body ILIKE 'w_001' ESCAPE '\\'",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.wildcardQuery("body", "w?001").caseInsensitive(true))
        );
    }

    public void testWildcardSqlAgreesWithTheFieldType() {
        // The coordinator's SQL and the field type's LanceScanFilterQuery
        // must express the same predicate, or hits.total (countRows on
        // the SQL) and the hits (the query) could disagree.
        String sql = LanceKnnFilterTranslator.toLanceSql(QueryBuilders.wildcardQuery("body", "50%_*O'B\\*"));
        assertEquals(LanceStringPatternSql.wildcard("body", "50%_*O'B\\*", false), sql);
        assertEquals("body LIKE '50\\%\\_%O''B*' ESCAPE '\\'", sql);
    }

    public void testRegexpTranslatesToAnchoredRegexpLike() {
        assertEquals(
            "regexp_like(body, '^(?:w0001.*)$')",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.regexpQuery("body", "w0001.*"))
        );
        assertEquals(
            "regexp_like(body, '(?i)^(?:w0001.*)$')",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.regexpQuery("body", "w0001.*").caseInsensitive(true))
        );
    }

    public void testRegexpWithLuceneOnlyOperatorRejected() {
        // The default flags (ALL) enable intersection, so & is refused;
        // the deprecated complement is off by default, so ~ passes as a
        // literal and is refused only when the COMPLEMENT flag is set.
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceKnnFilterTranslator.toLanceSql(QueryBuilders.regexpQuery("body", "a&b"))
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("Rust regex"));
        assertEquals("regexp_like(body, '^(?:~a)$')", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.regexpQuery("body", "~a")));
        Exception complement = expectThrows(
            IllegalArgumentException.class,
            () -> LanceKnnFilterTranslator.toLanceSql(QueryBuilders.regexpQuery("body", "~a").flags(RegexpFlag.COMPLEMENT))
        );
        assertTrue("unexpected message: " + complement.getMessage(), complement.getMessage().contains("complement"));
        assertEquals(
            "regexp_like(body, '^(?:a&b)$')",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.regexpQuery("body", "a&b").flags(RegexpFlag.NONE))
        );
    }

    public void testPrefixTranslatesToStartsWith() {
        assertEquals("starts_with(body, 'hel')", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.prefixQuery("body", "hel")));
        assertEquals(
            "starts_with(lower(body), lower('HEL'))",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.prefixQuery("body", "HEL").caseInsensitive(true))
        );
    }

    public void testPatternQueriesComposeInsideBool() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .filter(QueryBuilders.wildcardQuery("body", "*w0001*"))
            .filter(QueryBuilders.termQuery("rating", 5))
            .mustNot(QueryBuilders.prefixQuery("category", "cat1"));
        assertEquals(
            "(body LIKE '%w0001%' ESCAPE '\\' AND rating = 5 AND NOT (starts_with(category, 'cat1')))",
            LanceKnnFilterTranslator.toLanceSql(b)
        );
    }

    public void testPatternQueriesRejectMultiFieldPath() {
        for (QueryBuilder q : List.of(
            QueryBuilders.wildcardQuery("body.raw", "h*"),
            QueryBuilders.regexpQuery("body.raw", "h.*"),
            QueryBuilders.prefixQuery("body.raw", "h")
        )) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> LanceKnnFilterTranslator.toLanceSql(q));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("dotted field"));
        }
    }

    public void testStructChildPathsPrintAsLanceNestedFieldAccess() {
        // A dotted name the lookup resolves is a struct child mapped
        // through object properties; Lance's SQL parser reads the
        // dotted path as a nested field access, so the translator
        // prints it verbatim.
        Function<String, String> lookup = name -> switch (name) {
            case "meta.region" -> "keyword";
            case "meta.score" -> "double";
            case "meta.flags.active" -> "boolean";
            default -> null;
        };
        assertEquals("meta.region = 'east'", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termQuery("meta.region", "east"), lookup));
        assertEquals(
            "meta.region IN ('east', 'west')",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termsQuery("meta.region", List.of("east", "west")), lookup)
        );
        assertEquals("(meta.score >= 3.0)", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.rangeQuery("meta.score").gte(3.0), lookup));
        assertEquals(
            "meta.flags.active IS NOT NULL",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.existsQuery("meta.flags.active"), lookup)
        );
        // A dotted name the lookup does not resolve (a multi-field
        // sub-field, or no mapping context at all) keeps rejecting so
        // the query stays on the Lucene doc value path.
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termQuery("body.raw", "x"), lookup)
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("dotted field"));
        Exception noMapping = expectThrows(
            IllegalArgumentException.class,
            () -> LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termQuery("meta.region", "east"))
        );
        assertTrue("unexpected message: " + noMapping.getMessage(), noMapping.getMessage().contains("dotted field"));
    }

    public void testPatternQueriesOnNonStringColumnRejected() {
        // With a mapping in hand a wildcard on a numeric column is
        // refused so the coordinator falls back to the Lucene path,
        // where the field type answers OpenSearch's stock 400.
        Function<String, String> lookup = name -> switch (name) {
            case "rating" -> "integer";
            case "body" -> "lance_text";
            case "category" -> "keyword";
            default -> null;
        };
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceKnnFilterTranslator.toLanceSql(QueryBuilders.wildcardQuery("rating", "1*"), lookup)
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("[integer]"));
        assertEquals("body LIKE '1%' ESCAPE '\\'", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.wildcardQuery("body", "1*"), lookup));
        assertEquals("starts_with(category, 'c')", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.prefixQuery("category", "c"), lookup));
        assertEquals(
            "regexp_like(unmapped, '^(?:.*)$')",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.regexpQuery("unmapped", ".*"), lookup)
        );
    }

    public void testHasUnmappedFieldCoversPatternQueries() {
        Function<String, String> lookup = name -> "body".equals(name) ? "lance_text" : null;
        assertFalse(LanceKnnFilterTranslator.hasUnmappedField(QueryBuilders.wildcardQuery("body", "h*"), lookup));
        assertTrue(LanceKnnFilterTranslator.hasUnmappedField(QueryBuilders.wildcardQuery("nope", "h*"), lookup));
        assertTrue(LanceKnnFilterTranslator.hasUnmappedField(QueryBuilders.regexpQuery("nope", "h.*"), lookup));
        assertTrue(LanceKnnFilterTranslator.hasUnmappedField(QueryBuilders.prefixQuery("nope", "h"), lookup));
        assertTrue(
            LanceKnnFilterTranslator.hasUnmappedField(
                QueryBuilders.boolQuery().filter(QueryBuilders.wildcardQuery("body", "h*")).mustNot(QueryBuilders.prefixQuery("nope", "h")),
                lookup
            )
        );
        // A sub-field path is not "unmapped": rejectMultiFieldPath
        // handles it inside toLanceSql.
        assertFalse(LanceKnnFilterTranslator.hasUnmappedField(QueryBuilders.wildcardQuery("body.raw", "h*"), lookup));
    }

    public void testDateOnIntegerColumnKeepsLiteralsNumeric() {
        // A date-mapped field whose Lance column is a plain integer
        // (the attach body's type: date override) compares as a number:
        // ISO strings are parsed to epoch millis and numeric bounds
        // stay bare, so DataFusion never sees a Timestamp literal
        // against an Int64 column.
        Function<String, String> lookup = name -> "ts".equals(name) ? LanceKnnFilterTranslator.DATE_ON_INTEGER : null;
        assertEquals(
            "(ts >= 1709251200000 AND ts < 1711929600000)",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.rangeQuery("ts").gte("2024-03-01").lt("2024-04-01"), lookup)
        );
        assertEquals(
            "(ts >= 1709251200000)",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.rangeQuery("ts").gte(1709251200000L), lookup)
        );
        assertEquals("ts = 1709251200000", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termQuery("ts", "2024-03-01"), lookup));
    }

    public void testDateOnTimestampColumnKeepsTimestampLiterals() {
        // The real Date / Timestamp column path is unchanged by the
        // override handling.
        Function<String, String> lookup = name -> "ts".equals(name) ? "date" : null;
        assertEquals(
            "(ts >= timestamp '2024-03-01')",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.rangeQuery("ts").gte("2024-03-01"), lookup)
        );
        assertEquals(
            "(ts >= to_timestamp_millis(1709251200000))",
            LanceKnnFilterTranslator.toLanceSql(QueryBuilders.rangeQuery("ts").gte(1709251200000L), lookup)
        );
    }

    public void testIpFieldRejectsEveryPushableShape() {
        // No predicate on an ip-overridden field pushes to Lance SQL:
        // the SQL would compare stored strings while the doc-value path
        // compares 16 byte encoded forms, so a range or CIDR match over
        // strings answers differently. The refusal sends the caller to
        // the Lucene path where IpFieldType builds the doc-value query.
        Function<String, String> lookup = name -> "addr".equals(name) ? LanceKnnFilterTranslator.IP_ON_UTF8 : null;
        for (QueryBuilder builder : List.<QueryBuilder>of(
            QueryBuilders.termQuery("addr", "10.0.0.4"),
            QueryBuilders.termsQuery("addr", "10.0.0.4", "2001:db8::1"),
            QueryBuilders.rangeQuery("addr").gte("10.0.0.0").lte("10.255.255.255"),
            QueryBuilders.termQuery("addr", "10.0.0.0/8"),
            QueryBuilders.existsQuery("addr")
        )) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> LanceKnnFilterTranslator.toLanceSql(builder, lookup)
            );
            assertTrue(e.getMessage(), e.getMessage().contains("addr"));
        }
        // Pattern queries refuse through the string-column gate: an ip
        // field is not a string type on the SQL side.
        IllegalArgumentException wildcard = expectThrows(
            IllegalArgumentException.class,
            () -> LanceKnnFilterTranslator.toLanceSql(QueryBuilders.wildcardQuery("addr", "10.*"), lookup)
        );
        assertTrue(wildcard.getMessage(), wildcard.getMessage().contains("needs a string column"));
        // A bool wrapping an ip clause refuses as a whole, so the
        // coordinator falls back to a null filterSql for the request.
        expectThrows(
            IllegalArgumentException.class,
            () -> LanceKnnFilterTranslator.toLanceSql(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("addr", "10.0.0.4")), lookup)
        );
        // Other fields keep translating under the same lookup.
        assertEquals("id = 1", LanceKnnFilterTranslator.toLanceSql(QueryBuilders.termQuery("id", 1), lookup));
    }
}

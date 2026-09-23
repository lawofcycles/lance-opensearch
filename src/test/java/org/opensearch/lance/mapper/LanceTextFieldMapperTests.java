/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.mapper;

import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.lance.ipc.FullTextQuery;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceScanFilterQuery;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link LanceTextFieldMapper} focused on the contract the
 * plugin relies on: the {@code lance_text} type name, the shape of the
 * query the field type hands back to Lucene, and the read-only guarantee on
 * the mapper itself. Instantiation of the parametrized mapper requires a
 * {@code Mapper.BuilderContext}, so those code paths are covered from
 * {@link LancePluginTests} through {@code PARSER}; here we work with the
 * field type directly, which is enough to lock in the observable behaviour.
 */
public class LanceTextFieldMapperTests extends OpenSearchTestCase {

    private LanceTextFieldMapper.LanceTextFieldType fieldType(String tokensColumn) {
        return new LanceTextFieldMapper.LanceTextFieldType("body", tokensColumn, Map.of());
    }

    public void testTypeNameIsLanceText() {
        assertEquals(LanceTextFieldMapper.CONTENT_TYPE, fieldType(null).typeName());
    }

    public void testTermQueryUsesFieldNameWhenNoTokensColumn() {
        Query query = fieldType(null).termQuery("camera", null);
        assertTrue("expected LanceFtsQuery, got " + query.getClass(), query instanceof LanceFtsQuery);
        assertEquals(new LanceFtsQuery("body", "camera"), query);
    }

    public void testTermQueryRoutesToTokensColumnWhenSet() {
        // The RFC's analyzer mode uses tokens_column to redirect queries at the
        // derived column that holds OpenSearch-analyzed tokens. The FLS
        // visibility set keeps the mapped field name: the derived column
        // is not a mapped field, so its name would never appear in a
        // reader's FieldInfos and the query would match nothing.
        Query query = fieldType("body_tokens").termQuery("camera", null);
        assertEquals(new LanceFtsQuery(FullTextQuery.match("camera", "body_tokens"), Set.of("body")), query);
    }

    public void testTermQueryUnwrapsBytesRef() {
        // Lucene's TermQueryBuilder hands BytesRef down to termQuery; the field
        // type has to decode it before passing the string on to Lance.
        Query query = fieldType(null).termQuery(new BytesRef("phone"), null);
        assertEquals(new LanceFtsQuery("body", "phone"), query);
    }

    public void testExistsQueryMatchesAllDocs() {
        // Lance-backed indexes do not store per-field norms, so exists() falls
        // through to a match-all. Locking the type in prevents accidental
        // regressions to a term-based exists filter that would not work.
        Query query = fieldType(null).existsQuery(null);
        assertTrue("expected MatchAllDocsQuery, got " + query.getClass(), query instanceof MatchAllDocsQuery);
    }

    public void testTermQueryRejectsDroppedField() {
        // LanceNamespaceService marks fields dropped in the mapping meta
        // when the Lance schema drops the underlying column. lance_text
        // queries against such a field must throw a 400 rather than
        // silently return zero hits from a column that no longer exists.
        LanceTextFieldMapper.LanceTextFieldType dropped = new LanceTextFieldMapper.LanceTextFieldType(
            "body",
            null,
            Map.of("lance_dropped", "true")
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> dropped.termQuery("hello", null));
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("no longer exists"));
    }

    public void testExistsQueryRejectsDroppedField() {
        LanceTextFieldMapper.LanceTextFieldType dropped = new LanceTextFieldMapper.LanceTextFieldType(
            "body",
            null,
            Map.of("lance_dropped", "true")
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> dropped.existsQuery(null));
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("no longer exists"));
    }

    public void testWildcardQueryBecomesLikeScanFilter() {
        Query query = fieldType(null).wildcardQuery("*w0001*", null, false, null);
        assertEquals(new LanceScanFilterQuery("body LIKE '%w0001%' ESCAPE '\\'"), query);
        assertEquals(LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED, ((LanceScanFilterQuery) query).scanLimit());
    }

    public void testWildcardQuestionMarkBecomesUnderscore() {
        Query query = fieldType(null).wildcardQuery("w?001", null, false, null);
        assertEquals(new LanceScanFilterQuery("body LIKE 'w_001' ESCAPE '\\'"), query);
    }

    public void testWildcardEscapesLikeMetaCharactersAndQuotes() {
        // A literal %, _ or \ in the pattern is escaped for LIKE, a
        // single quote is doubled for the SQL literal, and Lucene's
        // own \* escape yields a literal asterisk.
        Query query = fieldType(null).wildcardQuery("100%_O'Brien\\\\\\*", null, false, null);
        assertEquals(new LanceScanFilterQuery("body LIKE '100\\%\\_O''Brien\\\\*' ESCAPE '\\'"), query);
    }

    public void testWildcardCaseInsensitiveUsesIlike() {
        Query query = fieldType(null).wildcardQuery("*Hello*", null, true, null);
        assertEquals(new LanceScanFilterQuery("body ILIKE '%Hello%' ESCAPE '\\'"), query);
    }

    public void testWildcardTargetsRawColumnNotTokensColumn() {
        // A wildcard matches the stored string; the derived tokens
        // column holds analyzed tokens and is the wrong target for it.
        Query query = fieldType("body_tokens").wildcardQuery("hel*", null, false, null);
        assertEquals(new LanceScanFilterQuery("body LIKE 'hel%' ESCAPE '\\'"), query);
    }

    public void testRegexpQueryIsAnchoredRegexpLike() {
        Query query = fieldType(null).regexpQuery("w0001.*", RegExp.ALL, 0, 10000, null, null);
        assertEquals(new LanceScanFilterQuery("regexp_like(body, '^(?:w0001.*)$')"), query);
    }

    public void testRegexpPassesRustCompatibleSyntaxThrough() {
        Query query = fieldType(null).regexpQuery("(hello|quick) [a-z]+ \\d{1,3}?", RegExp.ALL, 0, 10000, null, null);
        assertEquals(new LanceScanFilterQuery("regexp_like(body, '^(?:(hello|quick) [a-z]+ \\d{1,3}?)$')"), query);
    }

    public void testRegexpCaseInsensitiveAddsInlineFlag() {
        Query query = fieldType(null).regexpQuery("hello.*", RegExp.ALL, RegExp.ASCII_CASE_INSENSITIVE, 10000, null, null);
        assertEquals(new LanceScanFilterQuery("regexp_like(body, '(?i)^(?:hello.*)$')"), query);
    }

    public void testRegexpEscapesSingleQuote() {
        Query query = fieldType(null).regexpQuery("O'Brien.*", RegExp.ALL, 0, 10000, null, null);
        assertEquals(new LanceScanFilterQuery("regexp_like(body, '^(?:O''Brien.*)$')"), query);
    }

    public void testRegexpRejectsLuceneOnlyOperators() {
        int all = RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT;
        for (String pattern : new String[] { "~(hello)", "a&b", "<1-100>", "hello@", "hello#", "\"quoted\"" }) {
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> fieldType(null).regexpQuery(pattern, all, 0, 10000, null, null)
            );
            assertTrue("unexpected message for " + pattern + ": " + e.getMessage(), e.getMessage().contains("Rust regex"));
            assertTrue("message must name the field: " + e.getMessage(), e.getMessage().contains("[body]"));
        }
    }

    public void testRegexpTreatsOperatorAsLiteralWhenItsFlagIsOff() {
        // With the flag off Lucene reads the character literally, which
        // is also what Rust does, so the pattern passes through. The
        // default flags of RegexpQueryBuilder (RegExp.ALL) do not
        // include the deprecated complement, so ~ is literal by default.
        Query tilde = fieldType(null).regexpQuery("a~b", RegExp.ALL, 0, 10000, null, null);
        assertEquals(new LanceScanFilterQuery("regexp_like(body, '^(?:a~b)$')"), tilde);
        Query ampersand = fieldType(null).regexpQuery("a&b", RegExp.NONE, 0, 10000, null, null);
        assertEquals(new LanceScanFilterQuery("regexp_like(body, '^(?:a&b)$')"), ampersand);
    }

    public void testRegexpIgnoresOperatorsInsideCharacterClassesAndEscapes() {
        int all = RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT;
        Query inClass = fieldType(null).regexpQuery("[~&<@#\"]+", all, 0, 10000, null, null);
        assertEquals(new LanceScanFilterQuery("regexp_like(body, '^(?:[~&<@#\"]+)$')"), inClass);
        Query escaped = fieldType(null).regexpQuery("a\\&b\\~c\\\"", all, 0, 10000, null, null);
        assertEquals(new LanceScanFilterQuery("regexp_like(body, '^(?:a\\&b\\~c\\\")$')"), escaped);
    }

    public void testPrefixQueryBecomesStartsWith() {
        Query query = fieldType(null).prefixQuery("hel", null, false, null);
        assertEquals(new LanceScanFilterQuery("starts_with(body, 'hel')"), query);
    }

    public void testPrefixCaseInsensitiveLowersBothSides() {
        Query query = fieldType(null).prefixQuery("HEL'lo", null, true, null);
        assertEquals(new LanceScanFilterQuery("starts_with(lower(body), lower('HEL''lo'))"), query);
    }

    public void testPatternQueriesRejectDroppedField() {
        LanceTextFieldMapper.LanceTextFieldType dropped = new LanceTextFieldMapper.LanceTextFieldType(
            "body",
            null,
            Map.of("lance_dropped", "true")
        );
        Exception wildcard = expectThrows(IllegalArgumentException.class, () -> dropped.wildcardQuery("h*", null, false, null));
        assertTrue(wildcard.getMessage(), wildcard.getMessage().contains("no longer exists"));
        Exception regexp = expectThrows(IllegalArgumentException.class, () -> dropped.regexpQuery("h.*", RegExp.ALL, 0, 1, null, null));
        assertTrue(regexp.getMessage(), regexp.getMessage().contains("no longer exists"));
        Exception prefix = expectThrows(IllegalArgumentException.class, () -> dropped.prefixQuery("h", null, false, null));
        assertTrue(prefix.getMessage(), prefix.getMessage().contains("no longer exists"));
    }

    public void testAnalyzerModeAccessors() {
        LanceTextFieldMapper.LanceTextFieldType analyzerMode = new LanceTextFieldMapper.LanceTextFieldType(
            "body",
            "body__lance_tokens",
            Map.of("lance_analyzer", "english")
        );
        assertEquals("english", analyzerMode.analyzerName());
        assertEquals("body__lance_tokens", analyzerMode.lanceColumn());
        assertEquals("body__lance_tokens", analyzerMode.tokensColumn());
        assertNull(fieldType(null).analyzerName());
        assertEquals("body", fieldType(null).lanceColumn());
        assertNull(fieldType(null).tokensColumn());
    }

    public void testContentTypeConstantIsLanceText() {
        // The string is referenced from LancePlugin.getMappers(); guard against
        // an accidental rename.
        assertEquals("lance_text", LanceTextFieldMapper.CONTENT_TYPE);
    }

    public void testParserIsRegistered() {
        // The parser instance is the one LancePlugin.getMappers() hands out;
        // its identity across lookups matters for classloader friendliness.
        assertNotNull(LanceTextFieldMapper.PARSER);
        assertSame(LanceTextFieldMapper.PARSER, LanceTextFieldMapper.PARSER);
    }
}

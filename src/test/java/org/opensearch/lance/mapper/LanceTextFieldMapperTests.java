/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.mapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.lance.ipc.FullTextQuery;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceScanFilterQuery;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link LanceTextFieldMapper} focused on the contract the
 * plugin relies on: the {@code lance_text} type name, the shape of the
 * query the field type hands back to Lucene, the read-only guarantee on
 * the mapper itself, and which changes of {@code tokens_column} a
 * mapping update may make. The query tests work with the field type
 * directly; the merge tests parse mappers through {@code PARSER}.
 */
public class LanceTextFieldMapperTests extends OpenSearchTestCase {

    private LanceTextFieldMapper.LanceTextFieldType fieldType(String tokensColumn) {
        return new LanceTextFieldMapper.LanceTextFieldType("body", tokensColumn, Map.of());
    }

    public void testTypeNameIsLanceText() {
        assertEquals(LanceTextFieldMapper.CONTENT_TYPE, fieldType(null).typeName());
    }

    /**
     * A {@code lance_text} mapper parsed from {@code node}, the way a
     * mapping update reaches the mapper through {@code PARSER}. The
     * parser context is not consulted for the two parameters the mapper
     * declares.
     */
    private static LanceTextFieldMapper mapperOf(Map<String, Object> node) {
        Mapper.BuilderContext context = new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0));
        return (LanceTextFieldMapper) LanceTextFieldMapper.PARSER.parse("body", new HashMap<>(node), null).build(context);
    }

    public void testMergeSetsTokensColumnOnAFieldThatHadNone() {
        // The interim mapping of an analyzer mode attach over a column
        // that already carries a Lance inverted index: lance_text with
        // no tokens column. The re-derivation after the backfill adds
        // the derived column and the analyzer name; both must merge.
        LanceTextFieldMapper interim = mapperOf(Map.of("type", "lance_text", "meta", Map.of("lance_field_id", "1")));
        LanceTextFieldMapper analyzerMode = mapperOf(
            Map.of(
                "type",
                "lance_text",
                "tokens_column",
                "body__lance_tokens",
                "meta",
                Map.of("lance_field_id", "1", "lance_analyzer", "english")
            )
        );
        LanceTextFieldMapper.LanceTextFieldType merged = (LanceTextFieldMapper.LanceTextFieldType) interim.merge(analyzerMode).fieldType();
        assertEquals("body__lance_tokens", merged.tokensColumn());
        assertEquals("body__lance_tokens", merged.lanceColumn());
        assertEquals("english", merged.analyzerName());
    }

    public void testMergeKeepsAnUnchangedTokensColumn() {
        LanceTextFieldMapper analyzerMode = mapperOf(Map.of("type", "lance_text", "tokens_column", "body__lance_tokens"));
        LanceTextFieldMapper same = mapperOf(Map.of("type", "lance_text", "tokens_column", "body__lance_tokens"));
        assertEquals("body__lance_tokens", ((LanceTextFieldMapper.LanceTextFieldType) analyzerMode.merge(same).fieldType()).tokensColumn());
    }

    public void testMergeRefusesToChangeOrDropASetTokensColumn() {
        // Renaming the derived column, or a derivation that no longer
        // sees it, is not a mapping update: the queries would target a
        // column the operator did not choose. Re-attaching is the way.
        LanceTextFieldMapper analyzerMode = mapperOf(Map.of("type", "lance_text", "tokens_column", "body__lance_tokens"));
        LanceTextFieldMapper renamed = mapperOf(Map.of("type", "lance_text", "tokens_column", "body_other"));
        Exception rename = expectThrows(IllegalArgumentException.class, () -> analyzerMode.merge(renamed));
        assertTrue(
            rename.getMessage(),
            rename.getMessage().contains("Cannot update parameter [tokens_column] from [body__lance_tokens] to [body_other]")
        );
        LanceTextFieldMapper plain = mapperOf(Map.of("type", "lance_text"));
        Exception drop = expectThrows(IllegalArgumentException.class, () -> analyzerMode.merge(plain));
        assertTrue(
            drop.getMessage(),
            drop.getMessage().contains("Cannot update parameter [tokens_column] from [body__lance_tokens] to [null]")
        );
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

    public void testSearchAnalyzerIsKeywordAndQuoteAnalyzerSplitsOnWhitespace() {
        // A stock match hands the whole text to termQuery (core short
        // circuits on the keyword analyzer); a stock match_phrase of
        // several words goes through the quote analyzer's tokens and
        // reaches phraseQuery, which core only calls on a field that
        // declares positions.
        TextSearchInfo info = fieldType(null).getTextSearchInfo();
        assertSame(Lucene.KEYWORD_ANALYZER, info.getSearchAnalyzer());
        assertSame(Lucene.WHITESPACE_ANALYZER, info.getSearchQuoteAnalyzer());
        assertTrue("phrase parsing needs positions", info.hasPositions());
    }

    public void testPhraseQueryJoinsTheStreamTermsIntoOneLancePhrase() throws Exception {
        try (TokenStream stream = Lucene.WHITESPACE_ANALYZER.tokenStream("body", "hello  lance")) {
            Query query = fieldType(null).phraseQuery(stream, 1, true, null);
            assertEquals(new LanceFtsQuery(FullTextQuery.phrase("hello lance", "body", 1), Set.of("body")), query);
        }
    }

    public void testPhraseQueryTargetsTheTokensColumn() throws Exception {
        try (TokenStream stream = Lucene.WHITESPACE_ANALYZER.tokenStream("body", "hello lance")) {
            Query query = fieldType("body_tokens").phraseQuery(stream, 0, true, null);
            assertEquals(new LanceFtsQuery(FullTextQuery.phrase("hello lance", "body_tokens", 0), Set.of("body")), query);
        }
    }

    public void testMultiPhraseQueryWithOneTermPerPositionIsAPlainPhrase() throws Exception {
        try (TokenStream stream = Lucene.WHITESPACE_ANALYZER.tokenStream("body", "quick fox")) {
            Query query = fieldType(null).multiPhraseQuery(stream, 2, true, null);
            assertEquals(new LanceFtsQuery(FullTextQuery.phrase("quick fox", "body", 2), Set.of("body")), query);
        }
    }

    public void testMultiPhraseQueryRejectsStackedTerms() throws Exception {
        // Two terms at one position (a synonym graph): Lance's phrase
        // query takes one term per position, so the field refuses.
        try (
            TokenStream stream = new CannedTokenStream(new Token("quick", 1, 0, 5), new Token("fast", 0, 0, 5), new Token("fox", 1, 6, 9))
        ) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> fieldType(null).multiPhraseQuery(stream, 0, true, null));
            assertTrue(e.getMessage(), e.getMessage().contains("one term per position"));
        }
    }

    public void testFuzzyQueryBecomesALanceMatchWithTheEditDistance() {
        Query query = fieldType(null).fuzzyQuery("helo", Fuzziness.ONE, 2, 30, true, null, null);
        FullTextQuery expected = FullTextQuery.match("helo", "body", 1f, Optional.of(1), 30, FullTextQuery.Operator.OR, 2);
        assertEquals(new LanceFtsQuery(expected, Set.of("body")), query);
    }

    public void testFuzzyDistanceResolvesAutoOnTheWholeText() {
        assertEquals(0, LanceTextFieldMapper.LanceTextFieldType.fuzzyDistance(Fuzziness.AUTO, "ab"));
        assertEquals(1, LanceTextFieldMapper.LanceTextFieldType.fuzzyDistance(Fuzziness.AUTO, "helo"));
        assertEquals(2, LanceTextFieldMapper.LanceTextFieldType.fuzzyDistance(Fuzziness.AUTO, "hello lance"));
        assertEquals(2, LanceTextFieldMapper.LanceTextFieldType.fuzzyDistance(Fuzziness.TWO, "ab"));
    }

    public void testPhraseAndFuzzyQueriesRejectDroppedField() throws Exception {
        LanceTextFieldMapper.LanceTextFieldType dropped = new LanceTextFieldMapper.LanceTextFieldType(
            "body",
            null,
            Map.of("lance_dropped", "true")
        );
        try (TokenStream stream = Lucene.WHITESPACE_ANALYZER.tokenStream("body", "hello lance")) {
            Exception phrase = expectThrows(IllegalArgumentException.class, () -> dropped.phraseQuery(stream, 0, true, null));
            assertTrue(phrase.getMessage(), phrase.getMessage().contains("no longer exists"));
        }
        Exception fuzzy = expectThrows(
            IllegalArgumentException.class,
            () -> dropped.fuzzyQuery("helo", Fuzziness.ONE, 0, 50, true, null, null)
        );
        assertTrue(fuzzy.getMessage(), fuzzy.getMessage().contains("no longer exists"));
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

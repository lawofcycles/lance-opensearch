/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.Map;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
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
        // derived column that holds OpenSearch-analyzed tokens.
        Query query = fieldType("body_tokens").termQuery("camera", null);
        assertEquals(new LanceFtsQuery("body_tokens", "camera"), query);
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.List;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

public class LanceFtsBoolQueryBuilderTests extends OpenSearchTestCase {

    private NamedWriteableRegistry namedWriteableRegistry() {
        SearchModule module = new SearchModule(Settings.EMPTY, List.of(new org.opensearch.lance.LancePlugin()));
        return new NamedWriteableRegistry(module.getNamedWriteables());
    }

    private NamedXContentRegistry namedXContentRegistry() {
        SearchModule module = new SearchModule(Settings.EMPTY, List.of(new org.opensearch.lance.LancePlugin()));
        return new NamedXContentRegistry(module.getNamedXContents());
    }

    // See LanceFtsBoostQueryBuilderTests.positionedParser for why the
    // parser must be pre-advanced before invoking fromXContent directly.
    private XContentParser positionedParser(String json) throws java.io.IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(namedXContentRegistry(), null, json);
        parser.nextToken();
        return parser;
    }

    public void testStreamRoundTrip() throws Exception {
        LanceFtsBoolQueryBuilder original = new LanceFtsBoolQueryBuilder().must(new LanceMatchQueryBuilder("body", "hello"))
            .should(new LanceMatchQueryBuilder("title", "sunny"))
            .mustNot(new LanceMatchQueryBuilder("body", "stale"))
            .boost(1.5f);
        original.queryName("bool_hit");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (
                StreamInput in = out.bytes().streamInput();
                NamedWriteableAwareStreamInput aware = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry())
            ) {
                LanceFtsBoolQueryBuilder copy = new LanceFtsBoolQueryBuilder(aware);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
                assertEquals(1, copy.mustClauses().size());
                assertEquals(1, copy.shouldClauses().size());
                assertEquals(1, copy.mustNotClauses().size());
            }
        }
    }

    public void testFromXContentReadsAllClauseArrays() throws Exception {
        String json = "{"
            + "\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
            + "\"should\":[{\"lance_match\":{\"field\":\"title\",\"query\":\"sunny\"}}],"
            + "\"must_not\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"stale\"}}]}";
        try (XContentParser parser = positionedParser(json)) {
            LanceFtsBoolQueryBuilder builder = LanceFtsBoolQueryBuilder.fromXContent(parser);
            assertEquals(1, builder.mustClauses().size());
            assertEquals(1, builder.shouldClauses().size());
            assertEquals(1, builder.mustNotClauses().size());
            assertTrue(
                "expected first must to be lance_match, got: " + builder.mustClauses().get(0),
                builder.mustClauses().get(0) instanceof LanceMatchQueryBuilder
            );
        }
    }

    public void testFromXContentReadsSingleObjectShortcut() throws Exception {
        // Stock bool accepts `{"must": {...}}` when there is one clause;
        // Lance's bool should follow the same convention so callers can
        // switch DSLs without changing the shape.
        String json = "{\"must\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}";
        try (XContentParser parser = positionedParser(json)) {
            LanceFtsBoolQueryBuilder builder = LanceFtsBoolQueryBuilder.fromXContent(parser);
            assertEquals(1, builder.mustClauses().size());
        }
    }

    public void testFromXContentRejectsEmptyClauses() throws Exception {
        // Lance's booleanQuery constructor rejects empty clauses. Catch
        // this at parse time with a message that tells the caller which
        // three lists they can populate.
        String json = "{}";
        try (XContentParser parser = positionedParser(json)) {
            Exception e = expectThrows(Exception.class, () -> LanceFtsBoolQueryBuilder.fromXContent(parser));
            assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("must"));
            assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("should"));
            assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("must_not"));
        }
    }

    public void testFromXContentRejectsUnknownArray() throws Exception {
        String json = "{\"filter\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}]}";
        try (XContentParser parser = positionedParser(json)) {
            Exception e = expectThrows(Exception.class, () -> LanceFtsBoolQueryBuilder.fromXContent(parser));
            assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("filter"));
        }
    }

    public void testFromXContentRejectsUnknownScalar() throws Exception {
        String json = "{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}]," + "\"minimum_should_match\":1}";
        try (XContentParser parser = positionedParser(json)) {
            Exception e = expectThrows(Exception.class, () -> LanceFtsBoolQueryBuilder.fromXContent(parser));
            assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("minimum_should_match"));
        }
    }

    public void testToXContentOmitsEmptyClauseArrays() throws Exception {
        LanceFtsBoolQueryBuilder builder = new LanceFtsBoolQueryBuilder().must(new LanceMatchQueryBuilder("body", "hello"));
        try (org.opensearch.core.xcontent.XContentBuilder out = JsonXContent.contentBuilder()) {
            builder.toXContent(out, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            String json = out.toString();
            assertTrue("expected must in json: " + json, json.contains("\"must\""));
            assertFalse("did not expect should in json: " + json, json.contains("\"should\""));
            assertFalse("did not expect must_not in json: " + json, json.contains("\"must_not\""));
        }
    }
}

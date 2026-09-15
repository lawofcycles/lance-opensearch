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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

public class LanceFtsBoostQueryBuilderTests extends OpenSearchTestCase {

    private NamedWriteableRegistry namedWriteableRegistry() {
        SearchModule module = new SearchModule(Settings.EMPTY, List.of(new org.opensearch.lance.LancePlugin()));
        return new NamedWriteableRegistry(module.getNamedWriteables());
    }

    private NamedXContentRegistry namedXContentRegistry() {
        SearchModule module = new SearchModule(Settings.EMPTY, List.of(new org.opensearch.lance.LancePlugin()));
        return new NamedXContentRegistry(module.getNamedXContents());
    }

    // fromXContent expects the parser to already be positioned at the
    // START_OBJECT of the query body (that is what happens when it is
    // invoked via parseInnerQueryBuilder → namedObject dispatch). Fresh
    // parsers created directly in tests start before the first token, so
    // advance them once before handing them to the builder.
    private XContentParser positionedParser(String json) throws java.io.IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(namedXContentRegistry(), null, json);
        parser.nextToken();
        return parser;
    }

    public void testStreamRoundTrip() throws Exception {
        QueryBuilder positive = new LanceMatchQueryBuilder("body", "hello");
        QueryBuilder negative = new LanceMatchQueryBuilder("body", "outdated");
        LanceFtsBoostQueryBuilder original = new LanceFtsBoostQueryBuilder(positive, negative).negativeBoost(0.3f).boost(2f);
        original.queryName("boosted");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (
                StreamInput in = out.bytes().streamInput();
                NamedWriteableAwareStreamInput aware = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry())
            ) {
                LanceFtsBoostQueryBuilder copy = new LanceFtsBoostQueryBuilder(aware);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
                assertEquals(Float.valueOf(0.3f), copy.negativeBoost());
                assertEquals("boosted", copy.queryName());
            }
        }
    }

    public void testStreamRoundTripWithoutNegativeBoost() throws Exception {
        // When the caller omits negative_boost the stream form must not
        // rehydrate as 0f, which Lance would treat as "kill matches on
        // the negative clause"; keep it null so Lance applies its 0.5
        // default just like a fresh request would.
        QueryBuilder positive = new LanceMatchQueryBuilder("body", "hello");
        QueryBuilder negative = new LanceMatchQueryBuilder("body", "world");
        LanceFtsBoostQueryBuilder original = new LanceFtsBoostQueryBuilder(positive, negative);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (
                StreamInput in = out.bytes().streamInput();
                NamedWriteableAwareStreamInput aware = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry())
            ) {
                LanceFtsBoostQueryBuilder copy = new LanceFtsBoostQueryBuilder(aware);
                assertNull(copy.negativeBoost());
            }
        }
    }

    public void testFromXContentReadsPositiveNegative() throws Exception {
        String json = "{\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
            + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"stale\"}},"
            + "\"negative_boost\":0.4}";
        try (XContentParser parser = positionedParser(json)) {
            LanceFtsBoostQueryBuilder builder = LanceFtsBoostQueryBuilder.fromXContent(parser);
            assertTrue("expected positive lance_match, got: " + builder.positive(), builder.positive() instanceof LanceMatchQueryBuilder);
            assertTrue("expected negative lance_match, got: " + builder.negative(), builder.negative() instanceof LanceMatchQueryBuilder);
            assertEquals(Float.valueOf(0.4f), builder.negativeBoost());
        }
    }

    public void testFromXContentDefaultsNegativeBoostToNull() throws Exception {
        String json = "{\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
            + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"stale\"}}}";
        try (XContentParser parser = positionedParser(json)) {
            LanceFtsBoostQueryBuilder builder = LanceFtsBoostQueryBuilder.fromXContent(parser);
            assertNull("expected negative_boost null when omitted", builder.negativeBoost());
        }
    }

    public void testFromXContentRejectsMissingPositive() throws Exception {
        String json = "{\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"stale\"}}}";
        try (XContentParser parser = positionedParser(json)) {
            Exception e = expectThrows(Exception.class, () -> LanceFtsBoostQueryBuilder.fromXContent(parser));
            assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("positive"));
        }
    }

    public void testFromXContentRejectsMissingNegative() throws Exception {
        String json = "{\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}";
        try (XContentParser parser = positionedParser(json)) {
            Exception e = expectThrows(Exception.class, () -> LanceFtsBoostQueryBuilder.fromXContent(parser));
            assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("negative"));
        }
    }

    public void testFromXContentRejectsUnknownParameter() throws Exception {
        String json = "{\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
            + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"stale\"}},"
            + "\"minimum_should_match\":1}";
        try (XContentParser parser = positionedParser(json)) {
            Exception e = expectThrows(Exception.class, () -> LanceFtsBoostQueryBuilder.fromXContent(parser));
            assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("minimum_should_match"));
        }
    }

    public void testFromXContentRejectsNegativeNegativeBoost() throws Exception {
        String json = "{\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
            + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"stale\"}},"
            + "\"negative_boost\":-0.1}";
        try (XContentParser parser = positionedParser(json)) {
            Exception e = expectThrows(Exception.class, () -> LanceFtsBoostQueryBuilder.fromXContent(parser));
            assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("negative_boost"));
        }
    }

    public void testToXContentEmitsPositiveNegativeAndNegativeBoost() throws Exception {
        LanceFtsBoostQueryBuilder builder = new LanceFtsBoostQueryBuilder(
            new LanceMatchQueryBuilder("body", "hello"),
            new LanceMatchQueryBuilder("body", "stale")
        ).negativeBoost(0.25f);
        try (org.opensearch.core.xcontent.XContentBuilder out = JsonXContent.contentBuilder()) {
            builder.toXContent(out, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            String json = out.toString();
            assertTrue("expected positive: " + json, json.contains("\"positive\""));
            assertTrue("expected negative: " + json, json.contains("\"negative\""));
            assertTrue("expected negative_boost: " + json, json.contains("\"negative_boost\":0.25"));
        }
    }
}

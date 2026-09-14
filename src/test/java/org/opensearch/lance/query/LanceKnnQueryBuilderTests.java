/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

public class LanceKnnQueryBuilderTests extends OpenSearchTestCase {

    private static final String FIELD = "embedding";
    private static final float[] VECTOR = new float[] { 0.1f, 0.2f, 0.3f, 0.4f };
    private static final int K = 5;

    public void testConstructorPreservesFields() {
        LanceKnnQueryBuilder builder = new LanceKnnQueryBuilder(FIELD, VECTOR, K);
        assertEquals(LanceKnnQueryBuilder.NAME, builder.getWriteableName());
    }

    public void testStreamSerializationRoundTrip() throws Exception {
        LanceKnnQueryBuilder original = new LanceKnnQueryBuilder(FIELD, VECTOR, K);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceKnnQueryBuilder copy = new LanceKnnQueryBuilder(in);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
            }
        }
    }

    public void testFromXContentHappyPath() throws Exception {
        String json = "{\n" + "  \"field\": \"embedding\",\n" + "  \"vector\": [0.1, 0.2, 0.3, 0.4],\n" + "  \"k\": 5\n" + "}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceKnnQueryBuilder builder = LanceKnnQueryBuilder.fromXContent(parser);
            assertEquals(new LanceKnnQueryBuilder(FIELD, VECTOR, K), builder);
        }
    }

    public void testFromXContentDefaultsKToTen() throws Exception {
        // k is optional; the parser has to default to 10 rather than crash.
        String json = "{\n" + "  \"field\": \"embedding\",\n" + "  \"vector\": [0.1, 0.2]\n" + "}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceKnnQueryBuilder builder = LanceKnnQueryBuilder.fromXContent(parser);
            assertEquals(new LanceKnnQueryBuilder("embedding", new float[] { 0.1f, 0.2f }, 10), builder);
        }
    }

    public void testFromXContentMissingVectorFails() throws Exception {
        // Regression: the previous form of the query (with the field name as an
        // outer key and no explicit `vector` inside) hit an NPE inside
        // fromXContent when `vector` was missing. The parser should throw with a
        // clear message instead of NullPointerException.
        String json = "{\n" + "  \"field\": \"embedding\",\n" + "  \"k\": 3\n" + "}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            expectThrows(Exception.class, () -> LanceKnnQueryBuilder.fromXContent(parser));
        }
    }

    public void testEqualsAndHashCode() {
        LanceKnnQueryBuilder a = new LanceKnnQueryBuilder(FIELD, VECTOR, K);
        LanceKnnQueryBuilder b = new LanceKnnQueryBuilder(FIELD, new float[] { 0.1f, 0.2f, 0.3f, 0.4f }, K);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        LanceKnnQueryBuilder differentField = new LanceKnnQueryBuilder("other", VECTOR, K);
        assertNotEquals(a, differentField);

        LanceKnnQueryBuilder differentVector = new LanceKnnQueryBuilder(FIELD, new float[] { 1.0f, 2.0f, 3.0f, 4.0f }, K);
        assertNotEquals(a, differentVector);

        LanceKnnQueryBuilder differentK = new LanceKnnQueryBuilder(FIELD, VECTOR, K + 1);
        assertNotEquals(a, differentK);
    }

    public void testToXContentIsParseable() throws Exception {
        LanceKnnQueryBuilder original = new LanceKnnQueryBuilder(FIELD, VECTOR, K);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            original.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            String json = builder.toString();
            // toXContent wraps the query in an outer `lance_knn` object. Strip it
            // for the round trip so fromXContent sees the same shape it accepts
            // from user input.
            int start = json.indexOf('{', json.indexOf(LanceKnnQueryBuilder.NAME));
            int end = json.lastIndexOf('}');
            String inner = json.substring(start, end);
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, inner)) {
                LanceKnnQueryBuilder parsed = LanceKnnQueryBuilder.fromXContent(parser);
                assertEquals(original, parsed);
            }
        }
    }

    public void testStreamSerializationRoundTripWithExtras() throws Exception {
        LanceKnnQueryBuilder original = new LanceKnnQueryBuilder(FIELD, VECTOR, K).nprobes(20)
            .refineFactor(4)
            .ef(64)
            .metric("cosine")
            .useIndex(false);
        original.boost(1.5f);
        original.queryName("named");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceKnnQueryBuilder copy = new LanceKnnQueryBuilder(in);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
            }
        }
    }

    public void testFromXContentAcceptsAllTuningKnobs() throws Exception {
        String json = "{\n"
            + "  \"field\": \"embedding\",\n"
            + "  \"vector\": [0.1, 0.2, 0.3, 0.4],\n"
            + "  \"k\": 5,\n"
            + "  \"nprobes\": 20,\n"
            + "  \"refine_factor\": 4,\n"
            + "  \"ef\": 64,\n"
            + "  \"metric\": \"cosine\",\n"
            + "  \"use_index\": false,\n"
            + "  \"boost\": 1.5,\n"
            + "  \"_name\": \"primary\"\n"
            + "}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceKnnQueryBuilder builder = LanceKnnQueryBuilder.fromXContent(parser);
            assertEquals(Integer.valueOf(20), builder.nprobes());
            assertEquals(Integer.valueOf(4), builder.refineFactor());
            assertEquals(Integer.valueOf(64), builder.ef());
            assertEquals("cosine", builder.metric());
            assertEquals(Boolean.FALSE, builder.useIndex());
            assertEquals(1.5f, builder.boost(), 0.0001f);
            assertEquals("primary", builder.queryName());
        }
    }

    public void testFromXContentRejectsUnknownParameter() throws Exception {
        // A typo like `nprobe` (singular) should surface as 400 instead of
        // silently degrading recall. Rejecting unknown keys is the whole point
        // of the strict parser.
        String json = "{\n" + "  \"field\": \"embedding\",\n" + "  \"vector\": [0.1, 0.2],\n" + "  \"nprobe\": 20\n" + "}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceKnnQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("nprobe"));
        }
    }

    public void testFromXContentRejectsNonStringMetric() throws Exception {
        String json = "{\n" + "  \"field\": \"embedding\",\n" + "  \"vector\": [0.1, 0.2],\n" + "  \"metric\": 42\n" + "}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceKnnQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("metric"));
        }
    }

    public void testFromXContentRejectsZeroK() throws Exception {
        String json = "{\n" + "  \"field\": \"embedding\",\n" + "  \"vector\": [0.1, 0.2],\n" + "  \"k\": 0\n" + "}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceKnnQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("k must be"));
        }
    }

    public void testFilterSetterAndGetterRoundTrip() {
        LanceKnnQueryBuilder builder = new LanceKnnQueryBuilder(FIELD, VECTOR, K);
        org.opensearch.index.query.RangeQueryBuilder range = org.opensearch.index.query.QueryBuilders.rangeQuery("rating").gte(4);
        builder.filter(range);
        assertSame(range, builder.filter());
    }

    public void testFilterAffectsEqualsAndHashCode() {
        LanceKnnQueryBuilder a = new LanceKnnQueryBuilder(FIELD, VECTOR, K);
        LanceKnnQueryBuilder b = new LanceKnnQueryBuilder(FIELD, VECTOR, K);
        assertEquals(a, b);
        b.filter(org.opensearch.index.query.QueryBuilders.rangeQuery("rating").gte(4));
        assertNotEquals(a, b);
        a.filter(org.opensearch.index.query.QueryBuilders.rangeQuery("rating").gte(4));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testToXContentIncludesFilterClause() throws Exception {
        // toXContent has to surface the filter under the `filter` key so
        // that _explain and diagnostic dumps show what pre-filter Lance
        // will actually apply.
        LanceKnnQueryBuilder builder = new LanceKnnQueryBuilder(FIELD, VECTOR, K);
        builder.filter(org.opensearch.index.query.QueryBuilders.rangeQuery("rating").gte(4));
        try (XContentBuilder out = JsonXContent.contentBuilder()) {
            builder.toXContent(out, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            String json = out.toString();
            assertTrue("json missing filter: " + json, json.contains("\"filter\""));
            assertTrue("json missing range: " + json, json.contains("\"range\""));
            assertTrue("json missing rating: " + json, json.contains("\"rating\""));
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import org.lance.ipc.FullTextQuery;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

public class LanceMatchQueryBuilderTests extends OpenSearchTestCase {

    public void testStreamRoundTrip() throws Exception {
        LanceMatchQueryBuilder original = new LanceMatchQueryBuilder("body", "quick brown fox");
        original.operator(FullTextQuery.Operator.AND).fuzziness(1).prefixLength(2).maxExpansions(20).boost(1.5f);
        original.queryName("hit");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceMatchQueryBuilder copy = new LanceMatchQueryBuilder(in);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
                assertEquals(Integer.valueOf(1), copy.fuzziness());
                assertEquals(2, copy.prefixLength());
                assertEquals(20, copy.maxExpansions());
            }
        }
    }

    public void testStreamRoundTripWithoutFuzziness() throws Exception {
        // Fuzziness stays null when the caller does not set it; make sure
        // the optional VInt round trip keeps that shape (rather than
        // rehydrating as 0, which Lance would interpret as an exact match
        // but leaves the semantic difference between "unset" and
        // "0 edits" visible in the DSL).
        LanceMatchQueryBuilder original = new LanceMatchQueryBuilder("body", "hello");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceMatchQueryBuilder copy = new LanceMatchQueryBuilder(in);
                assertNull(copy.fuzziness());
            }
        }
    }

    public void testFromXContentDefaultOperatorIsOr() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"quick brown\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceMatchQueryBuilder builder = LanceMatchQueryBuilder.fromXContent(parser);
            assertEquals("body", builder.field());
            assertEquals("quick brown", builder.query());
            assertEquals(FullTextQuery.Operator.OR, builder.operator());
            assertNull(builder.fuzziness());
            assertEquals(0, builder.prefixLength());
            assertEquals(50, builder.maxExpansions());
        }
    }

    public void testFromXContentReadsAndOperator() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"quick brown\",\"operator\":\"and\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceMatchQueryBuilder builder = LanceMatchQueryBuilder.fromXContent(parser);
            assertEquals(FullTextQuery.Operator.AND, builder.operator());
        }
    }

    public void testFromXContentReadsFuzziness() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"quick\",\"fuzziness\":2}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceMatchQueryBuilder builder = LanceMatchQueryBuilder.fromXContent(parser);
            assertEquals(Integer.valueOf(2), builder.fuzziness());
        }
    }

    public void testFromXContentReadsPrefixLengthAndMaxExpansions() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"quick\",\"prefix_length\":3,\"max_expansions\":10}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceMatchQueryBuilder builder = LanceMatchQueryBuilder.fromXContent(parser);
            assertEquals(3, builder.prefixLength());
            assertEquals(10, builder.maxExpansions());
        }
    }

    public void testFromXContentRejectsUnknownOperator() throws Exception {
        // 'xor' is not a Lance FTS operator; surface it as 400 rather than
        // silently degrading to the default.
        String json = "{\"field\":\"body\",\"query\":\"quick\",\"operator\":\"xor\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("operator"));
        }
    }

    public void testFromXContentRejectsAutoFuzziness() throws Exception {
        // OpenSearch's stock match accepts "AUTO" as a fuzziness setting,
        // but Lance's FullTextQuery.fuzziness is Optional<Integer>. Reject
        // AUTO with an actionable 400 rather than silently accepting it
        // and running an exact match.
        String json = "{\"field\":\"body\",\"query\":\"quick\",\"fuzziness\":\"AUTO\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("fuzziness"));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("AUTO"));
        }
    }

    public void testFromXContentRejectsNegativeFuzziness() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"quick\",\"fuzziness\":-1}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("fuzziness"));
        }
    }

    public void testFromXContentRejectsZeroMaxExpansions() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"quick\",\"max_expansions\":0}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("max_expansions"));
        }
    }

    public void testFromXContentRejectsUnknownParameter() throws Exception {
        // analyzer is not on Lance's FTS surface; reject with 400 rather
        // than silently ignoring the setting.
        String json = "{\"field\":\"body\",\"query\":\"quick\",\"analyzer\":\"whitespace\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("analyzer"));
        }
    }

    public void testFromXContentRejectsMissingField() throws Exception {
        String json = "{\"query\":\"quick\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("field"));
        }
    }

    public void testFromXContentRejectsMissingQuery() throws Exception {
        String json = "{\"field\":\"body\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("query"));
        }
    }

    public void testToXContentIncludesOperatorWhenAnd() throws Exception {
        LanceMatchQueryBuilder builder = new LanceMatchQueryBuilder("body", "hello");
        builder.operator(FullTextQuery.Operator.AND);
        try (org.opensearch.core.xcontent.XContentBuilder out = JsonXContent.contentBuilder()) {
            builder.toXContent(out, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            String json = out.toString();
            assertTrue("expected operator in json: " + json, json.contains("\"operator\""));
            assertTrue("expected 'and' in json: " + json, json.contains("and"));
        }
    }

    public void testToXContentOmitsOperatorWhenOrDefault() throws Exception {
        LanceMatchQueryBuilder builder = new LanceMatchQueryBuilder("body", "hello");
        try (org.opensearch.core.xcontent.XContentBuilder out = JsonXContent.contentBuilder()) {
            builder.toXContent(out, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            String json = out.toString();
            assertFalse("did not expect operator in json when default OR: " + json, json.contains("\"operator\""));
        }
    }

    public void testToXContentEmitsFuzzinessOnlyWhenSet() throws Exception {
        LanceMatchQueryBuilder without = new LanceMatchQueryBuilder("body", "hello");
        try (org.opensearch.core.xcontent.XContentBuilder out = JsonXContent.contentBuilder()) {
            without.toXContent(out, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            assertFalse("did not expect fuzziness when unset: " + out, out.toString().contains("\"fuzziness\""));
        }

        LanceMatchQueryBuilder with = new LanceMatchQueryBuilder("body", "hello").fuzziness(2);
        try (org.opensearch.core.xcontent.XContentBuilder out = JsonXContent.contentBuilder()) {
            with.toXContent(out, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            String json = out.toString();
            assertTrue("expected fuzziness=2 in json: " + json, json.contains("\"fuzziness\":2"));
        }
    }
}

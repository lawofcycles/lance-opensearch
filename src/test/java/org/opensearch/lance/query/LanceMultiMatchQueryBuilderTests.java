/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.List;

import org.lance.ipc.FullTextQuery;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

public class LanceMultiMatchQueryBuilderTests extends OpenSearchTestCase {

    public void testStreamRoundTrip() throws Exception {
        LanceMultiMatchQueryBuilder original = new LanceMultiMatchQueryBuilder(List.of("body", "title"), "hello");
        original.operator(FullTextQuery.Operator.AND).boosts(List.of(2.0f, 0.5f)).boost(1.5f);
        original.queryName("mm_hit");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceMultiMatchQueryBuilder copy = new LanceMultiMatchQueryBuilder(in);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
                assertEquals(List.of("body", "title"), copy.fields());
                assertEquals(List.of(2.0f, 0.5f), copy.boosts());
                assertEquals(FullTextQuery.Operator.AND, copy.operator());
            }
        }
    }

    public void testStreamRoundTripWithoutBoosts() throws Exception {
        // The default-shaped variant (no per-field boosts, OR operator)
        // must serialise the same way an incoming request would, so a
        // node relaying the query does not accidentally introduce a
        // boost list of zeros.
        LanceMultiMatchQueryBuilder original = new LanceMultiMatchQueryBuilder(List.of("body"), "hello");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceMultiMatchQueryBuilder copy = new LanceMultiMatchQueryBuilder(in);
                assertNull(copy.boosts());
                assertEquals(FullTextQuery.Operator.OR, copy.operator());
            }
        }
    }

    public void testFromXContentReadsFieldsAndDefaults() throws Exception {
        String json = "{\"fields\":[\"body\",\"title\"],\"query\":\"hello\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceMultiMatchQueryBuilder builder = LanceMultiMatchQueryBuilder.fromXContent(parser);
            assertEquals(List.of("body", "title"), builder.fields());
            assertEquals("hello", builder.query());
            assertEquals(FullTextQuery.Operator.OR, builder.operator());
            assertNull(builder.boosts());
        }
    }

    public void testFromXContentReadsBoostsAndOperator() throws Exception {
        String json = "{\"fields\":[\"body\",\"title\"],\"query\":\"hello\",\"boosts\":[1.0,3.0],\"operator\":\"and\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceMultiMatchQueryBuilder builder = LanceMultiMatchQueryBuilder.fromXContent(parser);
            assertEquals(List.of(1.0f, 3.0f), builder.boosts());
            assertEquals(FullTextQuery.Operator.AND, builder.operator());
        }
    }

    public void testFromXContentRejectsMissingFields() throws Exception {
        String json = "{\"query\":\"hello\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMultiMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("fields"));
        }
    }

    public void testFromXContentRejectsEmptyFields() throws Exception {
        String json = "{\"fields\":[],\"query\":\"hello\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMultiMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("fields"));
        }
    }

    public void testFromXContentRejectsMissingQuery() throws Exception {
        String json = "{\"fields\":[\"body\"]}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMultiMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("query"));
        }
    }

    public void testFromXContentRejectsBoostsLengthMismatch() throws Exception {
        // Silently truncating or padding boosts to match fields would
        // change scoring in a way the caller cannot see. Return 400 so
        // the mismatch is caught before it reaches Lance.
        String json = "{\"fields\":[\"body\",\"title\"],\"query\":\"hello\",\"boosts\":[1.0]}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMultiMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("boosts"));
        }
    }

    public void testFromXContentRejectsUnknownOperator() throws Exception {
        String json = "{\"fields\":[\"body\"],\"query\":\"hello\",\"operator\":\"xor\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMultiMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("operator"));
        }
    }

    public void testFromXContentRejectsUnknownParameter() throws Exception {
        // "tie_breaker" is on OpenSearch's stock multi_match but not on
        // Lance's surface today; reject rather than silently ignoring.
        String json = "{\"fields\":[\"body\"],\"query\":\"hello\",\"tie_breaker\":0.3}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMultiMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("tie_breaker"));
        }
    }

    public void testToXContentEmitsBoostsAndOperatorWhenSet() throws Exception {
        LanceMultiMatchQueryBuilder builder = new LanceMultiMatchQueryBuilder(List.of("body", "title"), "hello");
        builder.operator(FullTextQuery.Operator.AND).boosts(List.of(2.0f, 1.0f));
        try (org.opensearch.core.xcontent.XContentBuilder out = JsonXContent.contentBuilder()) {
            builder.toXContent(out, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            String json = out.toString();
            assertTrue("expected operator in json: " + json, json.contains("\"operator\":\"and\""));
            assertTrue("expected boosts in json: " + json, json.contains("\"boosts\""));
            assertTrue("expected 2.0 in json: " + json, json.contains("2.0"));
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

public class LanceMatchPhraseQueryBuilderTests extends OpenSearchTestCase {

    public void testStreamRoundTrip() throws Exception {
        LanceMatchPhraseQueryBuilder original = new LanceMatchPhraseQueryBuilder("body", "quick brown fox");
        original.slop(2).boost(1.5f);
        original.queryName("phrase_hit");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceMatchPhraseQueryBuilder copy = new LanceMatchPhraseQueryBuilder(in);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
            }
        }
    }

    public void testFromXContentDefaultSlopIsZero() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"quick brown\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceMatchPhraseQueryBuilder builder = LanceMatchPhraseQueryBuilder.fromXContent(parser);
            assertEquals("body", builder.field());
            assertEquals("quick brown", builder.query());
            assertEquals(0, builder.slop());
        }
    }

    public void testFromXContentReadsSlop() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"quick fox\",\"slop\":3}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceMatchPhraseQueryBuilder builder = LanceMatchPhraseQueryBuilder.fromXContent(parser);
            assertEquals(3, builder.slop());
        }
    }

    public void testFromXContentRejectsNegativeSlop() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"q\",\"slop\":-1}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchPhraseQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("slop"));
        }
    }

    public void testFromXContentRejectsUnknownParameter() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"q\",\"analyzer\":\"whitespace\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchPhraseQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("analyzer"));
        }
    }

    public void testFromXContentRejectsMissingField() throws Exception {
        String json = "{\"query\":\"q\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchPhraseQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("field"));
        }
    }

    public void testFromXContentRejectsMissingQuery() throws Exception {
        String json = "{\"field\":\"body\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchPhraseQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("query"));
        }
    }
}

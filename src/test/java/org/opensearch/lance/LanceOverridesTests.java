/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.common.settings.Settings;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The overrides value object: settings accessor precedence (the new
 * setting first, the legacy multi-fields setting as fallback), the
 * structural validation of the attach clauses, the multi-fields fold
 * and the canonical JSON round trip.
 */
public class LanceOverridesTests extends OpenSearchTestCase {

    public void testOfPrefersOverridesSetting() {
        Settings settings = Settings.builder()
            .put(LanceEngineFactory.OVERRIDES_SETTING, "{\"ts\":{\"type\":\"date\"}}")
            .put(LanceEngineFactory.MULTI_FIELDS_SETTING, "{\"body\":{\"raw\":\"keyword\"}}")
            .build();
        LanceOverrides overrides = LanceOverrides.of(settings);
        assertEquals(Set.of("ts"), overrides.dateColumns().keySet());
        // The legacy setting is ignored once the new one is present.
        assertTrue(overrides.subFields().isEmpty());
    }

    public void testOfFallsBackToLegacyMultiFields() {
        Settings settings = Settings.builder().put(LanceEngineFactory.MULTI_FIELDS_SETTING, "{\"body\":{\"raw\":\"keyword\"}}").build();
        LanceOverrides overrides = LanceOverrides.of(settings);
        assertEquals(Set.of("body"), overrides.subFields().keySet());
        assertEquals("keyword", overrides.subFields().get("body").get("raw"));
        assertTrue(overrides.keywordColumns().isEmpty());
        assertTrue(overrides.dateColumns().isEmpty());
    }

    public void testOfBothEmpty() {
        assertTrue(LanceOverrides.of(Settings.EMPTY).isEmpty());
    }

    public void testJsonRoundTripKeepsOrderAndShape() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("ts", Map.of("type", "date", "format", "epoch_millis"));
        body.put("label", Map.of("type", "keyword"));
        body.put("body", Map.of("fields", Map.of("raw", Map.of("type", "keyword"))));
        LanceOverrides original = LanceOverrides.parseAttachClauses(body, null);
        LanceOverrides restored = LanceOverrides.parse(original.toJson());
        assertEquals(original, restored);
        assertEquals(List.of("ts", "label", "body"), List.copyOf(restored.columns().keySet()));
        assertEquals("epoch_millis", restored.columns().get("ts").format());
        assertEquals(Set.of("label"), restored.keywordColumns());
        assertEquals(Set.of("body"), restored.subFields().keySet());
    }

    public void testMultiFieldsClauseFoldsIntoFields() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(null, Map.of("body", Map.of("raw", Map.of("type", "keyword"))));
        assertEquals(Set.of("body"), overrides.subFields().keySet());
        assertEquals("keyword", overrides.subFields().get("body").get("raw"));
        assertNull(overrides.columns().get("body").type());
    }

    public void testBothClausesForTheSameColumnRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(
                Map.of("body", Map.of("fields", Map.of("raw", Map.of("type", "keyword")))),
                Map.of("body", Map.of("raw", Map.of("type", "keyword")))
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("both [multi_fields] and [overrides"));
    }

    public void testMultiFieldsClauseMergesWithTypeOnlyOverride() {
        // A type-only override and a legacy sub-field declaration for the
        // same column do not conflict: the fold fills the empty fields.
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            Map.of("body", Map.of("type", "keyword")),
            Map.of("body", Map.of("raw", Map.of("type", "keyword")))
        );
        assertEquals(Set.of("body"), overrides.keywordColumns());
        assertEquals("keyword", overrides.subFields().get("body").get("raw"));
    }

    public void testUnknownKeyRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("ts", Map.of("analyzer", "kuromoji")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("unknown key [analyzer]"));
        assertTrue(e.getMessage(), e.getMessage().contains("[type], [format], [fields]"));
    }

    public void testUnknownTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("col", Map.of("type", "ip")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("type=ip"));
        assertTrue(e.getMessage(), e.getMessage().contains("[date], [keyword]"));
    }

    public void testFormatWithoutDateTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("col", Map.of("type", "keyword", "format", "epoch_millis")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("only accepted together with [type: date]"));
    }

    public void testInvalidFormatRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("col", Map.of("type", "date", "format", "not a format [")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("not a valid date format"));
    }

    public void testEmptyColumnEntryRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("col", Map.of()), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("at least one of [type], [fields]"));
    }

    public void testNonObjectOverridesRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses("not an object", null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[overrides] must be an object"));
    }

    public void testSubFieldWithoutTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("body", Map.of("fields", Map.of("raw", Map.of()))), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("fields.raw.type] is required"));
    }
}

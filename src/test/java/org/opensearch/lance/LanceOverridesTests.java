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

    public void testSubFieldNameWithWhitespaceRejectedInBothClauses() {
        // The multi_fields clause and overrides.[col].fields share one
        // name rule: a space, a dot, a control character or an empty
        // name is refused before the table is opened, naming the clause.
        IllegalArgumentException legacy = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(null, Map.of("body", Map.of("raw name", Map.of("type", "keyword"))))
        );
        assertTrue(legacy.getMessage(), legacy.getMessage().contains("[multi_fields.body] sub-field name [raw name]"));
        assertTrue(legacy.getMessage(), legacy.getMessage().contains("whitespace or control characters"));

        IllegalArgumentException fields = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("body", Map.of("fields", Map.of("raw name", Map.of("type", "keyword")))), null)
        );
        assertTrue(fields.getMessage(), fields.getMessage().contains("[overrides.body.fields] sub-field name [raw name]"));

        IllegalArgumentException dotted = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(null, Map.of("body", Map.of("raw.", Map.of("type", "keyword"))))
        );
        assertTrue(dotted.getMessage(), dotted.getMessage().contains("must not contain [.]"));

        IllegalArgumentException control = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(null, Map.of("body", Map.of("raw\tname", Map.of("type", "keyword"))))
        );
        assertTrue(control.getMessage(), control.getMessage().contains("whitespace or control characters"));

        IllegalArgumentException empty = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(null, Map.of("body", Map.of("", Map.of("type", "keyword"))))
        );
        assertTrue(empty.getMessage(), empty.getMessage().contains("must be non-empty strings"));
    }

    public void testSubFieldNameWithEveryKindOfWhitespaceRejected() {
        // Character.isWhitespace covers more than the space and the tab:
        // a line feed, a carriage return and the Unicode space
        // separators outside ASCII are refused with the same message, in
        // both clauses.
        for (String name : List.of("raw\n", "raw\rname", "raw\u3000name", "raw\u2028name", "\u2003raw")) {
            IllegalArgumentException legacy = expectThrows(
                IllegalArgumentException.class,
                () -> LanceOverrides.parseAttachClauses(null, Map.of("body", Map.of(name, Map.of("type", "keyword"))))
            );
            assertTrue(legacy.getMessage(), legacy.getMessage().contains("[multi_fields.body] sub-field name [" + name + "]"));
            assertTrue(legacy.getMessage(), legacy.getMessage().contains("must not contain whitespace or control characters"));

            IllegalArgumentException fields = expectThrows(
                IllegalArgumentException.class,
                () -> LanceOverrides.parseAttachClauses(Map.of("body", Map.of("fields", Map.of(name, Map.of("type", "keyword")))), null)
            );
            assertTrue(fields.getMessage(), fields.getMessage().contains("[overrides.body.fields] sub-field name [" + name + "]"));
            assertTrue(fields.getMessage(), fields.getMessage().contains("must not contain whitespace or control characters"));
        }
    }

    public void testParseRejectsAStoredSubFieldNameWithWhitespace() {
        // The canonical JSON of the overrides setting goes through the
        // same name rule on every shard open, so an index attached
        // before the rule existed with such a name fails to open with
        // the message the attach path gives today.
        String stored = "{\"body\":{\"fields\":{\"raw name\":{\"type\":\"keyword\"}}}}";
        IllegalArgumentException parsed = expectThrows(IllegalArgumentException.class, () -> LanceOverrides.parse(stored));
        assertEquals(
            "[overrides.body.fields] sub-field name [raw name] must not contain whitespace or control characters",
            parsed.getMessage()
        );

        Settings settings = Settings.builder().put(LanceEngineFactory.OVERRIDES_SETTING, stored).build();
        IllegalArgumentException fromSettings = expectThrows(IllegalArgumentException.class, () -> LanceOverrides.of(settings));
        assertEquals(parsed.getMessage(), fromSettings.getMessage());
    }

    public void testSubFieldNameWithOrdinaryCharactersAccepted() {
        // Letters, digits, underscore and hyphen are what a mapping
        // field name ordinarily carries; both clauses accept them and
        // the name round trips through the canonical JSON unchanged.
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            Map.of("title", Map.of("fields", Map.of("Raw_Keyword-2", Map.of("type", "keyword")))),
            Map.of("body", Map.of("raw", Map.of("type", "keyword")))
        );
        assertEquals("keyword", overrides.subFields().get("title").get("Raw_Keyword-2"));
        assertEquals("keyword", overrides.subFields().get("body").get("raw"));
        assertEquals(overrides, LanceOverrides.parse(overrides.toJson()));
    }

    public void testUnknownKeyRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("ts", Map.of("tokenizer", "kuromoji")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("unknown key [tokenizer]"));
        assertTrue(e.getMessage(), e.getMessage().contains("[type], [format], [order], [fields]"));
    }

    public void testUnknownTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("col", Map.of("type", "text")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("type=text"));
        assertTrue(e.getMessage(), e.getMessage().contains("[date], [keyword], [ip], [wildcard], [geo_point], [lance_text]"));
    }

    public void testLanceTextTypeParsesAndReportsThroughLanceTextColumns() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            Map.of("body", Map.of("type", "lance_text", "fields", Map.of("raw", Map.of("type", "keyword")))),
            null
        );
        assertEquals(Set.of("body"), overrides.lanceTextColumns());
        // One `type` per column: a lance_text column is in none of the
        // other type sets, which is what keeps it off the keyword,
        // wildcard and ip paths.
        assertTrue(overrides.keywordColumns().isEmpty());
        assertTrue(overrides.wildcardColumns().isEmpty());
        assertTrue(overrides.ipColumns().isEmpty());
        assertEquals("keyword", overrides.subFields().get("body").get("raw"));
        LanceOverrides restored = LanceOverrides.parse(overrides.toJson());
        assertEquals(overrides, restored);
        assertEquals(Set.of("body"), restored.lanceTextColumns());
    }

    public void testLanceTextTypeTakesNoFormatOrOrder() {
        IllegalArgumentException format = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("body", Map.of("type", "lance_text", "format", "epoch_millis")), null)
        );
        assertTrue(format.getMessage(), format.getMessage().contains("only accepted together with [type: date]"));
        IllegalArgumentException order = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("body", Map.of("type", "lance_text", "order", "lat_lon")), null)
        );
        assertTrue(order.getMessage(), order.getMessage().contains("only accepted together with [type: geo_point]"));
    }

    public void testIpTypeParsesAndReportsThroughIpColumns() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(Map.of("addr", Map.of("type", "ip")), null);
        assertEquals(java.util.Set.of("addr"), overrides.ipColumns());
        assertTrue(overrides.keywordColumns().isEmpty());
        assertEquals(overrides, LanceOverrides.parse(overrides.toJson()));
    }

    public void testWildcardTypeParsesAndReportsThroughWildcardColumns() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(Map.of("path", Map.of("type", "wildcard")), null);
        assertEquals(java.util.Set.of("path"), overrides.wildcardColumns());
        assertTrue(overrides.keywordColumns().isEmpty());
        assertEquals(overrides, LanceOverrides.parse(overrides.toJson()));
    }

    public void testGeoPointTypeParsesAndReportsThroughGeoPointColumns() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(Map.of("loc", Map.of("type", "geo_point")), null);
        assertEquals(java.util.Set.of("loc"), overrides.geoPointColumns().keySet());
        assertNull("no order declared", overrides.geoPointColumns().get("loc"));
        assertEquals(overrides, LanceOverrides.parse(overrides.toJson()));
    }

    public void testGeoPointOrderParsesAndRoundTrips() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(Map.of("loc", Map.of("type", "geo_point", "order", "lon_lat")), null);
        assertEquals("lon_lat", overrides.geoPointColumns().get("loc"));
        // Persist and re-read; the order must survive the round trip.
        assertEquals(overrides, LanceOverrides.parse(overrides.toJson()));
    }

    public void testGeoPointOrderMustBeKnown() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("loc", Map.of("type", "geo_point", "order", "xy")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("order=xy"));
    }

    public void testOrderWithoutGeoPointTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("col", Map.of("type", "date", "order", "lat_lon")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("only accepted together with [type: geo_point]"));
    }

    public void testFormatWithWildcardTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("path", Map.of("type", "wildcard", "format", "epoch_millis")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("only accepted together with [type: date]"));
    }

    public void testFormatWithIpTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("addr", Map.of("type", "ip", "format", "epoch_millis")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("only accepted together with [type: date]"));
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

    public void testWithRenamedColumnsMovesKeyAndKeepsOthers() {
        LanceOverrides overrides = LanceOverrides.parse(
            "{\"ts\":{\"type\":\"date\",\"format\":\"epoch_millis\"},"
                + "\"label\":{\"type\":\"keyword\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}},"
                + "\"other\":{\"type\":\"keyword\"}}"
        );
        LanceOverrides renamed = overrides.withRenamedColumns(Map.of("ts", "event_ts", "label", "tag"));
        assertEquals(List.of("event_ts", "tag", "other"), List.copyOf(renamed.columns().keySet()));
        // The rules travel with the key untouched.
        assertEquals("epoch_millis", renamed.columns().get("event_ts").format());
        assertEquals("keyword", renamed.columns().get("tag").subFields().get("raw"));
        assertEquals("keyword", renamed.columns().get("other").type());
    }

    public void testWithRenamedColumnsUnmatchedRenamesAreNoOp() {
        LanceOverrides overrides = LanceOverrides.parse("{\"ts\":{\"type\":\"date\"}}");
        assertSame(overrides, overrides.withRenamedColumns(Map.of("absent", "elsewhere")));
        assertSame(overrides, overrides.withRenamedColumns(Map.of()));
        assertSame(LanceOverrides.EMPTY, LanceOverrides.EMPTY.withRenamedColumns(Map.of("a", "b")));
    }

    public void testWithRenamedColumnsKeepsExplicitTargetDeclaration() {
        // The operator declared rules for both names; the renamed entry
        // folds away instead of clobbering the explicit target.
        LanceOverrides overrides = LanceOverrides.parse("{\"ts\":{\"type\":\"date\"},\"event_ts\":{\"type\":\"keyword\"}}");
        LanceOverrides renamed = overrides.withRenamedColumns(Map.of("ts", "event_ts"));
        assertEquals(List.of("event_ts"), List.copyOf(renamed.columns().keySet()));
        assertEquals("keyword", renamed.columns().get("event_ts").type());
    }

    public void testWithoutColumnDropsOnlyTheNamedEntry() {
        LanceOverrides overrides = LanceOverrides.parse("{\"ts\":{\"type\":\"date\"},\"label\":{\"type\":\"keyword\"}}");
        LanceOverrides pruned = overrides.withoutColumn("ts");
        assertEquals(List.of("label"), List.copyOf(pruned.columns().keySet()));
        assertSame(overrides, overrides.withoutColumn("absent"));
        assertSame(LanceOverrides.EMPTY, pruned.withoutColumn("label"));
    }
}

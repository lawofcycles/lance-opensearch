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
            () -> LanceOverrides.parseAttachClauses(Map.of("ts", Map.of("tokenizer", "kuromoji")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("unknown key [tokenizer]"));
        assertTrue(e.getMessage(), e.getMessage().contains("[type], [format], [order], [analyzer], [derived_column_name], [fields]"));
    }

    public void testTextAnalyzerParsesAndRoundTrips() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english")),
            null
        );
        LanceOverrides.Column column = overrides.textAnalyzerColumns().get("body");
        assertNotNull(column);
        assertEquals("english", column.analyzer());
        assertNull(column.derivedColumn());
        assertEquals("body__lance_tokens", LanceOverrides.derivedColumnName("body", column));
        assertTrue(overrides.keywordColumns().isEmpty());
        // Persist and re-read; the analyzer must survive the round trip.
        LanceOverrides restored = LanceOverrides.parse(overrides.toJson());
        assertEquals(overrides, restored);
        assertEquals("english", restored.textAnalyzerColumns().get("body").analyzer());
    }

    public void testTextAnalyzerDerivedColumnNameRoundTrips() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            Map.of("body", Map.of("type", "text_analyzer", "analyzer", "standard", "derived_column_name", "body_tokens")),
            null
        );
        LanceOverrides.Column column = overrides.textAnalyzerColumns().get("body");
        assertEquals("body_tokens", column.derivedColumn());
        assertEquals("body_tokens", LanceOverrides.derivedColumnName("body", column));
        LanceOverrides restored = LanceOverrides.parse(overrides.toJson());
        assertEquals(overrides, restored);
        assertEquals("body_tokens", restored.textAnalyzerColumns().get("body").derivedColumn());
    }

    public void testTextAnalyzerRequiresAnalyzer() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("body", Map.of("type", "text_analyzer")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("requires an [analyzer]"));
    }

    public void testAnalyzerWithoutTextAnalyzerTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("body", Map.of("type", "keyword", "analyzer", "english")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("only accepted together with [type: text_analyzer]"));
        IllegalArgumentException noType = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("body", Map.of("analyzer", "english")), null)
        );
        assertTrue(noType.getMessage(), noType.getMessage().contains("only accepted together with [type: text_analyzer]"));
    }

    public void testDerivedColumnNameWithoutTextAnalyzerTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("body", Map.of("type", "keyword", "derived_column_name", "tokens")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("only accepted together with [type: text_analyzer]"));
    }

    public void testDerivedColumnNameMustDifferFromBase() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(
                Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english", "derived_column_name", "body")),
                null
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("must differ from the column"));
    }

    public void testTextAnalyzerKeepsSubFields() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english", "fields", Map.of("raw", Map.of("type", "keyword")))),
            null
        );
        assertEquals("keyword", overrides.subFields().get("body").get("raw"));
        assertEquals("english", overrides.textAnalyzerColumns().get("body").analyzer());
        assertEquals(overrides, LanceOverrides.parse(overrides.toJson()));
    }

    public void testUnknownTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("col", Map.of("type", "text")), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("type=text"));
        assertTrue(e.getMessage(), e.getMessage().contains("[date], [keyword], [ip], [wildcard], [geo_point]"));
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

    public void testIndexesClauseAcceptsEveryScalarAndVectorType() {
        for (String scalar : LanceOverrides.SCALAR_INDEX_TYPES.keySet()) {
            LanceOverrides overrides = LanceOverrides.parseAttachClauses(null, null, Map.of("col", Map.of("scalar", scalar)));
            assertEquals(scalar, overrides.indexPreferences().get("col").scalar());
            assertNull(overrides.indexPreferences().get("col").vector());
        }
        for (String vector : LanceOverrides.VECTOR_INDEX_TYPES.keySet()) {
            LanceOverrides overrides = LanceOverrides.parseAttachClauses(null, null, Map.of("vec", Map.of("vector", vector)));
            assertEquals(vector, overrides.indexPreferences().get("vec").vector());
            assertNull(overrides.indexPreferences().get("vec").scalar());
        }
        LanceOverrides scalarNone = LanceOverrides.parseAttachClauses(null, null, Map.of("col", Map.of("scalar", "none")));
        assertEquals("none", scalarNone.indexPreferences().get("col").scalar());
        LanceOverrides vectorNone = LanceOverrides.parseAttachClauses(null, null, Map.of("vec", Map.of("vector", "none")));
        assertEquals("none", vectorNone.indexPreferences().get("vec").vector());
    }

    public void testIndexesClauseAcceptsParamsOfTheChosenType() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            null,
            null,
            Map.of(
                "flags",
                Map.of("scalar", "bloomfilter", "params", Map.of("number_of_items", 100_000, "probability", 0.01)),
                "vec",
                Map.of("vector", "ivf_hnsw_sq", "params", Map.of("num_partitions", 4, "m", 16, "ef_construction", 100))
            )
        );
        LanceOverrides.IndexPreference flags = overrides.indexPreferences().get("flags");
        assertEquals(100_000, flags.params().get("number_of_items").intValue());
        assertEquals(0.01, flags.params().get("probability").doubleValue(), 0.0);
        LanceOverrides.IndexPreference vec = overrides.indexPreferences().get("vec");
        assertEquals(4, vec.params().get("num_partitions").intValue());
        assertEquals(16, vec.params().get("m").intValue());
        assertEquals(100, vec.params().get("ef_construction").intValue());
    }

    public void testIndexesClauseUnknownTypeRejected() {
        IllegalArgumentException scalar = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseIndexesClause(Map.of("col", Map.of("scalar", "hash")))
        );
        assertTrue(scalar.getMessage(), scalar.getMessage().contains("scalar=hash] is not supported"));
        assertTrue(scalar.getMessage(), scalar.getMessage().contains("btree"));
        IllegalArgumentException vector = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseIndexesClause(Map.of("col", Map.of("vector", "hnsw")))
        );
        assertTrue(vector.getMessage(), vector.getMessage().contains("vector=hnsw] is not supported"));
        assertTrue(vector.getMessage(), vector.getMessage().contains("ivf_pq"));
    }

    public void testIndexesClauseUnknownKeyRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseIndexesClause(Map.of("col", Map.of("scalar", "btree", "replace", true)))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("unknown key [replace]"));
    }

    public void testIndexesClauseUnknownParamRejectedNamingAcceptedKeys() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseIndexesClause(Map.of("col", Map.of("scalar", "zonemap", "params", Map.of("zone_size", 1024))))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("unknown key [zone_size]"));
        assertTrue(e.getMessage(), e.getMessage().contains("rows_per_zone"));
        IllegalArgumentException noParams = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseIndexesClause(Map.of("col", Map.of("scalar", "bitmap", "params", Map.of("shard_id", 1))))
        );
        assertTrue(noParams.getMessage(), noParams.getMessage().contains("[bitmap] accepts no params"));
    }

    public void testIndexesClauseNonNumericParamRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseIndexesClause(Map.of("col", Map.of("scalar", "zonemap", "params", Map.of("rows_per_zone", "many"))))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("must be a number"));
    }

    public void testIndexesClauseBothKindsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseIndexesClause(Map.of("col", Map.of("scalar", "btree", "vector", "ivf_pq")))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("declares both [scalar] and [vector]"));
    }

    public void testIndexesClauseNeitherKindRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseIndexesClause(Map.of("col", Map.of("params", Map.of("num_partitions", 2))))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("must declare one of [scalar], [vector]"));
    }

    public void testIndexesClauseParamsWithNoneRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseIndexesClause(Map.of("col", Map.of("scalar", "none", "params", Map.of("zone_size", 1))))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("not accepted together with [none]"));
    }

    public void testIndexesClauseNonObjectRejected() {
        IllegalArgumentException clause = expectThrows(IllegalArgumentException.class, () -> LanceOverrides.parseIndexesClause("btree"));
        assertTrue(clause.getMessage(), clause.getMessage().contains("[indexes] must be an object"));
        IllegalArgumentException entry = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseIndexesClause(Map.of("col", "btree"))
        );
        assertTrue(entry.getMessage(), entry.getMessage().contains("[indexes.col] must be an object"));
    }

    public void testOverridesColumnNamedIndexesRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceOverrides.parseAttachClauses(Map.of("indexes", Map.of("type", "keyword")), null, null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[indexes] is reserved"));
    }

    public void testJsonRoundTripKeepsIndexPreferences() {
        LinkedHashMap<String, Object> indexes = new LinkedHashMap<>();
        indexes.put("rating", Map.of("scalar", "zonemap", "params", Map.of("rows_per_zone", 4096)));
        indexes.put("category", Map.of("scalar", "bitmap"));
        indexes.put("flag", Map.of("scalar", "none"));
        indexes.put("embedding", Map.of("vector", "ivf_flat", "params", Map.of("num_partitions", 2)));
        LanceOverrides original = LanceOverrides.parseAttachClauses(Map.of("label", Map.of("type", "keyword")), null, indexes);
        LanceOverrides restored = LanceOverrides.parse(original.toJson());
        assertEquals(original, restored);
        assertEquals(List.of("rating", "category", "flag", "embedding"), List.copyOf(restored.indexPreferences().keySet()));
        assertEquals("zonemap", restored.indexPreferences().get("rating").scalar());
        assertEquals(4096L, restored.indexPreferences().get("rating").params().get("rows_per_zone").longValue());
        assertEquals("none", restored.indexPreferences().get("flag").scalar());
        assertEquals("ivf_flat", restored.indexPreferences().get("embedding").vector());
        assertEquals(Set.of("label"), restored.keywordColumns());
    }

    public void testIndexPreferencesAloneRoundTripThroughTheSetting() {
        LanceOverrides original = LanceOverrides.parseAttachClauses(null, null, Map.of("rating", Map.of("scalar", "bitmap")));
        assertFalse(original.isEmpty());
        Settings settings = Settings.builder().put(LanceEngineFactory.OVERRIDES_SETTING, original.toJson()).build();
        LanceOverrides restored = LanceOverrides.of(settings);
        assertEquals(original, restored);
        assertEquals("bitmap", restored.indexPreferences().get("rating").scalar());
        assertTrue(restored.columns().isEmpty());
    }

    public void testIndexPreferencesJsonHelpersRoundTrip() {
        LinkedHashMap<String, LanceOverrides.IndexPreference> preferences = LanceOverrides.parseIndexesClause(
            Map.of("rating", Map.of("scalar", "bloomfilter", "params", Map.of("number_of_items", 10, "probability", 0.05)))
        );
        String json = LanceOverrides.indexPreferencesToJson(preferences);
        Map<String, LanceOverrides.IndexPreference> restored = LanceOverrides.indexPreferencesFromJson(json);
        assertEquals(preferences, restored);
        assertEquals("", LanceOverrides.indexPreferencesToJson(Map.of()));
        assertTrue(LanceOverrides.indexPreferencesFromJson("").isEmpty());
        assertTrue(LanceOverrides.indexPreferencesFromJson(null).isEmpty());
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

    public void testWithRenamedColumnsMovesIndexPreferenceKeys() {
        LanceOverrides overrides = LanceOverrides.parse(
            "{\"label\":{\"type\":\"keyword\"}," + "\"indexes\":{\"label\":{\"scalar\":\"bitmap\"},\"rating\":{\"scalar\":\"btree\"}}}"
        );
        LanceOverrides renamed = overrides.withRenamedColumns(Map.of("label", "tag"));
        assertEquals(List.of("tag"), List.copyOf(renamed.columns().keySet()));
        assertEquals(List.of("tag", "rating"), List.copyOf(renamed.indexPreferences().keySet()));
        assertEquals("bitmap", renamed.indexPreferences().get("tag").scalar());
        assertEquals("btree", renamed.indexPreferences().get("rating").scalar());
    }

    public void testWithoutColumnDropsOnlyTheNamedEntry() {
        LanceOverrides overrides = LanceOverrides.parse("{\"ts\":{\"type\":\"date\"},\"label\":{\"type\":\"keyword\"}}");
        LanceOverrides pruned = overrides.withoutColumn("ts");
        assertEquals(List.of("label"), List.copyOf(pruned.columns().keySet()));
        assertSame(overrides, overrides.withoutColumn("absent"));
        assertSame(LanceOverrides.EMPTY, pruned.withoutColumn("label"));
    }

    public void testWithoutColumnKeepsTheIndexPreference() {
        LanceOverrides overrides = LanceOverrides.parse(
            "{\"label\":{\"type\":\"keyword\"},\"indexes\":{\"label\":{\"scalar\":\"bitmap\"}}}"
        );
        LanceOverrides pruned = overrides.withoutColumn("label");
        assertTrue(pruned.columns().isEmpty());
        assertEquals("bitmap", pruned.indexPreferences().get("label").scalar());
    }
}

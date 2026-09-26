/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.lance.Dataset;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceTextAnalyzerBackfill;
import org.opensearch.lance.engine.LanceIndexBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Mapping derivation for Arrow Struct columns: a struct maps to an
 * {@code object} field whose {@code properties} carry the supported
 * children (recursing into nested structs), an unsupported child is
 * skipped with a note while the parent object is still emitted, and
 * struct children stay out of the index-eligible column sets (Lance
 * FTS / scalar / vector index builds target top-level columns).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RestAttachActionDeriveTests extends OpenSearchTestCase {

    public void testStructColumnDerivesObjectMapping() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeStructTable(scratchDir, "derive-" + getTestName().toLowerCase(Locale.ROOT), 0);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset);

            String mapping = derivation.mappingJson();
            assertTrue("meta must map as object: " + mapping, mapping.contains("\"meta\":{\"type\":\"object\",\"properties\":{"));
            assertTrue("region must map as keyword: " + mapping, mapping.contains("\"region\":{\"type\":\"keyword\""));
            assertTrue("score must map as double: " + mapping, mapping.contains("\"score\":{\"type\":\"double\""));
            assertTrue("nested flags must map as object: " + mapping, mapping.contains("\"flags\":{\"type\":\"object\",\"properties\":{"));
            assertTrue("active must map as boolean: " + mapping, mapping.contains("\"active\":{\"type\":\"boolean\""));
            // Children are doc-values-only fields, like top-level scalars.
            assertTrue(
                "children must carry index:false doc_values:true: " + mapping,
                mapping.contains("\"region\":{\"type\":\"keyword\",\"meta\":{\"lance_field_id\"")
            );
            // The unsupported uint32 child stays out of the mapping and
            // leaves a note naming its dotted path; the parent object is
            // emitted regardless.
            assertFalse("uint32 child must not be mapped: " + mapping, mapping.contains("\"raw\""));
            assertTrue(
                "expected a skip note for meta.raw, saw: " + derivation.notes(),
                derivation.notes().stream().anyMatch(note -> note.startsWith("meta.raw:"))
            );
            // A nested struct with no supported descendants is skipped
            // whole with one note instead of surfacing as an empty
            // object, matching the reader, which keeps such a struct out
            // of the row take and therefore out of _source.
            assertFalse("all-unsupported nested struct must not be mapped: " + mapping, mapping.contains("\"audit\""));
            assertFalse("no empty properties object may be emitted: " + mapping, mapping.contains("\"properties\":{}"));
            assertTrue(
                "expected a skip note for meta.audit, saw: " + derivation.notes(),
                derivation.notes().contains("meta.audit: Struct with no supported children, not surfaced")
            );

            assertEquals("id", derivation.keyField());
            // Struct children are not index-eligible columns: build_indexes
            // and the auto builder keep targeting top-level columns only.
            assertTrue("ftsColumns must be empty: " + derivation.ftsColumns(), derivation.ftsColumns().isEmpty());
            assertEquals("scalarColumns must hold the PK only: " + derivation.scalarColumns(), Set.of("id"), derivation.scalarColumns());
            assertTrue("vectorColumns must be empty: " + derivation.vectorColumns(), derivation.vectorColumns().isEmpty());
        }
    }

    public void testListOfStructDerivesNestedMapping() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeNestedTable(scratchDir, "derive-" + getTestName().toLowerCase(Locale.ROOT), 0);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset);

            String mapping = derivation.mappingJson();
            assertTrue("items must map as nested: " + mapping, mapping.contains("\"items\":{\"type\":\"nested\",\"properties\":{"));
            assertTrue("color must map as keyword: " + mapping, mapping.contains("\"color\":{\"type\":\"keyword\""));
            assertTrue("size must map as keyword: " + mapping, mapping.contains("\"size\":{\"type\":\"keyword\""));
            assertTrue("qty must map as integer: " + mapping, mapping.contains("\"qty\":{\"type\":\"integer\""));
            assertEquals("id", derivation.keyField());
            assertEquals(Set.of("items"), derivation.nestedColumns());
            // Nested children stay out of the index-eligible column sets.
            assertTrue("ftsColumns must be empty: " + derivation.ftsColumns(), derivation.ftsColumns().isEmpty());
            assertFalse("children must not be scalar columns: " + derivation.scalarColumns(), derivation.scalarColumns().contains("items"));
        }
    }

    /** Overrides parsed the way the REST layer parses an attach body clause. */
    private static LanceOverrides overrides(Map<String, Object> body) {
        return LanceOverrides.parseAttachClauses(body, null);
    }

    private String epochMillisTable() throws Exception {
        Path scratchDir = createTempDir();
        return LanceTableFactory.writeEpochMillisTable(scratchDir, "derive-" + getTestName().toLowerCase(Locale.ROOT));
    }

    public void testDateOverrideOnInt64DerivesDateMapping() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, overrides(Map.of("ts", Map.of("type", "date"))));
            String mapping = derivation.mappingJson();
            assertTrue("ts must map as date: " + mapping, mapping.contains("\"ts\":{\"type\":\"date\""));
            assertTrue("default format must be epoch_millis: " + mapping, mapping.contains("\"format\":\"epoch_millis\""));
            assertTrue("meta must keep the real Arrow type: " + mapping, mapping.contains("\"lance_arrow_type\":\"Int(64, true)\""));
            assertTrue("ts stays a scalar column: " + derivation.scalarColumns(), derivation.scalarColumns().contains("ts"));
            assertTrue("overrides JSON must persist: " + derivation.overridesJson(), derivation.overridesJson().contains("\"ts\""));
        }
    }

    public void testDateOverrideEmitsDeclaredFormat() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("ts", Map.of("type", "date", "format", "strict_date_optional_time||epoch_millis")))
            );
            assertTrue(
                derivation.mappingJson(),
                derivation.mappingJson().contains("\"format\":\"strict_date_optional_time||epoch_millis\"")
            );
        }
    }

    public void testDateOverrideOnTimestampColumnPinsTheDerivedType() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeDatedTable(scratchDir, "derive-" + getTestName().toLowerCase(Locale.ROOT));
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, overrides(Map.of("ts", Map.of("type", "date"))));
            assertTrue(derivation.mappingJson(), derivation.mappingJson().contains("\"ts\":{\"type\":\"date\""));
        }
    }

    public void testDateOverrideOnUtf8Rejected() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("label", Map.of("type", "date"))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("needs a signed 32 or 64 bit integer"));
            assertTrue(e.getMessage(), e.getMessage().contains("label"));
        }
    }

    public void testKeywordOverrideOnInvertedIndexColumnDerivesKeyword() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("label", Map.of("type", "keyword")))
            );
            String mapping = derivation.mappingJson();
            assertTrue("label must map as keyword: " + mapping, mapping.contains("\"label\":{\"type\":\"keyword\""));
            assertFalse("label must not map as lance_text: " + mapping, mapping.contains("lance_text"));
            // Out of ftsColumns so the index build paths do not create or
            // optimise an FTS index for it.
            assertFalse("label must leave ftsColumns: " + derivation.ftsColumns(), derivation.ftsColumns().contains("label"));
            assertTrue("label joins scalarColumns: " + derivation.scalarColumns(), derivation.scalarColumns().contains("label"));
        }
    }

    public void testKeywordOverrideOnNonUtf8Rejected() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("ts", Map.of("type", "keyword"))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("needs a Utf8 or List<Utf8> column"));
        }
    }

    public void testOverrideOnUnknownColumnRejected() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("nope", Map.of("type", "date"))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("unknown column [nope]"));
        }
    }

    public void testOverrideOnPrimaryKeyColumnRejected() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeStructTable(scratchDir, "derive-" + getTestName().toLowerCase(Locale.ROOT), 0);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("id", Map.of("type", "date"))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("primary key"));
        }
    }

    public void testLenientDeriveSkipsUnknownColumnAndKeepsTheRest() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            LinkedHashMap<String, Object> body = new LinkedHashMap<>();
            body.put("nope", Map.of("type", "date"));
            body.put("ts", Map.of("type", "date"));
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, overrides(body), true);
            assertTrue(
                "ts override must still apply: " + derivation.mappingJson(),
                derivation.mappingJson().contains("\"ts\":{\"type\":\"date\"")
            );
            assertTrue(
                "a note must record the skip: " + derivation.notes(),
                derivation.notes().stream().anyMatch(note -> note.contains("override skipped"))
            );
            // The full list stays in the setting so the column picks the
            // override back up if a later manifest adds it.
            assertTrue(derivation.overridesJson(), derivation.overridesJson().contains("\"nope\""));
        }
    }

    public void testFieldsOverrideEmitsSubFieldBlock() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("label", Map.of("fields", Map.of("raw", Map.of("type", "keyword")))))
            );
            assertTrue(derivation.mappingJson(), derivation.mappingJson().contains("\"fields\":{\"raw\":{\"type\":\"keyword\""));
        }
    }

    public void testTypeAndFieldsTogetherApplyBoth() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("label", Map.of("type", "keyword", "fields", Map.of("raw", Map.of("type", "keyword")))))
            );
            String mapping = derivation.mappingJson();
            assertTrue(mapping, mapping.contains("\"label\":{\"type\":\"keyword\""));
            assertTrue(mapping, mapping.contains("\"fields\":{\"raw\":{\"type\":\"keyword\""));
        }
    }

    public void testFieldsOnNonUtf8Rejected() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("ts", Map.of("fields", Map.of("raw", Map.of("type", "keyword"))))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("must be Utf8"));
        }
    }

    public void testSubFieldTypeMustBeKeyword() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("label", Map.of("fields", Map.of("raw", Map.of("type", "text"))))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("must be [keyword]"));
        }
    }

    private String ipTable() throws Exception {
        Path scratchDir = createTempDir();
        return LanceTableFactory.writeIpTable(scratchDir, "derive-" + getTestName().toLowerCase(Locale.ROOT));
    }

    public void testIpOverrideOnUtf8DerivesIpMapping() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(ipTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, overrides(Map.of("ip", Map.of("type", "ip"))));
            String mapping = derivation.mappingJson();
            assertTrue("ip must map as ip: " + mapping, mapping.contains("\"ip\":{\"type\":\"ip\""));
            assertTrue("meta must keep the real Arrow type: " + mapping, mapping.contains("\"lance_arrow_type\":\"Utf8\""));
            assertTrue("doc values only, like every scalar: " + mapping, mapping.contains("\"index\":false,\"doc_values\":true"));
            assertFalse("ip must not join ftsColumns: " + derivation.ftsColumns(), derivation.ftsColumns().contains("ip"));
            assertTrue("ip joins scalarColumns: " + derivation.scalarColumns(), derivation.scalarColumns().contains("ip"));
        }
    }

    public void testIpOverrideOnListOfUtf8DerivesIpMapping() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(ipTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, overrides(Map.of("addrs", Map.of("type", "ip"))));
            String mapping = derivation.mappingJson();
            assertTrue("addrs must map as ip: " + mapping, mapping.contains("\"addrs\":{\"type\":\"ip\""));
            assertTrue("meta must keep the list shape: " + mapping, mapping.contains("\"lance_arrow_type\":\"list<utf8>\""));
        }
    }

    public void testIpOverrideWithKeywordSubFieldEmitsBoth() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(ipTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("ip", Map.of("type", "ip", "fields", Map.of("raw", Map.of("type", "keyword")))))
            );
            String mapping = derivation.mappingJson();
            assertTrue(mapping, mapping.contains("\"ip\":{\"type\":\"ip\""));
            assertTrue(mapping, mapping.contains("\"fields\":{\"raw\":{\"type\":\"keyword\""));
        }
    }

    public void testIpOverrideOnNonStringColumnRejected() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(ipTable(), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("id", Map.of("type", "ip"))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("needs a Utf8 or List<Utf8> column"));
            assertTrue(e.getMessage(), e.getMessage().contains("id"));
        }
    }

    public void testIpOverrideOnInvertedIndexColumnLeavesFts() throws Exception {
        // The label column of the epoch-millis fixture carries a Lance
        // inverted index; an ip override takes it off the FTS path like
        // a keyword override does.
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, overrides(Map.of("label", Map.of("type", "ip"))));
            String mapping = derivation.mappingJson();
            assertTrue("label must map as ip: " + mapping, mapping.contains("\"label\":{\"type\":\"ip\""));
            assertFalse("label must not map as lance_text: " + mapping, mapping.contains("lance_text"));
            assertFalse("label must leave ftsColumns: " + derivation.ftsColumns(), derivation.ftsColumns().contains("label"));
        }
    }

    public void testWildcardOverrideDerivesKeywordMappingWithIntentMeta() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(ipTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("path", Map.of("type", "wildcard")))
            );
            String mapping = derivation.mappingJson();
            assertTrue("path must map as keyword: " + mapping, mapping.contains("\"path\":{\"type\":\"keyword\""));
            assertTrue("meta must record the declared type: " + mapping, mapping.contains("\"lance_override_type\":\"wildcard\""));
            assertTrue("path joins scalarColumns: " + derivation.scalarColumns(), derivation.scalarColumns().contains("path"));
        }
    }

    public void testWildcardOverrideOnInvertedIndexColumnLeavesFts() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("label", Map.of("type", "wildcard")))
            );
            String mapping = derivation.mappingJson();
            assertTrue("label must map as keyword: " + mapping, mapping.contains("\"label\":{\"type\":\"keyword\""));
            assertFalse("label must not map as lance_text: " + mapping, mapping.contains("lance_text"));
            assertFalse("label must leave ftsColumns: " + derivation.ftsColumns(), derivation.ftsColumns().contains("label"));
        }
    }

    public void testWildcardOverrideOnNonUtf8Rejected() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(ipTable(), StorageOptions.empty())) {
            IllegalArgumentException onInt = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("id", Map.of("type", "wildcard"))))
            );
            assertTrue(onInt.getMessage(), onInt.getMessage().contains("needs a Utf8 column"));
            // Unlike ip, the multi-valued List<Utf8> shape is refused.
            IllegalArgumentException onList = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("addrs", Map.of("type", "wildcard"))))
            );
            assertTrue(onList.getMessage(), onList.getMessage().contains("needs a Utf8 column"));
        }
    }

    private String geoStructTable() throws Exception {
        Path scratchDir = createTempDir();
        return LanceTableFactory.writeGeoStructTable(scratchDir, "derive-" + getTestName().toLowerCase(Locale.ROOT));
    }

    private String geoFslTable(boolean latLonOrder) throws Exception {
        Path scratchDir = createTempDir();
        return LanceTableFactory.writeGeoFslTable(scratchDir, "derive-" + getTestName().toLowerCase(Locale.ROOT), latLonOrder);
    }

    public void testGeoPointOverrideOnStructDerivesGeoPointMapping() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(geoStructTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("location", Map.of("type", "geo_point")))
            );
            String mapping = derivation.mappingJson();
            assertTrue("location must map as geo_point: " + mapping, mapping.contains("\"location\":{\"type\":\"geo_point\""));
            assertTrue("meta must record the Arrow shape: " + mapping, mapping.contains("\"lance_arrow_type\":\"struct\""));
            assertTrue("meta must record the derived order: " + mapping, mapping.contains("\"lance_geo_order\":\"lat_lon\""));
            assertFalse(
                "children must not surface as an object mapping: " + mapping,
                mapping.contains("\"location\":{\"type\":\"object\"")
            );
            assertTrue("location joins scalarColumns: " + derivation.scalarColumns(), derivation.scalarColumns().contains("location"));
            assertTrue(
                "overrides JSON must persist: " + derivation.overridesJson(),
                derivation.overridesJson().contains("\"location\"") && derivation.overridesJson().contains("\"geo_point\"")
            );
        }
    }

    public void testGeoPointOverrideOnFslDerivesGeoPointMapping() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(geoFslTable(true), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("location", Map.of("type", "geo_point")))
            );
            String mapping = derivation.mappingJson();
            assertTrue("location must map as geo_point: " + mapping, mapping.contains("\"location\":{\"type\":\"geo_point\""));
            assertTrue("meta must record the Arrow shape: " + mapping, mapping.contains("\"lance_arrow_type\":\"fsl2f64\""));
            // Default order is lat_lon when the operator declares none.
            assertTrue("default order must be lat_lon: " + mapping, mapping.contains("\"lance_geo_order\":\"lat_lon\""));
            assertFalse("must not map as lance_vector: " + mapping, mapping.contains("lance_vector"));
            assertTrue("location joins scalarColumns: " + derivation.scalarColumns(), derivation.scalarColumns().contains("location"));
            assertFalse("must not join vectorColumns: " + derivation.vectorColumns(), derivation.vectorColumns().contains("location"));
        }
    }

    public void testGeoPointOverrideOnFslWithLonLatOrder() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(geoFslTable(false), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("location", Map.of("type", "geo_point", "order", "lon_lat")))
            );
            String mapping = derivation.mappingJson();
            assertTrue("meta must record the declared order: " + mapping, mapping.contains("\"lance_geo_order\":\"lon_lat\""));
        }
    }

    public void testGeoPointOverrideOrderOnStructRejected() throws Exception {
        // Struct child names fix the order — the operator cannot declare one.
        try (Dataset dataset = LanceRegistry.openDataset(geoStructTable(), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("location", Map.of("type", "geo_point", "order", "lat_lon"))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("only accepted on a FixedSizeList"));
        }
    }

    public void testGeoPointOverrideOnUnsupportedShapeRejected() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            // A signed integer column is not a valid geo_point shape.
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("ts", Map.of("type", "geo_point"))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("geo_point"));
            assertTrue(e.getMessage(), e.getMessage().contains("Struct with two Float64 children"));
        }
    }

    public void testGeoPointOverrideOrderRejectedForNonGeoType() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(epochMillisTable(), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("ts", Map.of("type", "date", "order", "lat_lon"))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("only accepted together with [type: geo_point]"));
        }
    }

    public void testGeoPointOverrideOrderMustBeKnownValue() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(geoFslTable(true), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("location", Map.of("type", "geo_point", "order", "xy"))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("order=xy"));
            assertTrue(e.getMessage(), e.getMessage().contains("lat_lon"));
        }
    }

    private String englishTextTable() throws Exception {
        Path scratchDir = createTempDir();
        return LanceTableFactory.writeEnglishTextTable(scratchDir, "derive-" + getTestName().toLowerCase(Locale.ROOT));
    }

    public void testTextAnalyzerPendingDerivesDefaultWithNote() throws Exception {
        // Before the backfill lands, the override derives the column by
        // the default rules (keyword: no FTS index on the raw column)
        // and records a pending note instead of failing, so the
        // derive: async attach and the namespace poll can both derive
        // while the backfill is in flight.
        try (Dataset dataset = LanceRegistry.openDataset(englishTextTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english")))
            );
            String mapping = derivation.mappingJson();
            assertTrue("body must fall back to keyword: " + mapping, mapping.contains("\"body\":{\"type\":\"keyword\""));
            assertFalse("no tokens_column before the backfill: " + mapping, mapping.contains("tokens_column"));
            assertTrue(
                "expected a pending note, saw: " + derivation.notes(),
                derivation.notes().stream().anyMatch(note -> note.contains("text_analyzer override pending"))
            );
            assertTrue("overrides JSON must persist: " + derivation.overridesJson(), derivation.overridesJson().contains("text_analyzer"));
        }
    }

    public void testTextAnalyzerStaysPendingUntilTheDerivedColumnIsIndexed() throws Exception {
        // The backfill commits the derived column first and its
        // inverted index second. Between the two commits the column
        // exists but a match on it would be a flat scan, so the
        // derivation keeps the base column on its default mapping and
        // says why; the index commit flips it.
        String uri = englishTextTable();
        LanceOverrides overrides = overrides(Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english")));
        LanceTableFactory.addColumnFromSql(uri, "body__lance_tokens", "lower(body)");
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            RestAttachAction.Derivation pending = RestAttachAction.derive(dataset, overrides);
            String mapping = pending.mappingJson();
            assertTrue("body must stay on its default mapping: " + mapping, mapping.contains("\"body\":{\"type\":\"keyword\""));
            assertFalse("no tokens_column before the index commit: " + mapping, mapping.contains("tokens_column"));
            assertFalse("derived column must not surface: " + mapping, mapping.contains("\"body__lance_tokens\":{"));
            assertTrue(
                "expected a pending note naming the missing index, saw: " + pending.notes(),
                pending.notes()
                    .stream()
                    .anyMatch(note -> note.contains("text_analyzer override pending") && note.contains("no inverted index"))
            );
            assertTrue(
                "derived column stays an FTS build target: " + pending.ftsColumns(),
                pending.ftsColumns().contains("body__lance_tokens")
            );

            LanceIndexBuilder.BuildResult build = LanceIndexBuilder.ensureVerbatimWhitespaceFtsIndexes(
                dataset,
                Set.of("body__lance_tokens")
            );
            assertTrue("index build must succeed: " + build.failed(), build.failed().isEmpty());
            RestAttachAction.Derivation flipped = RestAttachAction.derive(dataset, overrides);
            String flippedMapping = flipped.mappingJson();
            assertTrue(
                "body must map as lance_text with tokens_column once the index exists: " + flippedMapping,
                flippedMapping.contains("\"body\":{\"type\":\"lance_text\",\"tokens_column\":\"body__lance_tokens\"")
            );
            assertFalse(
                "no pending note once the index exists: " + flipped.notes(),
                flipped.notes().stream().anyMatch(note -> note.contains("text_analyzer override pending"))
            );
        }
    }

    public void testTextAnalyzerDerivesLanceTextWithTokensColumn() throws Exception {
        String uri = englishTextTable();
        LanceOverrides overrides = overrides(Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english")));
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            LanceTextAnalyzerBackfill.ensureDerivedColumns(
                dataset,
                overrides.textAnalyzerColumns(),
                name -> english,
                LanceRegistry.allocator(),
                LanceTextAnalyzerBackfill.Options.inline()
            );
            RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, overrides);
            String mapping = derivation.mappingJson();
            assertTrue(
                "body must map as lance_text with tokens_column: " + mapping,
                mapping.contains("\"body\":{\"type\":\"lance_text\",\"tokens_column\":\"body__lance_tokens\"")
            );
            assertTrue("meta must record the analyzer: " + mapping, mapping.contains("\"lance_analyzer\":\"english\""));
            assertTrue("meta must keep the Arrow type: " + mapping, mapping.contains("\"lance_arrow_type\":\"Utf8\""));
            // The derived column is plugin-managed: hidden from the
            // mapping, targeted by the FTS build and optimise paths.
            assertFalse("derived column must not surface: " + mapping, mapping.contains("\"body__lance_tokens\":{"));
            assertTrue(
                "derived column must be an FTS target: " + derivation.ftsColumns(),
                derivation.ftsColumns().contains("body__lance_tokens")
            );
            assertFalse("base column must not be an FTS target: " + derivation.ftsColumns(), derivation.ftsColumns().contains("body"));
            assertFalse(
                "base column must not be a scalar target: " + derivation.scalarColumns(),
                derivation.scalarColumns().contains("body")
            );
            assertTrue(
                "expected a not-surfaced note, saw: " + derivation.notes(),
                derivation.notes().stream().anyMatch(note -> note.startsWith("body__lance_tokens: derived tokens column"))
            );
        }
    }

    public void testTextAnalyzerOnNonUtf8Refused() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(englishTextTable(), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(dataset, overrides(Map.of("id", Map.of("type", "text_analyzer", "analyzer", "english"))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("type=text_analyzer] needs a Utf8 column"));
        }
    }

    public void testTextAnalyzerLenientSkipsNonUtf8WithNote() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(englishTextTable(), StorageOptions.empty())) {
            RestAttachAction.Derivation derivation = RestAttachAction.derive(
                dataset,
                overrides(Map.of("id", Map.of("type", "text_analyzer", "analyzer", "english"))),
                true
            );
            assertTrue(
                "expected a skip note, saw: " + derivation.notes(),
                derivation.notes().stream().anyMatch(note -> note.contains("override skipped"))
            );
            String mapping = derivation.mappingJson();
            assertTrue("id keeps its integer mapping: " + mapping, mapping.contains("\"id\":{\"type\":\"integer\""));
        }
    }

    public void testTextAnalyzerDerivedNameCollisionWithNonUtf8Refused() throws Exception {
        // derived_column_name names the Int32 id column: strict derive
        // refuses, naming the column and its type.
        try (Dataset dataset = LanceRegistry.openDataset(englishTextTable(), StorageOptions.empty())) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestAttachAction.derive(
                    dataset,
                    overrides(Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english", "derived_column_name", "id")))
                )
            );
            assertTrue(e.getMessage(), e.getMessage().contains("exists with Arrow type"));
        }
    }
}

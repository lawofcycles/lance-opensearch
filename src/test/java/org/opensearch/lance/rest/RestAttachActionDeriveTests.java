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

import org.lance.Dataset;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
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
}

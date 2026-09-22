/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;

import org.lance.Dataset;
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
        String uri = LanceTableFactory.writeStructTable(scratchDir, "derive-" + getTestName().toLowerCase(java.util.Locale.ROOT), 0);
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
            assertEquals(
                "scalarColumns must hold the PK only: " + derivation.scalarColumns(),
                java.util.Set.of("id"),
                derivation.scalarColumns()
            );
            assertTrue("vectorColumns must be empty: " + derivation.vectorColumns(), derivation.vectorColumns().isEmpty());
        }
    }
}

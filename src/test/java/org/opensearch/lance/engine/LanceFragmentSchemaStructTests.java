/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.lance.Dataset;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceFragmentSchema.ColumnKind;
import org.opensearch.lance.engine.LanceFragmentSchema.NumericPrecision;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Schema derivation for Arrow Struct columns: supported children join
 * {@link LanceFragmentSchema#columnKind()} under their dotted path
 * (recursing into nested structs), the struct parent joins the row
 * take under its own name so {@code _source} can render the object,
 * unsupported children are absent, and every dotted child carries a
 * Lucene {@code FieldInfo} with the doc value type of its kind.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFragmentSchemaStructTests extends OpenSearchTestCase {

    public void testStructChildrenClassifyUnderDottedPaths() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeStructTable(scratchDir, "schema-" + getTestName().toLowerCase(java.util.Locale.ROOT), 0);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceFragmentSchema schema = LanceFragmentSchema.derive(
                dataset,
                "id",
                LanceEngineFactory.LancePrimaryKeyType.LONG,
                Collections.emptyMap(),
                Collections.emptySet()
            );

            assertEquals(ColumnKind.NUMERIC, schema.columnKind().get("id"));
            assertEquals(ColumnKind.TEXT_KEYWORD, schema.columnKind().get("meta.region"));
            assertEquals(ColumnKind.NUMERIC, schema.columnKind().get("meta.score"));
            assertEquals(ColumnKind.BOOLEAN, schema.columnKind().get("meta.flags.active"));
            // The unsupported uint32 child and the struct parents carry no
            // column kind of their own, and a nested struct with no
            // supported descendants contributes nothing at all.
            assertNull(schema.columnKind().get("meta.raw"));
            assertNull(schema.columnKind().get("meta"));
            assertNull(schema.columnKind().get("meta.flags"));
            assertNull(schema.columnKind().get("meta.audit"));
            assertNull(schema.columnKind().get("meta.audit.checksum"));

            assertEquals(NumericPrecision.DOUBLE, schema.numericPrecision().get("meta.score"));

            // The take projects the whole struct under the parent name, in
            // schema order, and the parent is recorded as a struct column.
            assertEquals(List.of("id", "meta"), schema.takeColumns());
            assertEquals(2, schema.sourceColumnCount());
            assertEquals(Set.of("meta"), schema.structColumns());
            assertEquals(0, schema.pkTakeIndex());

            // Dotted children have FieldInfos so doc value consumers and
            // the FLS wrapper can address them by name.
            assertNotNull(schema.fieldInfos().fieldInfo("meta.region"));
            assertNotNull(schema.fieldInfos().fieldInfo("meta.score"));
            assertNotNull(schema.fieldInfos().fieldInfo("meta.flags.active"));
            assertNull(schema.fieldInfos().fieldInfo("meta"));
            assertNull(schema.fieldInfos().fieldInfo("meta.raw"));
        }
    }
}

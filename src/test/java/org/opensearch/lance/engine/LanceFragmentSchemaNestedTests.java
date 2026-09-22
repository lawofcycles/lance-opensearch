/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.lance.Dataset;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceFragmentSchema.ColumnKind;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Schema derivation for Arrow {@code List<Struct>} columns: the column
 * joins {@link LanceFragmentSchema#nestedColumns()}, its children
 * classify under dotted paths and map back to the column through
 * {@link LanceFragmentSchema#nestedChildToParent()}, and the field
 * infos gain the two synthetic entries the nested query machinery
 * reads ({@code _primary_term} numeric doc values for the parent
 * filter, {@code _nested_path} postings for the child filter).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFragmentSchemaNestedTests extends OpenSearchTestCase {

    public void testListOfStructClassifiesAsNested() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeNestedTable(scratchDir, "schema-" + getTestName().toLowerCase(Locale.ROOT), 0);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceFragmentSchema schema = LanceFragmentSchema.derive(
                dataset,
                "id",
                LanceEngineFactory.LancePrimaryKeyType.LONG,
                LanceOverrides.EMPTY,
                Collections.emptySet()
            );

            assertEquals(Set.of("items"), schema.nestedColumns());
            assertEquals(ColumnKind.TEXT_KEYWORD, schema.columnKind().get("items.color"));
            assertEquals(ColumnKind.TEXT_KEYWORD, schema.columnKind().get("items.size"));
            assertEquals(ColumnKind.NUMERIC, schema.columnKind().get("items.qty"));
            assertEquals("items", schema.nestedChildToParent().get("items.color"));
            assertEquals("items", schema.nestedChildToParent().get("items.qty"));
            assertNull(schema.columnKind().get("items"));

            // The take projects the whole list column so _source renders
            // the array; it is not a struct column.
            assertEquals(List.of("id", "title", "items"), schema.takeColumns());
            assertTrue(schema.structColumns().isEmpty());

            // FieldInfos: dotted children plus the two synthetic nested
            // machinery fields.
            assertNotNull(schema.fieldInfos().fieldInfo("items.color"));
            assertNotNull(schema.fieldInfos().fieldInfo("items.qty"));
            assertEquals(DocValuesType.NUMERIC, schema.fieldInfos().fieldInfo("_primary_term").getDocValuesType());
            assertEquals(IndexOptions.DOCS, schema.fieldInfos().fieldInfo("_nested_path").getIndexOptions());
            assertEquals(DocValuesType.NONE, schema.fieldInfos().fieldInfo("_nested_path").getDocValuesType());
        }
    }

    public void testTableWithoutNestedColumnsCarriesNoSyntheticFields() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeStructTable(scratchDir, "schema-" + getTestName().toLowerCase(Locale.ROOT), 0);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceFragmentSchema schema = LanceFragmentSchema.derive(
                dataset,
                "id",
                LanceEngineFactory.LancePrimaryKeyType.LONG,
                LanceOverrides.EMPTY,
                Collections.emptySet()
            );
            assertTrue(schema.nestedColumns().isEmpty());
            assertNull(schema.fieldInfos().fieldInfo("_primary_term"));
            assertNull(schema.fieldInfos().fieldInfo("_nested_path"));
        }
    }
}

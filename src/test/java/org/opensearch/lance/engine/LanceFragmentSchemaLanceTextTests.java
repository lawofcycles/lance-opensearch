/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.DocValuesType;
import org.lance.Dataset;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceFragmentSchema.ColumnKind;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Schema classification of a Utf8 column under a {@code type: lance_text}
 * override: the column classifies as {@link ColumnKind#TEXT_FTS} whether
 * or not the caller resolved an inverted index on it, where the same
 * column without the override and without an index is
 * {@link ColumnKind#TEXT_KEYWORD}.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFragmentSchemaLanceTextTests extends OpenSearchTestCase {

    private static LanceOverrides lanceText(String column) {
        return LanceOverrides.parseAttachClauses(Map.of(column, Map.of("type", "lance_text")), null);
    }

    public void testOverrideClassifiesAColumnWithoutAnIndexAsFullText() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeEnglishTextTable(scratchDir, "schema-" + getTestName().toLowerCase(Locale.ROOT));
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            // Nothing resolved an index on body: without the override the
            // column is keyword over doc values.
            LanceFragmentSchema plain = LanceFragmentSchema.derive(
                dataset,
                "",
                LanceEngineFactory.LancePrimaryKeyType.NONE,
                LanceOverrides.EMPTY,
                Collections.emptySet()
            );
            assertEquals(ColumnKind.TEXT_KEYWORD, plain.columnKind().get("body"));
            assertEquals(DocValuesType.SORTED_SET, plain.fieldInfos().fieldInfo("body").getDocValuesType());

            // With it the column is full text: no doc values, Lance
            // answers the full text scan.
            LanceFragmentSchema overridden = LanceFragmentSchema.derive(
                dataset,
                "",
                LanceEngineFactory.LancePrimaryKeyType.NONE,
                lanceText("body"),
                Collections.emptySet()
            );
            assertEquals(ColumnKind.TEXT_FTS, overridden.columnKind().get("body"));
            assertEquals(DocValuesType.NONE, overridden.fieldInfos().fieldInfo("body").getDocValuesType());

            // The same with the index resolved: the override and the
            // index agree, so the classification is the same.
            LanceFragmentSchema indexed = LanceFragmentSchema.derive(
                dataset,
                "",
                LanceEngineFactory.LancePrimaryKeyType.NONE,
                lanceText("body"),
                Set.of("body")
            );
            assertEquals(ColumnKind.TEXT_FTS, indexed.columnKind().get("body"));
        }
    }
}

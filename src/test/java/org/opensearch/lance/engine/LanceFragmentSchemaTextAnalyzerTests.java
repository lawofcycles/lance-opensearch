/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.lance.Dataset;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceFragmentSchema.ColumnKind;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Schema classification under a {@code type: text_analyzer} override:
 * the base column is served as {@link ColumnKind#TEXT_FTS} only once
 * the derived tokens column exists and carries its inverted index,
 * and the derived column itself is hidden as soon as it exists. The
 * classification follows the mapping derivation commit by commit, so
 * the reader never serves the base column through a derived column
 * that has no index yet.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFragmentSchemaTextAnalyzerTests extends OpenSearchTestCase {

    private static final LanceOverrides OVERRIDES = LanceOverrides.parseAttachClauses(
        Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english")),
        null
    );

    private static LanceFragmentSchema derive(Dataset dataset) throws Exception {
        Set<String> ftsColumns = LanceFragmentSchema.resolveFtsColumns(dataset);
        return LanceFragmentSchema.derive(dataset, "id", LanceEngineFactory.LancePrimaryKeyType.LONG, OVERRIDES, ftsColumns);
    }

    public void testBaseFlipsToFtsOnlyOnceTheDerivedColumnIsIndexed() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeEnglishTextTable(scratchDir, "schema-" + getTestName().toLowerCase(Locale.ROOT));

        // No derived column: the base is a plain keyword column (the
        // table carries no inverted index over it).
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceFragmentSchema schema = derive(dataset);
            assertEquals(ColumnKind.TEXT_KEYWORD, schema.columnKind().get("body"));
            assertNull(schema.columnKind().get("body__lance_tokens"));
        }

        // The derived column exists but has no index yet (the state
        // between the backfill's two commits): the base keeps its
        // keyword classification and the derived column is hidden.
        LanceTableFactory.addColumnFromSql(uri, "body__lance_tokens", "lower(body)");
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceFragmentSchema schema = derive(dataset);
            assertEquals(ColumnKind.TEXT_KEYWORD, schema.columnKind().get("body"));
            assertNull("the derived column is hidden as soon as it exists", schema.columnKind().get("body__lance_tokens"));
            assertFalse(schema.takeColumns().contains("body__lance_tokens"));

            // The index commit flips the base to the FTS path.
            LanceIndexBuilder.BuildResult build = LanceIndexBuilder.ensureVerbatimWhitespaceFtsIndexes(
                dataset,
                Set.of("body__lance_tokens")
            );
            assertTrue("index build must succeed: " + build.failed(), build.failed().isEmpty());
            LanceFragmentSchema flipped = derive(dataset);
            assertEquals(ColumnKind.TEXT_FTS, flipped.columnKind().get("body"));
            assertNull(flipped.columnKind().get("body__lance_tokens"));
        }
    }
}

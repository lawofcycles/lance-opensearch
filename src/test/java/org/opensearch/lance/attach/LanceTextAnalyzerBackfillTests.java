/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.schema.LanceField;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The text_analyzer backfill helper: token generation through a real
 * Lucene analyzer, the AddColumns commit that materialises the derived
 * column (null values stay null, values land in row order), the
 * whitespace FTS index over it, and the idempotency of a second pass.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceTextAnalyzerBackfillTests extends OpenSearchTestCase {

    public void testJoinTokensAppliesAnalyzer() throws Exception {
        try (Analyzer english = new EnglishAnalyzer()) {
            // The english analyzer drops stop words, lowercases and stems.
            assertEquals("dog run quickli", LanceTextAnalyzerBackfill.joinTokens(english, "body", "The dogs are running quickly"));
            assertEquals("", LanceTextAnalyzerBackfill.joinTokens(english, "body", "the and of"));
        }
        try (Analyzer whitespace = new WhitespaceAnalyzer()) {
            // The whitespace analyzer splits only; case is kept.
            assertEquals("Cats sleep", LanceTextAnalyzerBackfill.joinTokens(whitespace, "body", "Cats  sleep"));
        }
    }

    public void testEnsureDerivedColumnsBackfillsAndIndexes() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeEnglishTextTable(scratchDir, "backfill-" + getTestName().toLowerCase(Locale.ROOT));
        Map<String, LanceOverrides.Column> overrides = LanceOverrides.parseAttachClauses(
            Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english")),
            null
        ).textAnalyzerColumns();
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            long versionBefore = dataset.version();
            LanceTextAnalyzerBackfill.Ensured ensured = LanceTextAnalyzerBackfill.ensureDerivedColumns(
                dataset,
                overrides,
                name -> english,
                LanceRegistry.allocator()
            );
            assertEquals(List.of("body__lance_tokens"), ensured.created());
            assertTrue(ensured.existing().isEmpty());
            assertTrue("AddColumns must commit a new version", dataset.version() > versionBefore);

            // The schema now carries the derived Utf8 column.
            LanceField derived = null;
            for (LanceField field : dataset.getLanceSchema().fields()) {
                if (field.getName().equals("body__lance_tokens")) {
                    derived = field;
                }
            }
            assertNotNull("derived column must exist", derived);
            assertTrue(derived.getType() instanceof ArrowType.Utf8);

            // Values land in row order: id 0 gets the analyzed tokens of
            // sentence 0, the Arrow-null row stays null.
            String[] tokens = readDerivedByRow(dataset);
            assertEquals("dog run quickli through park", tokens[0]);
            assertEquals("dog run across wide field", tokens[1]);
            assertNull("null source row must stay null", tokens[4]);
            assertEquals("runner run marathon morn", tokens[5]);

            // The derived column carries an FTS-capable index.
            assertFalse(
                "derived column must carry an inverted index",
                dataset.describeIndices(new IndexCriteria.Builder().forColumn("body__lance_tokens").mustSupportFts(true).build()).isEmpty()
            );

            // A second pass is a no-op: the column and its index exist.
            LanceTextAnalyzerBackfill.Ensured again = LanceTextAnalyzerBackfill.ensureDerivedColumns(
                dataset,
                overrides,
                name -> english,
                LanceRegistry.allocator()
            );
            assertTrue(again.created().isEmpty());
            assertEquals(List.of("body__lance_tokens"), again.existing());
        }
    }

    public void testEnsureDerivedColumnsRefusesUnknownBaseColumn() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeEnglishTextTable(scratchDir, "backfill-" + getTestName().toLowerCase(Locale.ROOT));
        Map<String, LanceOverrides.Column> overrides = LanceOverrides.parseAttachClauses(
            Map.of("missing", Map.of("type", "text_analyzer", "analyzer", "english")),
            null
        ).textAnalyzerColumns();
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> LanceTextAnalyzerBackfill.ensureDerivedColumns(dataset, overrides, name -> english, LanceRegistry.allocator())
            );
            assertTrue(e.getMessage(), e.getMessage().contains("unknown column [missing]"));
        }
    }

    public void testEnsureDerivedColumnsRefusesNonUtf8Collision() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeEnglishTextTable(scratchDir, "backfill-" + getTestName().toLowerCase(Locale.ROOT));
        // derived_column_name points at the Int32 id column.
        Map<String, LanceOverrides.Column> overrides = LanceOverrides.parseAttachClauses(
            Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english", "derived_column_name", "id")),
            null
        ).textAnalyzerColumns();
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> LanceTextAnalyzerBackfill.ensureDerivedColumns(dataset, overrides, name -> english, LanceRegistry.allocator())
            );
            assertTrue(e.getMessage(), e.getMessage().contains("exists with Arrow type"));
        }
    }

    /** The derived column's values indexed by the row's {@code id}, null where the value is Arrow null. */
    private static String[] readDerivedByRow(Dataset dataset) throws Exception {
        String[] byId = new String[LanceTableFactory.ENGLISH_SENTENCES.length];
        ScanOptions options = new ScanOptions.Builder().columns(new ArrayList<>(Arrays.asList("id", "body__lance_tokens"))).build();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                IntVector ids = (IntVector) root.getVector("id");
                VarCharVector tokens = (VarCharVector) root.getVector("body__lance_tokens");
                for (int i = 0; i < root.getRowCount(); i++) {
                    byId[ids.get(i)] = tokens.isNull(i) ? null : new String(tokens.get(i), StandardCharsets.UTF_8);
                }
            }
        }
        return byId;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.index.Term;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link PrebuiltWeightQuery} hands its Weight back only to parents
 * the Weight can serve.
 */
public class PrebuiltWeightQueryTests extends OpenSearchTestCase {

    private Directory dir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                doc.add(new StringField("parity", i % 2 == 0 ? "even" : "odd", Field.Store.NO));
                writer.addDocument(doc);
            }
        }
    }

    @Override
    public void tearDown() throws Exception {
        dir.close();
        super.tearDown();
    }

    public void testHandsThePrebuiltWeightToAConjunctionAndToCount() throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            // The fragment searcher runs without a query cache; the stock
            // one would wrap no-scores Weights and hide the identity.
            searcher.setQueryCache(null);
            Weight prebuilt = searcher.createWeight(MatchAllDocsQuery.INSTANCE, ScoreMode.COMPLETE, 1f);
            PrebuiltWeightQuery query = new PrebuiltWeightQuery(prebuilt, ScoreMode.COMPLETE);
            // The shapes the executor builds: the hits page asks for
            // scores, IndexSearcher.count asks for none; both get the
            // one Weight back.
            assertSame(prebuilt, searcher.createWeight(query, ScoreMode.TOP_SCORES, 1f));
            assertSame(prebuilt, searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f));
            BooleanQuery conjunction = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("parity", "even")), BooleanClause.Occur.FILTER)
                .build();
            assertEquals(2, searcher.count(conjunction));
            assertEquals(2, searcher.search(conjunction, 10).totalHits.value());
        }
    }

    public void testRejectsABoostAndAScoringParentOfANoScoresWeight() throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            Weight complete = searcher.createWeight(MatchAllDocsQuery.INSTANCE, ScoreMode.COMPLETE, 1f);
            PrebuiltWeightQuery query = new PrebuiltWeightQuery(complete, ScoreMode.COMPLETE);
            IllegalArgumentException boosted = expectThrows(
                IllegalArgumentException.class,
                () -> searcher.createWeight(new BoostQuery(query, 2f), ScoreMode.COMPLETE, 1f)
            );
            assertTrue(boosted.getMessage(), boosted.getMessage().contains("boost"));

            Weight noScores = searcher.createWeight(MatchAllDocsQuery.INSTANCE, ScoreMode.COMPLETE_NO_SCORES, 1f);
            PrebuiltWeightQuery noScoresQuery = new PrebuiltWeightQuery(noScores, ScoreMode.COMPLETE_NO_SCORES);
            assertSame(noScores, searcher.createWeight(noScoresQuery, ScoreMode.COMPLETE_NO_SCORES, 1f));
            IllegalArgumentException scoring = expectThrows(
                IllegalArgumentException.class,
                () -> searcher.createWeight(noScoresQuery, ScoreMode.COMPLETE, 1f)
            );
            assertTrue(scoring.getMessage(), scoring.getMessage().contains("COMPLETE_NO_SCORES"));
        }
    }
}

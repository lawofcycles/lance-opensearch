/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link PlanExecutor#countLiveDocs} against a reader
 * whose {@code getLiveDocs()} hides documents that {@code numDocs()} still
 * counts. That is the shape the security plugin's DLS leaf reader has, and
 * it is why {@link IndexSearcher#count} with {@link MatchAllDocsQuery}
 * cannot be trusted under a reader wrapper.
 */
public class LiveDocsCountTests extends OpenSearchTestCase {

    public void testCountsLiveDocsNotNumDocsUnderInconsistentWrapper() throws IOException {
        int total = 7;
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < total; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                    writer.addDocument(doc);
                }
            }
            FixedBitSet visible = new FixedBitSet(total);
            visible.set(1);
            visible.set(4);
            visible.set(6);
            // Closing the wrapper closes the wrapped reader, so each variant opens its own.
            try (DirectoryReader wrapped = new LiveDocsOverridingReader(DirectoryReader.open(dir), visible)) {
                IndexSearcher searcher = new IndexSearcher(wrapped);
                // The shortcut reads numDocs, which the wrapper leaves unfiltered.
                assertEquals(total, searcher.count(MatchAllDocsQuery.INSTANCE));
                assertEquals(3L, PlanExecutor.countLiveDocs(wrapped.leaves()));
            }
            // A Bits that is not a FixedBitSet takes the per-doc loop.
            Bits evenOnly = new Bits() {
                @Override
                public boolean get(int index) {
                    return index % 2 == 0;
                }

                @Override
                public int length() {
                    return total;
                }
            };
            try (DirectoryReader wrapped = new LiveDocsOverridingReader(DirectoryReader.open(dir), evenOnly)) {
                assertEquals(4L, PlanExecutor.countLiveDocs(wrapped.leaves()));
            }
        }
    }

    public void testPlainReaderCountsEveryDoc() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(5L, PlanExecutor.countLiveDocs(reader.leaves()));
            }
        }
    }

    /** Replaces every leaf's liveDocs while leaving numDocs untouched. */
    private static final class LiveDocsOverridingReader extends FilterDirectoryReader {
        private final Bits liveDocs;

        LiveDocsOverridingReader(DirectoryReader in, Bits liveDocs) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FilterLeafReader(reader) {
                        @Override
                        public Bits getLiveDocs() {
                            return liveDocs;
                        }

                        @Override
                        public boolean hasDeletions() {
                            return true;
                        }

                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return null;
                        }

                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return null;
                        }
                    };
                }
            });
            this.liveDocs = liveDocs;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new LiveDocsOverridingReader(in, liveDocs);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }
}

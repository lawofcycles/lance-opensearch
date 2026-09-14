/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.lance.Dataset;
import org.lance.Fragment;

/** DirectoryReader whose leaves are Lance fragments. */
public final class LanceDirectoryReader extends DirectoryReader {

    private final IndexCommit commit;
    // The engine hands us a freshly opened Dataset when it builds a new reader,
    // so this reader takes ownership of it and closes it when the reader is
    // closed. Lucene's ReferenceManager releases the previous reader once the
    // last in-flight searcher completes, which is the point where we also want
    // to release the Lance native handle it was reading from.
    private final Dataset dataset;
    // Bridge to Lucene's cache lifecycle at the composite reader level. See the
    // matching field on LanceFragmentLeafReader for the rationale: OpenSearch's
    // request cache keys entries by IndexReader.CacheKey, and only Lucene's own
    // org.apache.lucene.index classes can construct one. Holding a one-doc
    // Lucene reader whose lifetime tracks this reader's lifetime lets us surface
    // a real CacheHelper without reimplementing Lucene's cache internals.
    private final DirectoryReader cacheLifetimeBridge;

    public static LanceDirectoryReader open(
        Directory directory,
        IndexCommit commit,
        Dataset dataset,
        String intField,
        int shardId,
        int numShards
    ) throws IOException {
        List<LeafReader> leaves = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            // fragment-to-shard partitioning: shard N serves fragments with id % numShards == N
            if (fragment.getId() % numShards != shardId) {
                continue;
            }
            leaves.add(new LanceFragmentLeafReader(dataset, fragment.getId(), fragment.metadata().getPhysicalRows(), intField));
        }
        ByteBuffersDirectory bridgeDir = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(bridgeDir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
        }
        DirectoryReader bridge = DirectoryReader.open(bridgeDir);
        return new LanceDirectoryReader(directory, leaves.toArray(new LeafReader[0]), commit, dataset, bridge);
    }

    private LanceDirectoryReader(
        Directory directory,
        LeafReader[] leaves,
        IndexCommit commit,
        Dataset dataset,
        DirectoryReader cacheLifetimeBridge
    ) throws IOException {
        super(directory, leaves, null);
        this.commit = commit;
        this.dataset = dataset;
        this.cacheLifetimeBridge = cacheLifetimeBridge;
    }

    @Override
    protected DirectoryReader doOpenIfChanged() {
        return null;
    }

    @Override
    protected DirectoryReader doOpenIfChanged(ExecutorService executorService) {
        return null;
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexCommit commit) {
        return null;
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexCommit commit, ExecutorService executorService) {
        return null;
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) {
        return null;
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes, ExecutorService executorService) {
        return null;
    }

    @Override
    public long getVersion() {
        return 1;
    }

    @Override
    public boolean isCurrent() {
        return true;
    }

    @Override
    public IndexCommit getIndexCommit() {
        return commit;
    }

    @Override
    protected void doClose() throws IOException {
        try {
            cacheLifetimeBridge.close();
        } finally {
            dataset.close();
        }
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return cacheLifetimeBridge.getReaderCacheHelper();
    }
}

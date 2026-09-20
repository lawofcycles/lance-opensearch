/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Checks that the manifest-recorded data file total the shard reader
 * reports through {@code DocsStats.totalSizeInBytes} matches the bytes
 * actually on disk under the table's {@code data/} directory.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceDataFileSizeTests extends OpenSearchTestCase {

    public void testSumDataFileSizesMatchesBytesOnDisk() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeTable(scratchDir, "sizes", 12);
        long onDisk = bytesUnderDataDir(Path.of(uri));
        assertTrue("fixture wrote no data files under " + uri, onDisk > 0L);

        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceDirectoryReader.DataFileSizes sizes = LanceDirectoryReader.sumDataFileSizes(dataset.getFragments());
            assertEquals("every data file written by Lance 12 carries a manifest size", 0, sizes.filesWithoutSize());
            assertEquals(onDisk, sizes.knownBytes());
        }

        // A delete adds a deletion file but rewrites no data file, so the
        // data file total is unchanged while the row count drops.
        LanceTableFactory.deleteRows(uri, "id IN (1, 4)");
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceDirectoryReader.DataFileSizes sizes = LanceDirectoryReader.sumDataFileSizes(dataset.getFragments());
            assertEquals(onDisk, sizes.knownBytes());
            assertEquals(10L, dataset.countRows());
        }
    }

    public void testEngineReaderChainResolvesDataFileSizes() throws Exception {
        // docStats() sees the shard reader through OpenSearchDirectoryReader,
        // so the unwrap in dataFileSizesOf must reach the LanceDirectoryReader
        // underneath; if that chain stops matching the reported total
        // silently falls back to 0.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeTable(scratchDir, "chain", 12);
        long onDisk = bytesUnderDataDir(Path.of(uri));
        ShardId shardId = new ShardId("chain", "uuid", 0);

        Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        try (
            LanceDirectoryReader reader = LanceDirectoryReader.open(
                new ByteBuffersDirectory(),
                null,
                dataset,
                "",
                LanceEngineFactory.LancePrimaryKeyType.NONE,
                Collections.emptyMap(),
                new NoopCircuitBreaker(CircuitBreaker.REQUEST)
            )
        ) {
            OpenSearchDirectoryReader wrapped = OpenSearchDirectoryReader.wrap(reader, shardId);
            assertSame(reader, FilterDirectoryReader.unwrap(wrapped));
            assertEquals(onDisk, LanceDirectoryReader.dataFileSizesOf(wrapped).knownBytes());
            assertEquals(0, LanceDirectoryReader.dataFileSizesOf(wrapped).filesWithoutSize());
            assertEquals(12, wrapped.numDocs());
        }

        // Per-request fragment readers never serve shard stats and skip the
        // manifest walk.
        Dataset fragmentDataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        List<Integer> fragmentIds = new ArrayList<>();
        for (Fragment fragment : fragmentDataset.getFragments()) {
            fragmentIds.add(fragment.getId());
        }
        try (
            LanceDirectoryReader reader = LanceDirectoryReader.openForFragments(
                new ByteBuffersDirectory(),
                null,
                fragmentDataset,
                "",
                LanceEngineFactory.LancePrimaryKeyType.NONE,
                Collections.emptyMap(),
                fragmentIds
            )
        ) {
            assertEquals(LanceDirectoryReader.DataFileSizes.NONE, reader.dataFileSizes());
            assertEquals(LanceDirectoryReader.DataFileSizes.NONE, LanceDirectoryReader.dataFileSizesOf(reader));
        }
    }

    private static long bytesUnderDataDir(Path tablePath) throws Exception {
        try (Stream<Path> files = Files.list(tablePath.resolve("data"))) {
            long total = 0L;
            for (Path file : (Iterable<Path>) files::iterator) {
                if (file.getFileName().toString().endsWith(".lance")) {
                    total += Files.size(file);
                }
            }
            return total;
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceTableSizes;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Checks that the manifest-recorded data file total
 * {@link LanceTableSizes#dataFileBytes(Dataset)} reports (the shard
 * engine's {@code DocsStats.totalSizeInBytes}) matches the bytes actually
 * on disk under the table's {@code data/} directory.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceDataFileSizeTests extends OpenSearchTestCase {

    public void testDataFileBytesMatchesBytesOnDisk() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeTable(scratchDir, "sizes", 12);
        long onDisk = bytesUnderDataDir(Path.of(uri));
        assertTrue("fixture wrote no data files under " + uri, onDisk > 0L);

        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceTableSizes.DataFileSizes sizes = LanceTableSizes.dataFileBytes(dataset);
            assertEquals("every data file written by Lance 12 carries a manifest size", 0, sizes.filesWithoutSize());
            assertEquals(onDisk, sizes.knownBytes());
            assertEquals(sizes, LanceTableSizes.dataFileBytes(dataset.getFragments()));
        }

        // A delete adds a deletion file but rewrites no data file, so the
        // data file total is unchanged while the row count drops.
        LanceTableFactory.deleteRows(uri, "id IN (1, 4)");
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceTableSizes.DataFileSizes sizes = LanceTableSizes.dataFileBytes(dataset);
            assertEquals(onDisk, sizes.knownBytes());
            assertEquals(10L, dataset.countRows());
        }
    }

    public void testReadersRecordTheHelperTotal() throws Exception {
        // The whole table reader records the helper's total at open; a
        // per-request fragment reader never serves shard stats and skips
        // the manifest walk.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeTable(scratchDir, "chain", 12);
        long onDisk = bytesUnderDataDir(Path.of(uri));

        Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        try (
            LanceDirectoryReader reader = LanceDirectoryReader.open(
                new ByteBuffersDirectory(),
                null,
                dataset,
                "",
                LanceEngineFactory.LancePrimaryKeyType.NONE,
                LanceOverrides.EMPTY,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST)
            )
        ) {
            assertEquals(onDisk, reader.dataFileSizes().knownBytes());
            assertEquals(0, reader.dataFileSizes().filesWithoutSize());
            assertEquals(12, reader.numDocs());
        }

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
                LanceOverrides.EMPTY,
                fragmentIds
            )
        ) {
            assertEquals(LanceTableSizes.DataFileSizes.NONE, reader.dataFileSizes());
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

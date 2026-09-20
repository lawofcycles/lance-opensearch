/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.lance.Dataset;
import org.opensearch.lance.engine.LanceDirectoryReader;
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

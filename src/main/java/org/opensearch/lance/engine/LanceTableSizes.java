/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.util.List;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.fragment.DataFile;

/**
 * Byte totals of a Lance table version, read from its manifest alone.
 *
 * <p>The manifest records a size for every data file a fragment
 * references, so the total needs no reader, no scan and no object store
 * request beyond the manifest open the caller already paid: the shard
 * engine's {@code docStats()} sums it per {@code _stats} call, and the
 * whole table reader and the warm cache snapshot record it at open.
 */
public final class LanceTableSizes {

    /**
     * Byte total the Lance manifest records for the data files behind a
     * set of fragments.
     *
     * @param knownBytes       sum of {@code DataFile.getFileSizeBytes()} over
     *                         every data file whose size the manifest
     *                         records
     * @param filesWithoutSize number of data files the manifest lists
     *                         without a size (older writers did not record
     *                         one); those files contribute nothing to
     *                         {@code knownBytes}, so the total is a lower
     *                         bound whenever this is non-zero
     */
    public record DataFileSizes(long knownBytes, int filesWithoutSize) {
        public static final DataFileSizes NONE = new DataFileSizes(0L, 0);
    }

    private LanceTableSizes() {}

    /** The data file total of the version {@code dataset} is open at. */
    public static DataFileSizes dataFileBytes(Dataset dataset) {
        return dataFileBytes(dataset.getFragments());
    }

    /**
     * Sum the manifest-recorded sizes of every data file the given fragments
     * reference. Reads only the in-memory manifest ({@code Fragment.metadata()}
     * is a field access on an already materialised {@code FragmentMetadata});
     * no object-store request is made, which is why this is affordable per
     * call where {@code Dataset.calculateDataSize()}, which fetches each
     * data file's footer, is not. Data overlay files are included through
     * {@code getReferencedLanceFiles()}; deletion files and index files are
     * not data files and are left out.
     */
    public static DataFileSizes dataFileBytes(List<Fragment> fragments) {
        long knownBytes = 0L;
        int filesWithoutSize = 0;
        for (Fragment fragment : fragments) {
            for (DataFile dataFile : fragment.metadata().getReferencedLanceFiles()) {
                Long size = dataFile.getFileSizeBytes();
                if (size == null) {
                    filesWithoutSize++;
                } else {
                    knownBytes += size;
                }
            }
        }
        return new DataFileSizes(knownBytes, filesWithoutSize);
    }
}

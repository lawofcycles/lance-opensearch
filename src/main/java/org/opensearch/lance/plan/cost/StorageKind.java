/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import java.util.Locale;

/**
 * Where a Lance table's column bytes come from when the pushed scan
 * reads them. The Lucene aggregator path reads warmed columns from the
 * node's off heap column store and never touches storage once warm, so
 * the storage kind only moves the pushed scan's cost: a request over an
 * object store table pays the store's request latency and its per byte
 * transfer on every scan, a request over a local table does not.
 */
public enum StorageKind {
    /** A local file system path or an unknown scheme: NVMe, EBS, a bind mount. */
    LOCAL,
    /** An object store: S3, GCS, Azure Blob and their aliases. */
    OBJECT_STORE;

    /**
     * The kind a table URI names by its scheme: {@code s3}, {@code s3a},
     * {@code gs}, {@code gcs}, {@code az}, {@code abfs}, {@code abfss},
     * {@code wasb}, {@code wasbs}, {@code adl} and {@code oss} are
     * object stores; {@code file}, a bare path and any other scheme are
     * local. A null or empty URI is local.
     */
    public static StorageKind fromUri(String tableUri) {
        if (tableUri == null) {
            return LOCAL;
        }
        int colon = tableUri.indexOf(':');
        if (colon <= 0) {
            return LOCAL;
        }
        String scheme = tableUri.substring(0, colon).toLowerCase(Locale.ROOT);
        switch (scheme) {
            case "s3":
            case "s3a":
            case "s3n":
            case "gs":
            case "gcs":
            case "az":
            case "abfs":
            case "abfss":
            case "wasb":
            case "wasbs":
            case "adl":
            case "oss":
                return OBJECT_STORE;
            default:
                return LOCAL;
        }
    }
}

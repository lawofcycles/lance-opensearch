/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.NativeMemoryLimit.IndexCacheSizing;
import org.opensearch.lance.query.LanceInvalidInput;
import org.opensearch.lance.rest.RestAttachAction.Derivation;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The attach time warning that a table's inverted index is heavier than
 * one index cache shard admits, and the log line of an attach the client
 * is answered a 4xx for. The warning's decision is a pure function of
 * the derivation and the Session's index cache sizing, so it is tested
 * on its message; the 4xx line is captured through a log appender.
 */
public class TransportLanceAttachActionTests extends OpenSearchTestCase {

    private static final long GIB = 1L << 30;

    /** The default sizing on a 16 vCPU node: 2 shards of 8 GiB - 1. */
    private static final IndexCacheSizing DEFAULT_16_CPUS = NativeMemoryLimit.sizeIndexCache(19 * GIB, 16);

    private static Derivation derivation(long rows, Set<String> ftsColumns) {
        return new Derivation("{}", "", "long", "", 3L, rows, 4, List.of(), ftsColumns, Set.of("rating"), Set.of(), Set.of());
    }

    public void testWarnsWhenTheEstimateExceedsTheShardShare() {
        // 1B rows estimate to 52 GB, far above the 8 GiB share.
        String warning = TransportLanceAttachAction.invertedIndexShardShareWarning(
            "perf1b",
            derivation(1_000_000_000L, Set.of("body")),
            DEFAULT_16_CPUS
        );
        assertNotNull(warning);
        assertEquals(
            "inverted index of [perf1b] (~48.4gb) may not fit one index cache shard (7.9gb); "
                + "raise plugins.lance.native_memory.limit or lower plugins.lance.cache.column_share",
            warning
        );

        // The 19 GiB cache the plugin used to hand Lance as is: 4 shards
        // of 4.75 GiB, below the 100M estimate of 4.84 GiB.
        String at19 = TransportLanceAttachAction.invertedIndexShardShareWarning(
            "perf100m",
            derivation(100_000_000L, Set.of("body")),
            IndexCacheSizing.ofCapacity(19 * GIB, 16)
        );
        assertNotNull(at19);
        assertTrue(at19, at19.startsWith("inverted index of [perf100m] (~4.8gb) may not fit one index cache shard (4.7gb)"));
    }

    public void testSilentWhenTheEstimateFitsTheShardShare() {
        // 100M rows estimate to 4.84 GiB, within the 8 GiB share.
        assertNull(
            TransportLanceAttachAction.invertedIndexShardShareWarning("perf100m", derivation(100_000_000L, Set.of("body")), DEFAULT_16_CPUS)
        );
        // Exactly the share still fits: the cache refuses entries heavier
        // than the share, not entries equal to it.
        long rowsAtShare = DEFAULT_16_CPUS.shardShareBytes() / NativeMemoryLimit.invertedIndexEntryEstimateBytes(1L);
        assertNull(
            TransportLanceAttachAction.invertedIndexShardShareWarning("edge", derivation(rowsAtShare, Set.of("body")), DEFAULT_16_CPUS)
        );
        assertNotNull(
            TransportLanceAttachAction.invertedIndexShardShareWarning("edge", derivation(rowsAtShare + 1, Set.of("body")), DEFAULT_16_CPUS)
        );
    }

    public void testSilentWithoutAFullTextColumnOrASession() {
        // No inverted index, nothing to fit.
        assertNull(
            TransportLanceAttachAction.invertedIndexShardShareWarning("keywords", derivation(1_000_000_000L, Set.of()), DEFAULT_16_CPUS)
        );
        // No Session installed (unit test JVMs), so no share to compare with.
        assertNull(TransportLanceAttachAction.invertedIndexShardShareWarning("perf1b", derivation(1_000_000_000L, Set.of("body")), null));
    }

    /**
     * An attach the client is answered a 400 for leaves one INFO line on
     * the node, with the index name and the message as the client sees
     * it (the store's error code kept, the access key id redacted); the
     * REST layer writes nothing for a 4xx. A failure the client is
     * answered a 500 for is left to the REST layer's WARN line.
     */
    public void testAClientErrorIsLoggedOnceAtInfoWithTheRedactedMessage() throws Exception {
        String keyId = "AKIA" + "IOSFODNN7EXAMPLE";
        String table = "s3://redaction-bucket/attach.lance";
        IOException refused = new IOException(
            "LanceError(IO): Generic S3 error: Error performing GET https://s3.us-east-1.amazonaws.com/redaction-bucket/attach.lance "
                + "- Server returned non-2xx status code: 403 Forbidden: <Error><Code>InvalidAccessKeyId</Code><AWSAccessKeyId>"
                + keyId
                + "</AWSAccessKeyId></Error>, /rust/lance-io/src/object_store.rs:1490:92"
        );
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(TransportLanceAttachAction.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "the refusal is logged once at INFO with the index name and the redacted message",
                    TransportLanceAttachAction.class.getName(),
                    Level.INFO,
                    "lance.attach: attach of table ["
                        + table
                        + "] as index [attach] was refused with 400: "
                        + LanceInvalidInput.CREDENTIALS_REJECTED_PREFIX
                        + table
                        + "]: *InvalidAccessKeyId*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "the key id is not logged",
                    TransportLanceAttachAction.class.getName(),
                    Level.INFO,
                    "*" + keyId + "*"
                )
            );
            Exception reported = TransportLanceAttachAction.reportFailure(refused, table, null);
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(reported));
            appender.assertAllExpectationsMatched();
        }

        // The request's own index name is the one logged.
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(TransportLanceAttachAction.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "a tag that does not resolve is the caller's error too",
                    TransportLanceAttachAction.class.getName(),
                    Level.INFO,
                    "lance.attach: attach of table [" + table + "] as index [named] was refused with 400: tag [v9] could not be resolved*"
                )
            );
            Exception reported = TransportLanceAttachAction.reportFailure(
                new OpenSearchStatusException("tag [v9] could not be resolved on table [" + table + "]", RestStatus.BAD_REQUEST),
                table,
                "named"
            );
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(reported));
            appender.assertAllExpectationsMatched();
        }

        // A server error writes nothing here; the REST layer's WARN reports it.
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(TransportLanceAttachAction.class))) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "a 500 is left to the REST layer",
                    TransportLanceAttachAction.class.getName(),
                    Level.INFO,
                    "lance.attach: attach of table*"
                )
            );
            Exception reported = TransportLanceAttachAction.reportFailure(
                new IOException("LanceError(IO): Generic S3 error: <Error><Code>NoSuchBucket</Code></Error>"),
                table,
                null
            );
            assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(reported));
            appender.assertAllExpectationsMatched();
        }
    }
}

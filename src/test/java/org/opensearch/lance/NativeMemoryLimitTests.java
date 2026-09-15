/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.opensearch.OpenSearchParseException;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link NativeMemoryLimit}. The parser has two flavours: an
 * absolute {@code ByteSizeValue} form (e.g. {@code "10gb"}) and a
 * percentage form (e.g. {@code "40%"}). The percentage flavour uses the
 * live host's memory numbers, so this class covers both the deterministic
 * absolute case with exact assertions and the percentage case with a
 * relaxed assertion tied to the same {@link OsProbe} / {@link JvmInfo}
 * calls that the parser makes at runtime.
 */
public class NativeMemoryLimitTests extends OpenSearchTestCase {

    private static final String KEY = "lance.native_memory.limit";

    public void testParsesAbsoluteByteValues() {
        assertEquals(10L * 1024 * 1024 * 1024, NativeMemoryLimit.parse("10gb", KEY));
        assertEquals(512L * 1024 * 1024, NativeMemoryLimit.parse("512mb", KEY));
        assertEquals(1L * 1024, NativeMemoryLimit.parse("1kb", KEY));
        assertEquals(0L, NativeMemoryLimit.parse("0b", KEY));
    }

    public void testParsesPercentageAgainstEligibleMemory() {
        long physical = OsProbe.getInstance().getTotalPhysicalMemorySize();
        long heap = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        long eligible = Math.max(0L, physical - heap);

        long fortyPercent = NativeMemoryLimit.parse("40%", KEY);
        assertEquals((long) (0.40 * eligible), fortyPercent);

        long fullEligible = NativeMemoryLimit.parse("100%", KEY);
        assertEquals(eligible, fullEligible);

        assertEquals(0L, NativeMemoryLimit.parse("0%", KEY));
    }

    public void testPercentageOutOfRangeRejected() {
        // Above 100% would exceed the host's eligible memory and is a
        // configuration error, so surface the malformed input rather
        // than silently clamp.
        OpenSearchParseException high = expectThrows(OpenSearchParseException.class, () -> NativeMemoryLimit.parse("150%", KEY));
        assertTrue(high.getMessage(), high.getMessage().contains("percentage must be in [0, 100]"));

        OpenSearchParseException low = expectThrows(OpenSearchParseException.class, () -> NativeMemoryLimit.parse("-1%", KEY));
        assertTrue(low.getMessage(), low.getMessage().contains("percentage must be in [0, 100]"));
    }

    public void testPercentageMustBeNumeric() {
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> NativeMemoryLimit.parse("abc%", KEY));
        assertTrue(e.getMessage(), e.getMessage().contains("percentage must be numeric"));
    }

    public void testNullValueRejected() {
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> NativeMemoryLimit.parse(null, KEY));
        assertTrue(e.getMessage(), e.getMessage().contains("cannot be null"));
    }

    public void testSplitBetweenIndexAndMetadataFollowsLanceDefaultRatio() {
        // Lance's own defaults are 6 GiB index cache and 1 GiB metadata
        // cache, so we split the shared limit 6:1 to preserve that
        // ratio when operators configure a single number.
        long total = 7_000_000L;
        long indexBytes = NativeMemoryLimit.indexCacheBytes(total);
        long metadataBytes = NativeMemoryLimit.metadataCacheBytes(total);

        assertEquals(6_000_000L, indexBytes);
        assertEquals(1_000_000L, metadataBytes);
        assertTrue("sum should not exceed the total", indexBytes + metadataBytes <= total);
    }

    public void testHumanReadableRoundTripsAcrossUnits() {
        assertEquals("1gb", NativeMemoryLimit.humanReadable(1L * 1024 * 1024 * 1024));
        assertEquals("512mb", NativeMemoryLimit.humanReadable(512L * 1024 * 1024));
        assertEquals("0b", NativeMemoryLimit.humanReadable(0L));
    }
}

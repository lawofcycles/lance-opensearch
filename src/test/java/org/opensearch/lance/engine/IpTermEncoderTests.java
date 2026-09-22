/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The {@code ip} dictionary value encoder: IPv4, IPv6 and IPv4-mapped
 * strings encode into the 16 byte {@link InetAddressPoint} form, an
 * unparseable string encodes to {@code null}, and a
 * {@link KeywordDictionaryBuilder} carrying the encoder serves the
 * invalid rows as missing while counting them once for the
 * per-fragment log line.
 */
public class IpTermEncoderTests extends OpenSearchTestCase {

    private static BytesRef encode(String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        return IpTermEncoder.INSTANCE.encode(utf8, utf8.length);
    }

    private static BytesRef expected(String address) {
        return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(address)));
    }

    public void testIpv4EncodesToSixteenBytes() {
        BytesRef encoded = encode("10.0.0.4");
        assertEquals(16, encoded.length);
        assertEquals(expected("10.0.0.4"), encoded);
    }

    public void testIpv6Encodes() {
        assertEquals(expected("2001:db8::1"), encode("2001:db8::1"));
    }

    public void testIpv4MappedIpv6EqualsThePlainIpv4Form() {
        assertEquals(encode("10.0.0.2"), encode("::ffff:10.0.0.2"));
    }

    public void testInvalidStringEncodesToNull() {
        assertNull(encode("not-an-ip"));
        assertNull(encode(""));
        assertNull(encode("10.0.0.4/8"));
    }

    public void testEncodedOrderIsAddressOrderNotStringOrder() {
        // As strings "10.0.0.30" < "10.0.0.4"; as addresses 30 > 4.
        assertTrue(encode("10.0.0.4").compareTo(encode("10.0.0.30")) < 0);
        // Every IPv4 address sorts before this IPv6 address in the
        // encoded space (0x00... prefix against 0x20...).
        assertTrue(encode("192.168.1.7").compareTo(encode("2001:db8::1")) < 0);
    }

    public void testBuilderWithEncoderServesInvalidValuesAsMissing() {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE); VarCharVector vector = new VarCharVector("ip", allocator)) {
            String[] values = { "10.0.0.30", "not-an-ip", "10.0.0.4", "10.0.0.4" };
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
            }
            vector.setValueCount(values.length);

            KeywordDictionaryBuilder builder = new KeywordDictionaryBuilder(IpTermEncoder.INSTANCE);
            int[] ids = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                ids[i] = builder.intern(vector, i);
            }
            assertEquals("invalid value must intern as missing", -1, ids[1]);
            assertEquals("duplicate values must intern to one id", ids[2], ids[3]);
            assertEquals(1, builder.invalidCount());
            assertEquals(2, builder.size());

            KeywordDictionaryBuilder.Dictionary dictionary = builder.finish();
            dictionary.remap(ids);
            assertEquals(-1, ids[1]);
            // Sorted encoded order: 10.0.0.4 before 10.0.0.30.
            assertEquals(expected("10.0.0.4"), dictionary.terms()[0]);
            assertEquals(expected("10.0.0.30"), dictionary.terms()[1]);
            assertEquals(1, ids[0]);
            assertEquals(0, ids[2]);
        }
    }

    public void testBuilderWithoutEncoderKeepsRawBytes() {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE); VarCharVector vector = new VarCharVector("ip", allocator)) {
            vector.allocateNew(1);
            vector.setSafe(0, "not-an-ip".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);
            KeywordDictionaryBuilder builder = new KeywordDictionaryBuilder();
            assertEquals(0, builder.intern(vector, 0));
            assertEquals(0, builder.invalidCount());
            assertEquals(new BytesRef("not-an-ip"), builder.finish().terms()[0]);
        }
    }
}

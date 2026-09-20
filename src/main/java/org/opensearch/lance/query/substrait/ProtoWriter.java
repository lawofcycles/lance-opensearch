/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query.substrait;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Minimal protobuf wire format encoder: enough of the encoding rules
 * (varints, 64-bit fixed values, length-delimited fields) to write the
 * Substrait messages {@link SubstraitAggregatePlan} needs. The plugin
 * carries no protobuf runtime because OpenSearch's jar hell check
 * rejects classes that duplicate the server's, and {@code protobuf-java}
 * is one of them.
 *
 * <p>Only the writer side exists: nothing in the plugin decodes
 * Substrait. Each method appends one field; nested messages are built
 * with their own writer and appended with {@link #message}.
 */
final class ProtoWriter {

    private static final int WIRE_VARINT = 0;
    private static final int WIRE_FIXED64 = 1;
    private static final int WIRE_LENGTH_DELIMITED = 2;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    /** Appends a varint field: int32, int64, uint32, bool and enum values. */
    ProtoWriter varint(int fieldNumber, long value) {
        tag(fieldNumber, WIRE_VARINT);
        rawVarint(value);
        return this;
    }

    /** Appends a {@code bool} field. */
    ProtoWriter bool(int fieldNumber, boolean value) {
        return varint(fieldNumber, value ? 1L : 0L);
    }

    /** Appends a {@code double} field (fixed 64-bit little endian). */
    ProtoWriter fixed64Double(int fieldNumber, double value) {
        tag(fieldNumber, WIRE_FIXED64);
        long bits = Double.doubleToLongBits(value);
        for (int i = 0; i < 8; i++) {
            out.write((int) (bits >>> (8 * i)) & 0xFF);
        }
        return this;
    }

    /** Appends a {@code string} field. */
    ProtoWriter string(int fieldNumber, String value) {
        return bytes(fieldNumber, value.getBytes(StandardCharsets.UTF_8));
    }

    /** Appends a length-delimited field from raw bytes. */
    ProtoWriter bytes(int fieldNumber, byte[] value) {
        tag(fieldNumber, WIRE_LENGTH_DELIMITED);
        rawVarint(value.length);
        out.write(value, 0, value.length);
        return this;
    }

    /** Appends an embedded message field. An empty writer encodes an empty message, which is how {@code RootReference {}} is written. */
    ProtoWriter message(int fieldNumber, ProtoWriter nested) {
        return bytes(fieldNumber, nested.toBytes());
    }

    byte[] toBytes() {
        return out.toByteArray();
    }

    private void tag(int fieldNumber, int wireType) {
        rawVarint(((long) fieldNumber << 3) | wireType);
    }

    private void rawVarint(long value) {
        // Negative int64 values take the full ten bytes, which is what
        // protobuf does for the non-zigzag integer types.
        while ((value & ~0x7FL) != 0L) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }
}

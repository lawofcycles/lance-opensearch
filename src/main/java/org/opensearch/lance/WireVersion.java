/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * The version marker every plugin-internal message that travels
 * between nodes opens with.
 *
 * <p>The plugin's wire formats are internal to the plugin and assume
 * every node in the cluster runs the same plugin version; there is no
 * negotiation between versions. Without a marker, a request between
 * two plugin versions fails wherever the field layouts first diverge,
 * as a stream corruption error that names neither the message nor the
 * cause. With it, the reader compares the number before it reads any
 * field and refuses the stream with an {@link IOException} naming the
 * message and both numbers.
 *
 * <p>Each message class owns a {@code WIRE_VERSION} constant, writes
 * it through {@link #write} as the first field it owns (after the
 * fields its OpenSearch base class writes) and reads it through
 * {@link #read} at the same position. The constant is bumped whenever
 * a field of that message, or of a class nested in it that has no
 * marker of its own, is added, removed or retyped. No reader decodes
 * an older number today: the marker detects a mismatch, it does not
 * bridge one, and the request fails rather than falling back to
 * another path.
 *
 * <p>The number travels as a variable length int, so the marker costs
 * one byte while it stays below 128.
 */
public final class WireVersion {

    private WireVersion() {}

    /** Writes {@code version} as the message's first field. */
    public static void write(StreamOutput out, int version) throws IOException {
        out.writeVInt(version);
    }

    /**
     * Reads the marker and refuses the stream when it is not
     * {@code expected}.
     *
     * @param in the stream positioned at the marker
     * @param format the message's name, used in the error message
     * @param expected the {@code WIRE_VERSION} of the reading class
     * @throws IOException when the stream's marker is another number
     */
    public static void read(StreamInput in, String format, int expected) throws IOException {
        int version = in.readVInt();
        if (version != expected) {
            throw new IOException(mismatchMessage(format, version, expected));
        }
    }

    /** The message {@link #read} refuses a stream of another version with. */
    public static String mismatchMessage(String format, int found, int expected) {
        return format
            + " wire version ["
            + found
            + "] does not match this node's ["
            + expected
            + "]: every node must run the same plugin version";
    }
}

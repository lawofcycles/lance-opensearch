/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * The version marker every plugin-internal message that travels
 * between nodes opens with, and the framing that lets two plugin
 * versions read each other's messages.
 *
 * <p>The plugin's wire formats are internal to the plugin, and during
 * a rolling upgrade a cluster runs two plugin versions at once: a
 * request written by one version is read by the other, and the
 * response comes back the same way. OpenSearch versions its own
 * messages by the receiving node's version, which the plugin does not
 * know for its own classes, so a plugin message is instead written in
 * one shape that both sides can read.
 *
 * <p>Each message class owns a {@code WIRE_VERSION} constant and lays
 * its stream out in three parts. The marker, {@link #write}, is the
 * writer's constant. The base fields follow it inline: the fields the
 * class had at version 1, in that order, forever. Every later version
 * appends one block, {@link #writeBlock}: the fields that version
 * added, as a length prefixed byte array behind a flag, so a reader
 * that does not know the version can step over the block without
 * decoding it. A reader opens with {@link #read}, reads the base
 * fields itself, and takes each block it knows through
 * {@link Reader#block}, which returns the block's fallback when the
 * writer was too old to have written it; {@link Reader#finish} then
 * steps over the blocks of the versions the reader does not know.
 *
 * <p>The flag says whether an older reader may ignore the block. A
 * writer marks the block critical when the fallback an older reader
 * would substitute changes the answer (a filter the older node would
 * not apply), and leaves it optional when the fallback is merely
 * slower or less informative (a pruning hint, a new statistic). An
 * older reader steps over optional blocks and refuses a critical one
 * with an {@link IOException} naming the message and both versions, so
 * a request between plugin versions either runs correctly or fails at
 * once, and never runs with a field silently dropped.
 *
 * <p>Nothing is ever removed from or retyped in the base fields or in
 * a block that shipped: a field that falls out of use keeps being
 * written with its default, because an older reader still expects it
 * at that position. The policy and the version history of every
 * message are in {@code docs/design/wire-format-compat.md}.
 *
 * <p>The marker and the flag travel as variable length ints, one byte
 * each while they stay below 128; a block costs its length prefix on
 * top of its fields.
 */
public final class WireVersion {

    private static final int CRITICAL = 1;

    private WireVersion() {}

    /** Writes {@code version} as the message's first field. */
    public static void write(StreamOutput out, int version) throws IOException {
        out.writeVInt(version);
    }

    /**
     * Writes the fields one version added as a block an older reader
     * can step over.
     *
     * @param out the stream, positioned after the base fields and the
     *     blocks of the versions before this one
     * @param critical whether a reader that does not know this version
     *     must refuse the message instead of ignoring the block; true
     *     when the fallback the reader would substitute changes the
     *     answer
     * @param fields writes the block's fields
     */
    public static void writeBlock(StreamOutput out, boolean critical, Writeable fields) throws IOException {
        try (BytesStreamOutput buffer = new BytesStreamOutput()) {
            buffer.setVersion(out.getVersion());
            fields.writeTo(buffer);
            BytesReference bytes = buffer.bytes();
            out.writeVInt(critical ? CRITICAL : 0);
            out.writeVInt(bytes.length());
            bytes.writeTo(out);
        }
    }

    /**
     * Reads the marker and returns the reader that walks the blocks
     * behind the base fields, for a message whose base fields are the
     * layout of version 1.
     *
     * @param in the stream positioned at the marker
     * @param format the message's name, used in error messages
     * @param current the {@code WIRE_VERSION} of the reading class
     */
    public static Reader read(StreamInput in, String format, int current) throws IOException {
        return read(in, format, current, 1);
    }

    /**
     * Reads the marker and returns the reader that walks the blocks
     * behind the base fields, for a message whose base fields took
     * their layout at version {@code base}. The versions before
     * {@code base} predate the block framing and are the reading
     * class's own to decode from {@link Reader#marker}; the blocks
     * start at {@code base + 1}. Only {@code LanceExplainResponse} has
     * such a version, from before the framing existed; the policy
     * forbids new ones.
     */
    public static Reader read(StreamInput in, String format, int current, int base) throws IOException {
        return new Reader(in, format, in.readVInt(), current, base);
    }

    /** The message a reader refuses a critical block of a version it does not know with. */
    public static String criticalBlockMessage(String format, int found, int blockVersion, int current) {
        return format
            + " wire version ["
            + found
            + "] adds fields in version ["
            + blockVersion
            + "] that this node's ["
            + current
            + "] cannot ignore: upgrade this node before sending it this message";
    }

    /**
     * Walks the version blocks of one message. {@link #block} is called
     * once per version above 1 the reading class knows, in ascending
     * order, right after the base fields are read; {@link #finish}
     * closes the walk.
     */
    public static final class Reader {

        private final StreamInput in;
        private final String format;
        private final int marker;
        private final int current;
        private int next;

        private Reader(StreamInput in, String format, int marker, int current, int base) {
            this.in = in;
            this.format = format;
            this.marker = marker;
            this.current = current;
            this.next = base + 1;
        }

        /** The writer's {@code WIRE_VERSION}. */
        public int marker() {
            return marker;
        }

        /**
         * Reads the block of {@code version}, or returns
         * {@code fallback} when the writer was older than
         * {@code version} and never wrote it. When the reading class
         * is itself older than {@code version} (a reader acting for an
         * older node, see the mixed version tests), the block is
         * stepped over like an unknown one and {@code fallback} is
         * returned.
         *
         * @param version the version that added the block; the blocks
         *     are read in ascending order starting at 2
         * @param parser reads the block's fields from a stream holding
         *     the block alone
         * @param fallback the value an older writer's message stands for
         * @throws IOException when {@code parser} leaves bytes of the
         *     block unread, or when the block is critical and this
         *     reader does not know its version
         */
        public <T> T block(int version, Writeable.Reader<T> parser, T fallback) throws IOException {
            if (version != next) {
                throw new IllegalStateException(format + " reads wire version block [" + version + "] but block [" + next + "] comes next");
            }
            next++;
            if (version > marker) {
                return fallback;
            }
            if (version > current) {
                skip(version);
                return fallback;
            }
            in.readVInt();
            byte[] bytes = in.readByteArray();
            try (StreamInput block = wrap(bytes)) {
                T value = parser.read(block);
                int left = block.available();
                if (left != 0) {
                    throw new IOException(format + " wire version block [" + version + "] left " + left + " bytes unread");
                }
                return value;
            }
        }

        /**
         * Steps over the blocks of the versions this reader does not
         * know, refusing a critical one.
         *
         * @throws IllegalStateException when the reading class did not
         *     call {@link #block} for a version it knows, whatever the
         *     writer's marker: the fallback would otherwise stand in
         *     for the block reader that was meant to run
         * @throws IOException when a block this reader does not know is
         *     critical
         */
        public void finish() throws IOException {
            if (next <= current) {
                throw new IllegalStateException(format + " did not read wire version block [" + next + "]");
            }
            while (next <= marker) {
                skip(next);
                next++;
            }
        }

        private void skip(int version) throws IOException {
            int flags = in.readVInt();
            in.readByteArray();
            if ((flags & CRITICAL) != 0) {
                throw new IOException(criticalBlockMessage(format, marker, version, current));
            }
        }

        private StreamInput wrap(byte[] bytes) {
            StreamInput block = StreamInput.wrap(bytes);
            block.setVersion(in.getVersion());
            NamedWriteableRegistry registry = in.namedWriteableRegistry();
            return registry == null ? block : new NamedWriteableAwareStreamInput(block, registry);
        }
    }
}

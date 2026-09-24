/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The marker and block framing that lets two plugin versions read each
 * other's messages: a newer reader takes the fallbacks of the blocks an
 * older writer did not send, an older reader steps over the optional
 * blocks of a newer writer and refuses a critical one by name.
 */
public class WireVersionTests extends OpenSearchTestCase {

    /** A message at version 3: base field {@code name}, block 2 {@code count}, block 3 {@code label}. */
    private record Message(String name, int count, String label) {

        static final int WIRE_VERSION = 3;

        void writeTo(StreamOutput out, boolean labelCritical) throws IOException {
            WireVersion.write(out, WIRE_VERSION);
            out.writeString(name);
            WireVersion.writeBlock(out, false, o -> o.writeVInt(count));
            WireVersion.writeBlock(out, labelCritical, o -> o.writeOptionalString(label));
        }

        /** Reads as a node at {@code asVersion} would. */
        static Message read(StreamInput in, int asVersion) throws IOException {
            WireVersion.Reader reader = WireVersion.read(in, "Message", asVersion);
            String name = in.readString();
            int count = reader.block(2, StreamInput::readVInt, -1);
            String label = reader.block(3, StreamInput::readOptionalString, null);
            reader.finish();
            return new Message(name, count, label);
        }
    }

    public void testSameVersionRoundTripsEveryField() throws IOException {
        Message message = new Message("m", 7, "l");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            message.writeTo(out, true);
            out.writeString("after");
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(message, Message.read(in, Message.WIRE_VERSION));
                assertEquals("the reader stops right after the message", "after", in.readString());
            }
        }
    }

    public void testNewerReaderTakesTheFallbacksOfBlocksAnOlderWriterDidNotSend() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // A version 1 writer: marker and base field only.
            WireVersion.write(out, 1);
            out.writeString("m");
            out.writeString("after");
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(new Message("m", -1, null), Message.read(in, Message.WIRE_VERSION));
                assertEquals("after", in.readString());
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // A version 2 writer: marker, base field and the count block.
            WireVersion.write(out, 2);
            out.writeString("m");
            WireVersion.writeBlock(out, false, o -> o.writeVInt(5));
            out.writeString("after");
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(new Message("m", 5, null), Message.read(in, Message.WIRE_VERSION));
                assertEquals("after", in.readString());
            }
        }
    }

    public void testOlderReaderStepsOverOptionalBlocksOfANewerWriter() throws IOException {
        Message message = new Message("m", 7, "l");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            message.writeTo(out, false);
            out.writeString("after");
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(new Message("m", -1, null), Message.read(in, 1));
                assertEquals("the reader lands right after the skipped blocks", "after", in.readString());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(new Message("m", 7, null), Message.read(in, 2));
                assertEquals("after", in.readString());
            }
        }
    }

    public void testOlderReaderRefusesACriticalBlockByName() throws IOException {
        Message message = new Message("m", 7, "l");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            message.writeTo(out, true);
            try (StreamInput in = out.bytes().streamInput()) {
                IOException refused = expectThrows(IOException.class, () -> Message.read(in, 2));
                assertEquals(
                    "Message wire version [3] adds fields in version [3] that this node's [2] cannot ignore: "
                        + "upgrade this node before sending it this message",
                    refused.getMessage()
                );
            }
            try (StreamInput in = out.bytes().streamInput()) {
                IOException refused = expectThrows(IOException.class, () -> Message.read(in, 1));
                assertEquals(WireVersion.criticalBlockMessage("Message", 3, 3, 1), refused.getMessage());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals("the reader of the block's version is not concerned", message, Message.read(in, 3));
            }
        }
    }

    public void testBlockLeftPartlyUnreadIsRefused() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WireVersion.write(out, 2);
            out.writeString("m");
            WireVersion.writeBlock(out, false, o -> {
                o.writeVInt(5);
                o.writeVInt(6);
            });
            try (StreamInput in = out.bytes().streamInput()) {
                IOException refused = expectThrows(IOException.class, () -> Message.read(in, Message.WIRE_VERSION));
                assertEquals("Message wire version block [2] left 1 bytes unread", refused.getMessage());
            }
        }
    }

    public void testBlocksMustBeReadInOrderAndNoneSkipped() throws IOException {
        Message message = new Message("m", 7, "l");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            message.writeTo(out, false);
            try (StreamInput in = out.bytes().streamInput()) {
                WireVersion.Reader reader = WireVersion.read(in, "Message", Message.WIRE_VERSION);
                in.readString();
                IllegalStateException outOfOrder = expectThrows(
                    IllegalStateException.class,
                    () -> reader.block(3, StreamInput::readOptionalString, null)
                );
                assertEquals("Message reads wire version block [3] but block [2] comes next", outOfOrder.getMessage());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                WireVersion.Reader reader = WireVersion.read(in, "Message", Message.WIRE_VERSION);
                in.readString();
                reader.block(2, StreamInput::readVInt, -1);
                IllegalStateException forgotten = expectThrows(IllegalStateException.class, reader::finish);
                assertEquals("Message did not read wire version block [3]", forgotten.getMessage());
            }
        }
    }

    public void testBaseAfterVersionOneStartsTheBlocksThere() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // A message whose base fields took their layout at version
            // 2: the version 2 stream has no block, version 3 adds one.
            WireVersion.write(out, 3);
            out.writeString("m");
            WireVersion.writeBlock(out, false, o -> o.writeVInt(9));
            try (StreamInput in = out.bytes().streamInput()) {
                WireVersion.Reader reader = WireVersion.read(in, "Rebased", 3, 2);
                assertEquals(3, reader.marker());
                assertEquals("m", in.readString());
                assertEquals(9, (int) reader.block(3, StreamInput::readVInt, -1));
                reader.finish();
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WireVersion.write(out, 2);
            out.writeString("m");
            try (StreamInput in = out.bytes().streamInput()) {
                WireVersion.Reader reader = WireVersion.read(in, "Rebased", 2, 2);
                assertEquals("m", in.readString());
                reader.finish();
            }
        }
    }

    public void testMarkerBelow128IsOneByteAndAnEmptyBlockTwo() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WireVersion.write(out, 1);
            assertEquals(1, out.bytes().length());
            WireVersion.writeBlock(out, false, o -> {});
            assertEquals("flag and length prefix", 3, out.bytes().length());
        }
    }
}

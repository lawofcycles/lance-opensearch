/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * Builds the stream a message of the next plugin version would write,
 * for tests that play the older node reading it.
 */
public final class WireVersionTestSupport {

    private WireVersionTestSupport() {}

    /** A message whose OpenSearch base class writes nothing before the marker. */
    public static final Writeable NO_PRELUDE = out -> {};

    /**
     * The bytes of {@code message} with its marker raised by one and one
     * block of that next version appended.
     *
     * @param message the message as this version writes it
     * @param prelude what the message's OpenSearch base class writes
     *     before the marker (the parent task id of a request, the
     *     cluster manager timeout of a cluster manager request), so the
     *     marker can be found; {@link #NO_PRELUDE} when nothing precedes it
     * @param critical whether the appended block is critical
     * @param block the fields of the appended block
     */
    public static BytesReference asNextVersion(Writeable message, Writeable prelude, boolean critical, Writeable block) throws IOException {
        byte[] preludeBytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            prelude.writeTo(out);
            preludeBytes = BytesReference.toBytes(out.bytes());
        }
        try (BytesStreamOutput written = new BytesStreamOutput(); BytesStreamOutput out = new BytesStreamOutput()) {
            message.writeTo(written);
            try (StreamInput in = written.bytes().streamInput()) {
                byte[] preludeRead = new byte[preludeBytes.length];
                in.readBytes(preludeRead, 0, preludeRead.length);
                int marker = in.readVInt();
                out.writeBytes(preludeRead);
                out.writeVInt(marker + 1);
                out.writeBytes(in.readAllBytes());
            }
            WireVersion.writeBlock(out, critical, block);
            return out.bytes();
        }
    }
}

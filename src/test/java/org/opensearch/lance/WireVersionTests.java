/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class WireVersionTests extends OpenSearchTestCase {

    public void testMatchingMarkerIsConsumedAndNothingElse() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WireVersion.write(out, 3);
            out.writeString("after");
            try (StreamInput in = out.bytes().streamInput()) {
                WireVersion.read(in, "Message", 3);
                assertEquals("the reader stops right after the marker", "after", in.readString());
            }
        }
    }

    public void testAnotherMarkerIsRefusedByNameBeforeAnyFieldIsRead() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WireVersion.write(out, 2);
            out.writeString("never read");
            try (StreamInput in = out.bytes().streamInput()) {
                IOException refused = expectThrows(IOException.class, () -> WireVersion.read(in, "Message", 1));
                assertEquals(
                    "Message wire version [2] does not match this node's [1]: every node must run the same plugin version",
                    refused.getMessage()
                );
                assertEquals("only the marker was consumed", "never read", in.readString());
            }
        }
    }

    public void testMarkerBelow128IsOneByte() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WireVersion.write(out, 1);
            assertEquals(1, out.bytes().length());
        }
    }
}

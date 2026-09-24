/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.lance.WireVersion;
import org.opensearch.transport.TransportRequest;

/**
 * The per node leg of {@link LanceStatsRequest}. Carries nothing but its
 * {@link #WIRE_VERSION} (see {@link WireVersion}): every node reports
 * all of its figures.
 */
public final class LanceStatsNodeRequest extends TransportRequest {

    /** The wire format's version, the first field the request writes after its base class. */
    public static final int WIRE_VERSION = 1;

    public LanceStatsNodeRequest() {
        super();
    }

    public LanceStatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        WireVersion.read(in, "LanceStatsNodeRequest", WIRE_VERSION);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
    }
}

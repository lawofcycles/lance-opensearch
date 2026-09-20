/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

/**
 * The per node leg of {@link LanceStatsRequest}. Carries nothing: every
 * node reports all of its figures.
 */
public final class LanceStatsNodeRequest extends TransportRequest {

    public LanceStatsNodeRequest() {
        super();
    }

    public LanceStatsNodeRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}

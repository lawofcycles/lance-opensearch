/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.List;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.lance.WireVersion;
import org.opensearch.transport.TransportRequest;

/**
 * The per node leg of {@link LanceRequestCacheClearRequest}: the index
 * uuids whose entries the receiving node drops from its result cache.
 * The stream opens with {@link #WIRE_VERSION} after the fields the
 * OpenSearch base class writes (see {@link WireVersion}).
 */
public final class LanceRequestCacheClearNodeRequest extends TransportRequest {

    /** The wire format's version, the first field the request writes after its base class. */
    public static final int WIRE_VERSION = 1;

    private final List<String> indexUuids;

    public LanceRequestCacheClearNodeRequest(List<String> indexUuids) {
        super();
        this.indexUuids = List.copyOf(indexUuids);
    }

    public LanceRequestCacheClearNodeRequest(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceRequestCacheClearNodeRequest", WIRE_VERSION);
        this.indexUuids = in.readStringList();
        reader.finish();
    }

    public List<String> indexUuids() {
        return indexUuids;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        out.writeStringCollection(indexUuids);
    }
}

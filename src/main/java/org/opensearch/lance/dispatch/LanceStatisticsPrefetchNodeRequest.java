/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.lance.WireVersion;
import org.opensearch.transport.TransportRequest;

/**
 * The per node leg of {@link LanceStatisticsPrefetchRequest}: the
 * index, the table URI the statistics cache keys on and the version the
 * receiving node collects. The stream opens with {@link #WIRE_VERSION}
 * after the fields the OpenSearch base class writes (see
 * {@link WireVersion}).
 */
public final class LanceStatisticsPrefetchNodeRequest extends TransportRequest {

    /** The wire format's version, the first field the request writes after its base class. */
    public static final int WIRE_VERSION = 1;

    private final String indexName;
    private final String tableUri;
    private final long version;

    public LanceStatisticsPrefetchNodeRequest(String indexName, String tableUri, long version) {
        super();
        this.indexName = indexName;
        this.tableUri = tableUri;
        this.version = version;
    }

    public LanceStatisticsPrefetchNodeRequest(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceStatisticsPrefetchNodeRequest", WIRE_VERSION);
        this.indexName = in.readString();
        this.tableUri = in.readString();
        this.version = in.readLong();
        reader.finish();
    }

    public String indexName() {
        return indexName;
    }

    public String tableUri() {
        return tableUri;
    }

    public long version() {
        return version;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        out.writeString(indexName);
        out.writeString(tableUri);
        out.writeLong(version);
    }
}

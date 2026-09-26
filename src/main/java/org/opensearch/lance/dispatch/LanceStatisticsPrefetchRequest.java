/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for {@link LanceStatisticsPrefetchAction}: the index whose
 * table moved, the table URI the statistics cache keys on (the URI as
 * Lance spells it, which is what a plan looks up) and the version to
 * collect. Every node resolves the table setting and the storage
 * options from the index's settings in its own cluster state, so no
 * credential travels with the request. Always sent to the data nodes
 * (the cache is per node and only a data node executes and coordinates
 * Lance requests), so the node selector is fixed.
 */
public final class LanceStatisticsPrefetchRequest extends BaseNodesRequest<LanceStatisticsPrefetchRequest> {

    /** The node selector every request resolves to: the data nodes. */
    static final String DATA_NODES = "data:true";

    private final String indexName;
    private final String tableUri;
    private final long version;

    public LanceStatisticsPrefetchRequest(String indexName, String tableUri, long version) {
        super(DATA_NODES);
        this.indexName = indexName;
        this.tableUri = tableUri;
        this.version = version;
    }

    public LanceStatisticsPrefetchRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.tableUri = in.readString();
        this.version = in.readLong();
    }

    public String indexName() {
        return indexName;
    }

    /** The key the statistics cache holds the table under. */
    public String tableUri() {
        return tableUri;
    }

    public long version() {
        return version;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeString(tableUri);
        out.writeLong(version);
    }
}

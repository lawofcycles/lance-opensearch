/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for {@link LanceIndexSyncAction}: the index named in
 * {@code POST /{index}/_lance/sync}. A {@link SingleShardRequest}, so the
 * transport action routes it to the node holding the index's one shard,
 * and a security plugin applies index level permissions to the name.
 */
public final class LanceIndexSyncRequest extends SingleShardRequest<LanceIndexSyncRequest> {

    public LanceIndexSyncRequest(String index) {
        super(index);
    }

    public LanceIndexSyncRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return super.validateNonNullIndex();
    }
}

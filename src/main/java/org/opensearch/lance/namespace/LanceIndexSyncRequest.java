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
import org.opensearch.lance.WireVersion;

/**
 * Request for {@link LanceIndexSyncAction}: the index named in
 * {@code POST /{index}/_lance/sync}. A {@link SingleShardRequest}, so the
 * transport action routes it to the node holding the index's one shard,
 * and a security plugin applies index level permissions to the name.
 * Carries nothing of its own but {@link #WIRE_VERSION} (see
 * {@link WireVersion}).
 */
public final class LanceIndexSyncRequest extends SingleShardRequest<LanceIndexSyncRequest> {

    /** The wire format's version, the first field the request writes after its base class. */
    public static final int WIRE_VERSION = 1;

    public LanceIndexSyncRequest(String index) {
        super(index);
    }

    public LanceIndexSyncRequest(StreamInput in) throws IOException {
        super(in);
        WireVersion.read(in, "LanceIndexSyncRequest", WIRE_VERSION).finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
    }

    @Override
    public ActionRequestValidationException validate() {
        return super.validateNonNullIndex();
    }
}

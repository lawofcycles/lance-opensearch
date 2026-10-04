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
 * {@code POST /_plugins/_lance/sync/{index}}, or the index whose table
 * the search coordinator read at {@link #observedVersion()}. A
 * {@link SingleShardRequest}, so the transport action routes it to the
 * node holding the index's one shard, and a security plugin applies
 * index level permissions to the name. Opens with {@link #WIRE_VERSION}
 * (see {@link WireVersion}); version 2 added the observed version as an
 * optional block, so a node of the previous plugin version runs the
 * check unconditionally, as it does for the manual trigger.
 */
public final class LanceIndexSyncRequest extends SingleShardRequest<LanceIndexSyncRequest> {

    /** The wire format's version, the first field the request writes after its base class; 2 added the observed version. */
    public static final int WIRE_VERSION = 2;

    /** The observed version of a request that checks unconditionally. */
    public static final long NO_OBSERVED_VERSION = -1L;

    private final long observedVersion;

    /** The manual trigger: the check runs whatever the mapping was last derived at. */
    public LanceIndexSyncRequest(String index) {
        this(index, NO_OBSERVED_VERSION);
    }

    /**
     * @param observedVersion the manifest version a request read the
     *     table at; the check is skipped when the mapping was derived
     *     at it already. {@link #NO_OBSERVED_VERSION} checks
     *     unconditionally.
     */
    public LanceIndexSyncRequest(String index, long observedVersion) {
        super(index);
        this.observedVersion = observedVersion;
    }

    public LanceIndexSyncRequest(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceIndexSyncRequest", WIRE_VERSION);
        this.observedVersion = reader.block(2, StreamInput::readLong, NO_OBSERVED_VERSION);
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        // A shard node of the previous version checks without the
        // version, one manifest read more than it needed; the answer is
        // the same, so the block is never critical.
        WireVersion.writeBlock(out, false, o -> o.writeLong(observedVersion));
    }

    /** The version the asking request read the table at, or {@link #NO_OBSERVED_VERSION}. */
    public long observedVersion() {
        return observedVersion;
    }

    @Override
    public ActionRequestValidationException validate() {
        return super.validateNonNullIndex();
    }
}

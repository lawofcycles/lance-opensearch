/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.lance.WireVersion;

/**
 * Transport response for
 * {@link LanceNamespaceUpdateAction}. Extends
 * {@link AcknowledgedResponse} with a {@link #changed()} bit so the
 * REST handler can tell the "already at target state" case (unregister
 * of an unknown path, register of an already-registered path) apart
 * from a real state transition. The distinction lets the REST layer
 * surface a 404 for {@code DELETE} on an unknown namespace without
 * hitting the local cluster-state cache — the cache on a follower may
 * lag a recent register from another node, so a decision made from
 * local state alone would be racy. Opens, after the acknowledged bit
 * the base class writes, with {@link #WIRE_VERSION} (see
 * {@link WireVersion}).
 */
public final class LanceNamespaceUpdateResponse extends AcknowledgedResponse {

    /** The wire format's version, the first field the response writes after its base class. */
    public static final int WIRE_VERSION = 1;

    private final boolean changed;

    public LanceNamespaceUpdateResponse(boolean acknowledged, boolean changed) {
        super(acknowledged);
        this.changed = changed;
    }

    public LanceNamespaceUpdateResponse(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceNamespaceUpdateResponse", WIRE_VERSION);
        this.changed = in.readBoolean();
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        out.writeBoolean(changed);
    }

    /**
     * True if the cluster state actually transitioned. False when
     * the request was a no-op: unregister of a path never
     * registered, or register of an already-registered path.
     */
    public boolean changed() {
        return changed;
    }
}

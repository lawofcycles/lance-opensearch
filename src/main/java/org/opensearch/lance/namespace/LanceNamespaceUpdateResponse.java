/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

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
 * local state alone would be racy.
 */
public final class LanceNamespaceUpdateResponse extends AcknowledgedResponse {

    private final boolean changed;

    public LanceNamespaceUpdateResponse(boolean acknowledged, boolean changed) {
        super(acknowledged);
        this.changed = changed;
    }

    public LanceNamespaceUpdateResponse(StreamInput in) throws IOException {
        super(in);
        this.changed = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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

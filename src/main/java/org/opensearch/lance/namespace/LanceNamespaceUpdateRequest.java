/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.ack.AckedRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.lance.StorageOptions;

/**
 * Transport request the plugin routes to the cluster manager to add
 * or remove an entry in {@link LanceNamespaceMetadata}. Wraps the
 * two mutations in a single request class so a shared
 * {@link org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction}
 * can serve them without duplicating the routing plumbing.
 */
public final class LanceNamespaceUpdateRequest extends ClusterManagerNodeRequest<LanceNamespaceUpdateRequest> implements AckedRequest {

    /** Which of the two supported mutations the manager should perform. */
    public enum Operation {
        REGISTER,
        UNREGISTER
    }

    private final Operation operation;
    private final String rootUri;
    private final StorageOptions storageOptions;

    /**
     * Register a namespace at {@code rootUri} with the given storage
     * options. Unused fields for unregister are ignored on the wire.
     */
    public static LanceNamespaceUpdateRequest register(String rootUri, StorageOptions storageOptions) {
        return applyLongTimeout(new LanceNamespaceUpdateRequest(Operation.REGISTER, rootUri, storageOptions));
    }

    public static LanceNamespaceUpdateRequest unregister(String rootUri) {
        return applyLongTimeout(new LanceNamespaceUpdateRequest(Operation.UNREGISTER, rootUri, StorageOptions.empty()));
    }

    private static LanceNamespaceUpdateRequest applyLongTimeout(LanceNamespaceUpdateRequest request) {
        // Widen the master-node timeout to match the ack timeout so
        // the caller does not get a 30s ProcessClusterEventTimeoutException
        // when the manager is queued behind namespace-poll CreateIndex
        // updates. 90s aligns with the plugin's own await window.
        request.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(90));
        return request;
    }

    private LanceNamespaceUpdateRequest(Operation operation, String rootUri, StorageOptions storageOptions) {
        this.operation = operation;
        this.rootUri = rootUri;
        this.storageOptions = storageOptions;
    }

    public LanceNamespaceUpdateRequest(StreamInput in) throws IOException {
        super(in);
        this.operation = Operation.values()[in.readVInt()];
        this.rootUri = in.readString();
        this.storageOptions = StorageOptions.readFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(operation.ordinal());
        out.writeString(rootUri);
        storageOptions.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        // Register / unregister both need a non-empty rootUri. Empty
        // rootUri would silently succeed against the empty-string
        // sentinel; explicit validation surfaces the bug early.
        if (rootUri == null || rootUri.isEmpty()) {
            ActionRequestValidationException ex = new ActionRequestValidationException();
            ex.addValidationError("rootUri must not be empty");
            return ex;
        }
        return null;
    }

    public Operation operation() {
        return operation;
    }

    public String rootUri() {
        return rootUri;
    }

    public StorageOptions storageOptions() {
        return storageOptions;
    }

    @Override
    public TimeValue ackTimeout() {
        // Give followers up to 90 seconds to acknowledge the state
        // change. Namespace polls on the manager occasionally batch
        // several CreateIndex updates ahead of a register /
        // unregister, so the ack has to wait behind them. The
        // plugin's own await loops align to the same window.
        return TimeValue.timeValueSeconds(90);
    }
}

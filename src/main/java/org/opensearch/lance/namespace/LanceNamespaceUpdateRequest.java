/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.util.Map;

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
    private final String name;
    private final String type;
    private final String rootUri;
    private final StorageOptions storageOptions;
    private final Map<String, String> config;
    /** Canonical JSON of the per-column mapping overrides, empty when none were declared. */
    private final String overridesJson;

    /**
     * Register a directory namespace at {@code rootUri}, named after
     * the root, with the given storage options and mapping overrides.
     */
    public static LanceNamespaceUpdateRequest register(String rootUri, StorageOptions storageOptions, String overridesJson) {
        return register(rootUri, LanceNamespaceMetadata.Entry.TYPE_DIRECTORY, rootUri, storageOptions, Map.of(), overridesJson);
    }

    /**
     * Register a namespace of any type. {@code rootUri} is the
     * directory root and null for catalog types whose root lives in
     * {@code config}; {@code config} carries the implementation's
     * initialize properties as-is, secrets included.
     */
    public static LanceNamespaceUpdateRequest register(
        String name,
        String type,
        String rootUri,
        StorageOptions storageOptions,
        Map<String, String> config,
        String overridesJson
    ) {
        return applyLongTimeout(
            new LanceNamespaceUpdateRequest(Operation.REGISTER, name, type, rootUri, storageOptions, config, overridesJson)
        );
    }

    /**
     * Remove the entry whose name is {@code identifier}, or, for
     * directory entries, whose root path is {@code identifier}.
     */
    public static LanceNamespaceUpdateRequest unregister(String identifier) {
        return applyLongTimeout(
            new LanceNamespaceUpdateRequest(Operation.UNREGISTER, identifier, null, null, StorageOptions.empty(), Map.of(), "")
        );
    }

    private static LanceNamespaceUpdateRequest applyLongTimeout(LanceNamespaceUpdateRequest request) {
        // Widen the master-node timeout to match the ack timeout so
        // the caller does not get a 30s ProcessClusterEventTimeoutException
        // when the manager is queued behind namespace-poll CreateIndex
        // updates. 90s aligns with the plugin's own await window.
        request.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(90));
        return request;
    }

    private LanceNamespaceUpdateRequest(
        Operation operation,
        String name,
        String type,
        String rootUri,
        StorageOptions storageOptions,
        Map<String, String> config,
        String overridesJson
    ) {
        this.operation = operation;
        this.name = name;
        this.type = type;
        this.rootUri = rootUri;
        this.storageOptions = storageOptions;
        this.config = config;
        this.overridesJson = overridesJson == null ? "" : overridesJson;
    }

    public LanceNamespaceUpdateRequest(StreamInput in) throws IOException {
        super(in);
        this.operation = Operation.values()[in.readVInt()];
        this.name = in.readString();
        this.type = in.readOptionalString();
        this.rootUri = in.readOptionalString();
        this.storageOptions = StorageOptions.readFromStream(in);
        this.config = in.readMap(StreamInput::readString, StreamInput::readString);
        this.overridesJson = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(operation.ordinal());
        out.writeString(name);
        out.writeOptionalString(type);
        out.writeOptionalString(rootUri);
        storageOptions.writeTo(out);
        out.writeMap(config, StreamOutput::writeString, StreamOutput::writeString);
        out.writeString(overridesJson);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException ex = null;
        if (name == null || name.isEmpty()) {
            ex = new ActionRequestValidationException();
            ex.addValidationError("name must not be empty");
        }
        if (operation == Operation.REGISTER) {
            if (type == null || !LanceNamespaceMetadata.Entry.ACCEPTED_TYPES.contains(type)) {
                if (ex == null) {
                    ex = new ActionRequestValidationException();
                }
                ex.addValidationError(
                    "unknown namespace type [" + type + "]; accepted values are " + LanceNamespaceMetadata.Entry.ACCEPTED_TYPES
                );
            }
            if (LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(type) && (rootUri == null || rootUri.isEmpty())) {
                if (ex == null) {
                    ex = new ActionRequestValidationException();
                }
                ex.addValidationError("a directory namespace requires a path");
            }
        }
        return ex;
    }

    public Operation operation() {
        return operation;
    }

    /** Registration name for register; the identifier (name or directory root) for unregister. */
    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public String rootUri() {
        return rootUri;
    }

    public StorageOptions storageOptions() {
        return storageOptions;
    }

    public Map<String, String> config() {
        return config;
    }

    /** Canonical overrides JSON, empty when the register call declared none. */
    public String overridesJson() {
        return overridesJson;
    }

    /** The metadata entry a register request describes. */
    public LanceNamespaceMetadata.Entry toEntry() {
        return new LanceNamespaceMetadata.Entry(name, type, rootUri, storageOptions, config, overridesJson);
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

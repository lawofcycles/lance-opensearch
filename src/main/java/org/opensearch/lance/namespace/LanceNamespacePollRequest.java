/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for {@link LanceNamespacePollAction}: run one catalog listing
 * cycle on the cluster manager, over every registration or over the one
 * {@link #name()} identifies (a registration name, or a directory
 * registration's path).
 */
public final class LanceNamespacePollRequest extends ClusterManagerNodeRequest<LanceNamespacePollRequest> {

    private final String name;

    /** @param name the registration to list, or {@code null} for every registration */
    public LanceNamespacePollRequest(String name) {
        this.name = name;
        // The cycle lists catalogs and waits for the indexes it creates;
        // the default 30 seconds are short for a large directory root.
        clusterManagerNodeTimeout(TimeValue.timeValueSeconds(90));
    }

    public LanceNamespacePollRequest(StreamInput in) throws IOException {
        super(in);
        this.name = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /** The registration to list, or {@code null} for every registration. */
    public String name() {
        return name;
    }
}

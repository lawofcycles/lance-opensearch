/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for {@link LanceExplainAction}: the index, the logical plan
 * and the physical plan the Volcano planner chose, each as
 * {@code RelOptUtil.toString} renders them, one node per line. The
 * text is for humans; its format will change as the planner grows
 * traits and costs.
 *
 * <p>The stream fields are read and written unconditionally, so the
 * wire format is not rolling upgrade safe; the plugin has no mixed
 * version story yet, as the backwards-compatibility policy on
 * {@link org.opensearch.lance.namespace.LanceNamespaceMetadata} spells
 * out.
 */
public final class LanceExplainResponse extends ActionResponse implements ToXContentObject {

    private final String index;
    private final String logical;
    private final String physical;

    public LanceExplainResponse(String index, String logical, String physical) {
        this.index = index;
        this.logical = logical;
        this.physical = physical;
    }

    public LanceExplainResponse(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.logical = in.readString();
        this.physical = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(logical);
        out.writeString(physical);
    }

    public String index() {
        return index;
    }

    public String logical() {
        return logical;
    }

    public String physical() {
        return physical;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("logical", logical);
        builder.field("physical", physical);
        return builder.endObject();
    }
}

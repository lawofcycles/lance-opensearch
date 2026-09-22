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
 * Response for {@link LanceExplainAction}: the index and the logical
 * plan as {@code RelOptUtil.toString} renders it, one node per line.
 * The text is for humans; its format will change as the planner grows
 * physical plans, traits and costs.
 */
public final class LanceExplainResponse extends ActionResponse implements ToXContentObject {

    private final String index;
    private final String logical;

    public LanceExplainResponse(String index, String logical) {
        this.index = index;
        this.logical = logical;
    }

    public LanceExplainResponse(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.logical = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(logical);
    }

    public String index() {
        return index;
    }

    public String logical() {
        return logical;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("logical", logical);
        return builder.endObject();
    }
}

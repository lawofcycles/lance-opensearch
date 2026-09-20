/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.io.IOException;
import java.util.List;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for {@link LanceBuildIndexesAction}: which Lance indexes were
 * built or optimised, echoing the column and fragment filters the caller
 * supplied. {@link #toXContent} produces the
 * {@code POST /_lance/build_indexes/{index}} body.
 */
public final class LanceBuildIndexesResponse extends ActionResponse implements ToXContentObject {

    private final String index;
    private final List<String> ftsBuilt;
    private final List<String> scalarBuilt;
    private final List<String> vectorBuilt;
    private final List<String> columnsFilter;
    private final List<Integer> fragmentIds;

    public LanceBuildIndexesResponse(
        String index,
        List<String> ftsBuilt,
        List<String> scalarBuilt,
        List<String> vectorBuilt,
        List<String> columnsFilter,
        List<Integer> fragmentIds
    ) {
        this.index = index;
        this.ftsBuilt = List.copyOf(ftsBuilt);
        this.scalarBuilt = List.copyOf(scalarBuilt);
        this.vectorBuilt = List.copyOf(vectorBuilt);
        this.columnsFilter = columnsFilter == null ? null : List.copyOf(columnsFilter);
        this.fragmentIds = fragmentIds == null ? null : List.copyOf(fragmentIds);
    }

    public LanceBuildIndexesResponse(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.ftsBuilt = in.readStringList();
        this.scalarBuilt = in.readStringList();
        this.vectorBuilt = in.readStringList();
        this.columnsFilter = in.readOptionalStringList();
        this.fragmentIds = in.readBoolean() ? in.readList(StreamInput::readVInt) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeStringCollection(ftsBuilt);
        out.writeStringCollection(scalarBuilt);
        out.writeStringCollection(vectorBuilt);
        out.writeOptionalStringCollection(columnsFilter);
        if (fragmentIds == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeCollection(fragmentIds, StreamOutput::writeVInt);
        }
    }

    public String index() {
        return index;
    }

    public List<String> ftsBuilt() {
        return ftsBuilt;
    }

    public List<String> scalarBuilt() {
        return scalarBuilt;
    }

    public List<String> vectorBuilt() {
        return vectorBuilt;
    }

    /** Column filter echoed from the request, or {@code null} when none was given. */
    public List<String> columnsFilter() {
        return columnsFilter;
    }

    /** Fragment filter echoed from the request, or {@code null} when none was given. */
    public List<Integer> fragmentIds() {
        return fragmentIds;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("index", index);
        b.startObject("built");
        b.field("fts", ftsBuilt);
        b.field("scalar", scalarBuilt);
        b.field("vector", vectorBuilt);
        b.endObject();
        if (columnsFilter != null) {
            b.field("columns_filter", columnsFilter);
        }
        if (fragmentIds != null) {
            b.field("fragment_ids", fragmentIds);
        }
        return b.endObject();
    }
}

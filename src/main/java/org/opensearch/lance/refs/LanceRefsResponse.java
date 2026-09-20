/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.refs;

import java.io.IOException;
import java.util.List;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for {@link LanceRefsAction}: the tags (name and the manifest
 * version each points at) and branches (name) of the Lance table behind an
 * index. {@link #toXContent} produces the {@code GET /_lance/refs/{index}}
 * body.
 */
public final class LanceRefsResponse extends ActionResponse implements ToXContentObject {

    /** One Lance tag: its name and the manifest version it points at. */
    public record Tag(String name, long version) implements Writeable {

        public Tag(StreamInput in) throws IOException {
            this(in.readString(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(version);
        }
    }

    private final String index;
    private final String table;
    private final List<Tag> tags;
    private final List<String> branches;

    public LanceRefsResponse(String index, String table, List<Tag> tags, List<String> branches) {
        this.index = index;
        this.table = table;
        this.tags = List.copyOf(tags);
        this.branches = List.copyOf(branches);
    }

    public LanceRefsResponse(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.table = in.readString();
        this.tags = in.readList(Tag::new);
        this.branches = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(table);
        out.writeList(tags);
        out.writeStringCollection(branches);
    }

    public String index() {
        return index;
    }

    public String table() {
        return table;
    }

    public List<Tag> tags() {
        return tags;
    }

    public List<String> branches() {
        return branches;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("index", index);
        b.field("table", table);
        b.startArray("tags");
        for (Tag tag : tags) {
            b.startObject();
            b.field("name", tag.name());
            b.field("version", tag.version());
            b.endObject();
        }
        b.endArray();
        b.startArray("branches");
        for (String branch : branches) {
            b.startObject();
            b.field("name", branch);
            b.endObject();
        }
        b.endArray();
        return b.endObject();
    }
}

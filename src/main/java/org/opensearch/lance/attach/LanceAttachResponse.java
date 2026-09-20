/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for {@link LanceAttachAction}: what the derivation found in
 * the table plus whether the index already existed for the same table.
 * {@link #toXContent} produces the {@code POST /_lance/attach} body.
 */
public final class LanceAttachResponse extends ActionResponse implements ToXContentObject {

    private final String index;
    private final String table;
    private final long version;
    private final long rows;
    private final int fragments;
    private final String derivedKeyField;
    private final String derivedMappingJson;
    private final List<String> notes;
    private final boolean alreadyAttached;

    public LanceAttachResponse(
        String index,
        String table,
        long version,
        long rows,
        int fragments,
        String derivedKeyField,
        String derivedMappingJson,
        List<String> notes,
        boolean alreadyAttached
    ) {
        this.index = index;
        this.table = table;
        this.version = version;
        this.rows = rows;
        this.fragments = fragments;
        this.derivedKeyField = derivedKeyField;
        this.derivedMappingJson = derivedMappingJson;
        this.notes = List.copyOf(notes);
        this.alreadyAttached = alreadyAttached;
    }

    public LanceAttachResponse(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.table = in.readString();
        this.version = in.readLong();
        this.rows = in.readLong();
        this.fragments = in.readVInt();
        this.derivedKeyField = in.readString();
        this.derivedMappingJson = in.readString();
        this.notes = in.readStringList();
        this.alreadyAttached = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(table);
        out.writeLong(version);
        out.writeLong(rows);
        out.writeVInt(fragments);
        out.writeString(derivedKeyField);
        out.writeString(derivedMappingJson);
        out.writeStringCollection(notes);
        out.writeBoolean(alreadyAttached);
    }

    public String index() {
        return index;
    }

    public String table() {
        return table;
    }

    public long version() {
        return version;
    }

    public long rows() {
        return rows;
    }

    public int fragments() {
        return fragments;
    }

    public String derivedKeyField() {
        return derivedKeyField;
    }

    public String derivedMappingJson() {
        return derivedMappingJson;
    }

    public List<String> notes() {
        return notes;
    }

    public boolean alreadyAttached() {
        return alreadyAttached;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("index", index);
        b.field("table", table);
        b.field("version", version);
        b.field("rows", rows);
        b.field("fragments", fragments);
        b.field("derived_key_field", derivedKeyField);
        b.rawField(
            "derived_mapping",
            new ByteArrayInputStream(derivedMappingJson.getBytes(StandardCharsets.UTF_8)),
            MediaTypeRegistry.JSON
        );
        b.field("notes", notes);
        b.field("already_attached", alreadyAttached);
        return b.endObject();
    }
}

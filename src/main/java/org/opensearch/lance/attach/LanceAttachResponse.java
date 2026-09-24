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
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.WireVersion;

/**
 * Response for {@link LanceAttachAction}: what the derivation found in
 * the table plus whether the index already existed for the same table.
 * {@link #toXContent} produces the {@code POST /_lance/attach} body.
 * Opens with {@link #WIRE_VERSION} (see {@link WireVersion}).
 */
public final class LanceAttachResponse extends ActionResponse implements ToXContentObject {

    /** The wire format's version, the first field the response writes. */
    public static final int WIRE_VERSION = 1;

    private final String index;
    private final String table;
    private final long version;
    private final long rows;
    private final int fragments;
    private final String derivedKeyField;
    private final String derivedMappingJson;
    private final List<String> notes;
    private final boolean alreadyAttached;
    // Whether the table has more rows than one Lucene reader may hold, so
    // the shard reader serves part of it and searches run in fragment
    // groups; rendered only when true.
    private final boolean luceneBoundExceeded;
    // What the text_analyzer backfill started by a `derive: async`
    // attach is going to do; null when the attach started none.
    private final Backfill backfill;

    /**
     * The {@code backfill} object of a {@code derive: async} attach.
     *
     * @param estimatedBytes about how many bytes the derived tokens
     *     columns add to the table
     * @param spoolPath where the backfill spools; {@code "none"}
     *     because it streams into the table without a local spool
     * @param threads how many threads tokenize at once
     */
    public record Backfill(long estimatedBytes, String spoolPath, int threads) implements Writeable, ToXContentObject {

        public Backfill(StreamInput in) throws IOException {
            this(in.readVLong(), in.readString(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(estimatedBytes);
            out.writeString(spoolPath);
            out.writeVInt(threads);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
            b.startObject();
            b.field("estimated_bytes", estimatedBytes);
            b.field("spool_path", spoolPath);
            b.field("threads", threads);
            return b.endObject();
        }
    }

    public LanceAttachResponse(
        String index,
        String table,
        long version,
        long rows,
        int fragments,
        String derivedKeyField,
        String derivedMappingJson,
        List<String> notes,
        boolean alreadyAttached,
        boolean luceneBoundExceeded
    ) {
        this(
            index,
            table,
            version,
            rows,
            fragments,
            derivedKeyField,
            derivedMappingJson,
            notes,
            alreadyAttached,
            luceneBoundExceeded,
            null
        );
    }

    public LanceAttachResponse(
        String index,
        String table,
        long version,
        long rows,
        int fragments,
        String derivedKeyField,
        String derivedMappingJson,
        List<String> notes,
        boolean alreadyAttached,
        boolean luceneBoundExceeded,
        Backfill backfill
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
        this.luceneBoundExceeded = luceneBoundExceeded;
        this.backfill = backfill;
    }

    public LanceAttachResponse(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceAttachResponse", WIRE_VERSION);
        this.index = in.readString();
        this.table = in.readString();
        this.version = in.readLong();
        this.rows = in.readLong();
        this.fragments = in.readVInt();
        this.derivedKeyField = in.readString();
        this.derivedMappingJson = in.readString();
        this.notes = in.readStringList();
        this.alreadyAttached = in.readBoolean();
        this.luceneBoundExceeded = in.readBoolean();
        this.backfill = in.readOptionalWriteable(Backfill::new);
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        WireVersion.write(out, WIRE_VERSION);
        out.writeString(index);
        out.writeString(table);
        out.writeLong(version);
        out.writeLong(rows);
        out.writeVInt(fragments);
        out.writeString(derivedKeyField);
        out.writeString(derivedMappingJson);
        out.writeStringCollection(notes);
        out.writeBoolean(alreadyAttached);
        out.writeBoolean(luceneBoundExceeded);
        out.writeOptionalWriteable(backfill);
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

    public boolean luceneBoundExceeded() {
        return luceneBoundExceeded;
    }

    /** The backfill a {@code derive: async} attach started, or {@code null}. */
    public Backfill backfill() {
        return backfill;
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
        if (luceneBoundExceeded) {
            b.field("lucene_bound_exceeded", true);
        }
        if (backfill != null) {
            b.field("backfill", backfill);
        }
        return b.endObject();
    }
}

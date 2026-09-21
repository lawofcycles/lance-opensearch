/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.engine.LanceIndexWarmer;

/**
 * One table's index warm-up as {@code GET /_lance/stats} reports it: the
 * OpenSearch index and Lance table, the manifest version the warm-up
 * read, the mode it ran under, its state, when it started, how long it
 * took, and one {@link IndexEntry} per Lance index it visited. Built by
 * {@link LanceStatsCollector} from {@link LanceIndexWarmer.TableStatus};
 * the strings are the lower case names of the warmer's enums so the
 * wire format does not depend on their ordinals.
 */
public final class LanceWarmUpStatus implements Writeable, ToXContentObject {

    /** One Lance index inside a table's warm-up. */
    public record IndexEntry(String name, String type, String column, String state, double seconds, String detail)
        implements
            Writeable,
            ToXContentObject {

        static IndexEntry read(StreamInput in) throws IOException {
            return new IndexEntry(in.readString(), in.readString(), in.readString(), in.readString(), in.readDouble(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(type);
            out.writeString(column);
            out.writeString(state);
            out.writeDouble(seconds);
            out.writeString(detail);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", name);
            builder.field("type", type);
            builder.field("column", column);
            builder.field("state", state);
            builder.field("seconds", roundSeconds(seconds));
            if (!detail.isEmpty()) {
                builder.field("detail", detail);
            }
            return builder.endObject();
        }
    }

    private final String index;
    private final String table;
    private final long version;
    private final String mode;
    private final String state;
    private final long startedAtMillis;
    private final double seconds;
    private final List<IndexEntry> indexes;

    public LanceWarmUpStatus(
        String index,
        String table,
        long version,
        String mode,
        String state,
        long startedAtMillis,
        double seconds,
        List<IndexEntry> indexes
    ) {
        this.index = index;
        this.table = table;
        this.version = version;
        this.mode = mode;
        this.state = state;
        this.startedAtMillis = startedAtMillis;
        this.seconds = seconds;
        this.indexes = List.copyOf(indexes);
    }

    public LanceWarmUpStatus(StreamInput in) throws IOException {
        this.index = in.readString();
        this.table = in.readString();
        this.version = in.readLong();
        this.mode = in.readString();
        this.state = in.readString();
        this.startedAtMillis = in.readLong();
        this.seconds = in.readDouble();
        int count = in.readVInt();
        List<IndexEntry> read = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            read.add(IndexEntry.read(in));
        }
        this.indexes = List.copyOf(read);
    }

    /** Translate the warmer's view into the stats shape. */
    public static LanceWarmUpStatus of(LanceIndexWarmer.TableStatus status) {
        List<IndexEntry> entries = new ArrayList<>(status.indexes().size());
        for (LanceIndexWarmer.IndexStatus indexStatus : status.indexes()) {
            entries.add(
                new IndexEntry(
                    indexStatus.name(),
                    indexStatus.type(),
                    indexStatus.column(),
                    indexStatus.state().value(),
                    indexStatus.seconds(),
                    indexStatus.detail() == null ? "" : indexStatus.detail()
                )
            );
        }
        return new LanceWarmUpStatus(
            status.index(),
            status.table(),
            status.version(),
            status.mode().settingValue(),
            status.state().value(),
            status.startedAtMillis(),
            status.seconds(),
            entries
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(table);
        out.writeLong(version);
        out.writeString(mode);
        out.writeString(state);
        out.writeLong(startedAtMillis);
        out.writeDouble(seconds);
        out.writeVInt(indexes.size());
        for (IndexEntry entry : indexes) {
            entry.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("table", table);
        builder.field("version", version);
        builder.field("mode", mode);
        builder.field("state", state);
        if (startedAtMillis > 0L) {
            builder.field("started_at", Instant.ofEpochMilli(startedAtMillis).toString());
        }
        builder.field("seconds", roundSeconds(seconds));
        builder.startArray("indexes");
        for (IndexEntry entry : indexes) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    /** Hundredths of a second are enough to read a warm-up; the raw double travels on the wire. */
    private static double roundSeconds(double seconds) {
        return Math.round(seconds * 100d) / 100d;
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

    public String mode() {
        return mode;
    }

    public String state() {
        return state;
    }

    public long startedAtMillis() {
        return startedAtMillis;
    }

    public double seconds() {
        return seconds;
    }

    public List<IndexEntry> indexes() {
        return indexes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceWarmUpStatus other)) {
            return false;
        }
        return index.equals(other.index)
            && table.equals(other.table)
            && version == other.version
            && mode.equals(other.mode)
            && state.equals(other.state)
            && startedAtMillis == other.startedAtMillis
            && Double.compare(seconds, other.seconds) == 0
            && indexes.equals(other.indexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, table, version, mode, state, startedAtMillis, seconds, indexes);
    }
}

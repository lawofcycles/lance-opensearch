/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import org.opensearch.Version;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.lance.StorageOptions;

/**
 * Cluster-state representation of the Lance namespaces the plugin
 * polls for new tables. Stored as {@link Metadata.Custom} so every
 * node in the cluster sees the same list without a separate transport
 * broadcast: registering a namespace on any node submits a cluster
 * state update task that adds a matching {@link Entry} here, and the
 * updated {@link Metadata} propagates through OpenSearch's cluster
 * state applier chain to every follower.
 *
 * <p>The entry payload only carries the {@code rootUri} and the
 * {@link StorageOptions} the coordinator needs to open the Lance
 * catalog on each node. The runtime
 * {@link org.lance.namespace.DirectoryNamespace} handle is
 * reconstructed lazily on the node that actually polls the
 * namespace; the metadata makes no attempt to serialise the native
 * catalog object.
 *
 * <p>Backwards-compatibility policy: because the version wire is
 * {@link Version#V_3_8_0} (matching the OpenSearch release this
 * plugin ships against), nodes downgraded past that point will
 * silently drop the metadata. In practice the plugin has no
 * downgrade story yet, so this is acceptable.
 */
public final class LanceNamespaceMetadata implements Metadata.Custom {

    public static final String TYPE = "lance.namespaces";
    public static final LanceNamespaceMetadata EMPTY = new LanceNamespaceMetadata(Collections.emptyList());

    private static final ParseField ENTRIES = new ParseField("entries");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<LanceNamespaceMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        args -> new LanceNamespaceMetadata((List<Entry>) args[0])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (parser, ctx) -> Entry.fromXContent(parser), ENTRIES);
    }

    private final List<Entry> entries;

    public LanceNamespaceMetadata(List<Entry> entries) {
        this.entries = List.copyOf(entries);
    }

    public LanceNamespaceMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<Entry> read = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            read.add(new Entry(in));
        }
        this.entries = List.copyOf(read);
    }

    public List<Entry> entries() {
        return entries;
    }

    /**
     * Return a copy of this metadata with {@code entry} added if no
     * existing entry already matches its {@link Entry#rootUri()}.
     * Returns {@code this} unchanged when the URI is already
     * registered, letting the cluster state update task detect a
     * no-op and skip the publication.
     */
    public LanceNamespaceMetadata withRegistered(Entry entry) {
        for (Entry existing : entries) {
            if (existing.rootUri().equals(entry.rootUri())) {
                return this;
            }
        }
        List<Entry> next = new ArrayList<>(entries.size() + 1);
        next.addAll(entries);
        next.add(entry);
        return new LanceNamespaceMetadata(next);
    }

    /**
     * Return a copy with the entry matching {@code rootUri} removed.
     * Returns {@code this} unchanged when nothing matched, again so
     * the update task can identify a no-op.
     */
    public LanceNamespaceMetadata withUnregistered(String rootUri) {
        List<Entry> next = new ArrayList<>(entries.size());
        boolean removed = false;
        for (Entry existing : entries) {
            if (!existing.rootUri().equals(rootUri)) {
                next.add(existing);
            } else {
                removed = true;
            }
        }
        return removed ? new LanceNamespaceMetadata(next) : this;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_8_0;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        // Namespaces belong in gateway state so the plugin remembers
        // registrations across full cluster restarts. Snapshots
        // deliberately omit them: a restored cluster on a different
        // node might not have file-system access to the recorded
        // rootUri, so a manual re-registration is safer than blindly
        // materialising indexes for missing paths.
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.API);
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        // Full-state diffs would be an easy optimisation; the total
        // payload is bounded by the number of registered namespaces
        // times the URI + options size, so replicating whole every
        // update is fine for realistic operator scales.
        return new LanceNamespaceMetadataDiff((LanceNamespaceMetadata) previousState, this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            entry.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, org.opensearch.core.xcontent.ToXContent.Params params) throws IOException {
        builder.startArray(ENTRIES.getPreferredName());
        for (Entry entry : entries) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public static LanceNamespaceMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new LanceNamespaceMetadataDiff(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LanceNamespaceMetadata other)) return false;
        return entries.equals(other.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        return "LanceNamespaceMetadata{entries=" + entries + "}";
    }

    /**
     * One registered namespace. Kept minimal: only what a fresh
     * node needs to reconstruct the DirectoryNamespace on demand.
     */
    public static final class Entry implements Writeable {

        private static final ParseField ROOT_URI = new ParseField("root_uri");
        private static final ParseField STORAGE_OPTIONS = new ParseField("storage_options");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Entry, Void> ENTRY_PARSER = new ConstructingObjectParser<>(
            "lance_namespace_entry",
            false,
            args -> new Entry((String) args[0], StorageOptions.of((java.util.Map<String, String>) args[1]))
        );

        static {
            ENTRY_PARSER.declareString(ConstructingObjectParser.constructorArg(), ROOT_URI);
            ENTRY_PARSER.declareObject(ConstructingObjectParser.constructorArg(), (parser, ctx) -> parser.mapStrings(), STORAGE_OPTIONS);
        }

        private final String rootUri;
        private final StorageOptions storageOptions;

        public Entry(String rootUri, StorageOptions storageOptions) {
            this.rootUri = Objects.requireNonNull(rootUri, "rootUri");
            this.storageOptions = Objects.requireNonNull(storageOptions, "storageOptions");
        }

        public Entry(StreamInput in) throws IOException {
            this.rootUri = in.readString();
            this.storageOptions = StorageOptions.readFromStream(in);
        }

        public String rootUri() {
            return rootUri;
        }

        public StorageOptions storageOptions() {
            return storageOptions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(rootUri);
            storageOptions.writeTo(out);
        }

        public XContentBuilder toXContent(XContentBuilder builder, org.opensearch.core.xcontent.ToXContent.Params params)
            throws IOException {
            builder.startObject();
            builder.field(ROOT_URI.getPreferredName(), rootUri);
            builder.field(STORAGE_OPTIONS.getPreferredName(), storageOptions.asMap());
            builder.endObject();
            return builder;
        }

        public static Entry fromXContent(XContentParser parser) throws IOException {
            return ENTRY_PARSER.parse(parser, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Entry other)) return false;
            return rootUri.equals(other.rootUri) && storageOptions.equals(other.storageOptions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rootUri, storageOptions);
        }

        @Override
        public String toString() {
            return "Entry{rootUri=" + rootUri + ", storageOptions=" + storageOptions + "}";
        }
    }

    /**
     * Diff wire type registered alongside {@link LanceNamespaceMetadata}.
     * Sending the whole target state instead of a delta keeps the
     * implementation trivial; realistic namespace counts stay well
     * under any threshold that would justify a smarter encoding.
     */
    public static final class LanceNamespaceMetadataDiff implements NamedDiff<Metadata.Custom>, NamedWriteable {

        private final LanceNamespaceMetadata target;

        public LanceNamespaceMetadataDiff(LanceNamespaceMetadata previous, LanceNamespaceMetadata target) {
            this.target = target;
        }

        public LanceNamespaceMetadataDiff(StreamInput in) throws IOException {
            this.target = new LanceNamespaceMetadata(in);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return target;
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            target.writeTo(out);
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_3_8_0;
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

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
 * <p>The entry payload carries the registration {@code name} (the key
 * every mutation identifies an entry by), the catalog {@code type}
 * (one of {@link Entry#ACCEPTED_TYPES}), the
 * {@code rootUri} for directory catalogs, the {@code config}
 * properties handed to the implementation's {@code initialize}, and
 * the {@link StorageOptions} the poll needs to open the Lance tables
 * the catalog names. The runtime
 * {@link org.lance.namespace.LanceNamespace} handle is reconstructed
 * lazily on the node that actually polls the namespace; the metadata
 * makes no attempt to serialise the native catalog object.
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

    /**
     * Wire format marker written ahead of the entry list. The initial
     * format carried no marker and started directly with the entry
     * count, so the reader treats any leading value other than the
     * current marker as that old count and reads the remaining stream
     * in the old shape (rootUri + storage options per entry). The
     * marker value is deliberately far above any realistic entry
     * count so an old stream is never mistaken for a new one.
     */
    static final int WIRE_FORMAT_VERSION = 0xFFFF_0002;

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
        int first = in.readVInt();
        List<Entry> read;
        if (first == WIRE_FORMAT_VERSION) {
            int size = in.readVInt();
            read = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                read.add(new Entry(in));
            }
        } else {
            // Old-format stream: the leading value is the entry count and
            // each entry is rootUri + storage options. Old entries are
            // directory registrations named after their root.
            read = new ArrayList<>(first);
            for (int i = 0; i < first; i++) {
                String rootUri = in.readString();
                StorageOptions storageOptions = StorageOptions.readFromStream(in);
                String overridesJson = in.readString();
                read.add(new Entry(rootUri, storageOptions, overridesJson));
            }
        }
        this.entries = List.copyOf(read);
    }

    public List<Entry> entries() {
        return entries;
    }

    /**
     * Return a copy of this metadata with {@code entry} added if no
     * existing entry already matches its {@link Entry#name()} — or,
     * for a directory entry, its {@link Entry#rootUri()}, so
     * re-registering the same path stays a no-op even under a custom
     * name. Returns {@code this} unchanged when a match exists,
     * letting the cluster state update task detect a no-op and skip
     * the publication.
     */
    public LanceNamespaceMetadata withRegistered(Entry entry) {
        for (Entry existing : entries) {
            if (existing.name().equals(entry.name())) {
                return this;
            }
            if (Entry.TYPE_DIRECTORY.equals(existing.type())
                && Entry.TYPE_DIRECTORY.equals(entry.type())
                && Objects.equals(existing.rootUri(), entry.rootUri())) {
                return this;
            }
        }
        List<Entry> next = new ArrayList<>(entries.size() + 1);
        next.addAll(entries);
        next.add(entry);
        return new LanceNamespaceMetadata(next);
    }

    /**
     * Return a copy with the entry matching {@code identifier}
     * removed. The identifier matches an entry's {@link Entry#name()}
     * for every type, or a directory entry's {@link Entry#rootUri()}
     * so the pre-{@code name} calling convention (DELETE by path)
     * keeps working. Returns {@code this} unchanged when nothing
     * matched, again so the update task can identify a no-op.
     */
    public LanceNamespaceMetadata withUnregistered(String identifier) {
        List<Entry> next = new ArrayList<>(entries.size());
        boolean removed = false;
        for (Entry existing : entries) {
            if (existing.matchesIdentifier(identifier)) {
                removed = true;
            } else {
                next.add(existing);
            }
        }
        return removed ? new LanceNamespaceMetadata(next) : this;
    }

    /** The entry matching {@code identifier} (name, or directory root), or {@code null}. */
    public Entry findByIdentifier(String identifier) {
        for (Entry existing : entries) {
            if (existing.matchesIdentifier(identifier)) {
                return existing;
            }
        }
        return null;
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
        out.writeVInt(WIRE_FORMAT_VERSION);
        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            entry.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, org.opensearch.core.xcontent.ToXContent.Params params) throws IOException {
        // Gateway persistence must keep the raw config values so a full
        // cluster restart can re-initialise the catalogs; every other
        // context (the cluster state API in particular) gets the
        // redacted view so credential-bearing keys never leave the node.
        boolean redact = !Metadata.CONTEXT_MODE_GATEWAY.equals(params.param(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API));
        builder.startArray(ENTRIES.getPreferredName());
        for (Entry entry : entries) {
            entry.toXContent(builder, redact);
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
     * One registered namespace: the registration name, the catalog
     * type, the directory root (directory type only), the storage
     * options for opening the tables the catalog names, and the
     * properties handed to the implementation's {@code initialize}.
     */
    public static final class Entry implements Writeable {

        public static final String TYPE_DIRECTORY = "directory";
        public static final String TYPE_REST = "rest";
        public static final String TYPE_GLUE = "glue";
        public static final String TYPE_ICEBERG = "iceberg";
        public static final String TYPE_POLARIS = "polaris";
        public static final String TYPE_UNITY = "unity";

        /** Accepted values for {@link #type()}, in the order register error messages list them. */
        public static final List<String> ACCEPTED_TYPES = List.of(
            TYPE_DIRECTORY,
            TYPE_REST,
            TYPE_GLUE,
            TYPE_ICEBERG,
            TYPE_POLARIS,
            TYPE_UNITY
        );

        private static final ParseField NAME = new ParseField("name");
        private static final ParseField CATALOG_TYPE = new ParseField("type");
        private static final ParseField ROOT_URI = new ParseField("root_uri");
        private static final ParseField STORAGE_OPTIONS = new ParseField("storage_options");
        private static final ParseField CONFIG = new ParseField("config");
        private static final ParseField OVERRIDES = new ParseField("overrides");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Entry, Void> ENTRY_PARSER = new ConstructingObjectParser<>(
            "lance_namespace_entry",
            false,
            args -> {
                String rootUri = (String) args[0];
                Map<String, String> storageOptions = (Map<String, String>) args[1];
                String overridesJson = (String) args[2];
                String name = (String) args[3];
                String type = (String) args[4];
                Map<String, String> config = (Map<String, String>) args[5];
                return new Entry(
                    name == null ? rootUri : name,
                    type == null ? TYPE_DIRECTORY : type,
                    rootUri,
                    StorageOptions.of(storageOptions == null ? Map.of() : storageOptions),
                    config == null ? Map.of() : config,
                    overridesJson == null ? "" : overridesJson
                );
            }
        );

        static {
            ENTRY_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ROOT_URI);
            ENTRY_PARSER.declareObject(
                ConstructingObjectParser.optionalConstructorArg(),
                (parser, ctx) -> parser.mapStrings(),
                STORAGE_OPTIONS
            );
            // Optional so gateway state written before the field existed
            // still parses.
            ENTRY_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), OVERRIDES);
            ENTRY_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME);
            ENTRY_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), CATALOG_TYPE);
            ENTRY_PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (parser, ctx) -> parser.mapStrings(), CONFIG);
        }

        private final String name;
        private final String type;
        private final String rootUri;
        private final StorageOptions storageOptions;
        private final Map<String, String> config;
        /**
         * Canonical JSON of the per-column mapping overrides the
         * register call declared (see
         * {@link org.opensearch.lance.LanceOverrides}), applied to
         * every table the poll surfaces from this catalog. Empty when
         * none were declared.
         */
        private final String overridesJson;

        /** Directory registration named after its root, with no extra config or overrides. */
        public Entry(String rootUri, StorageOptions storageOptions) {
            this(rootUri, TYPE_DIRECTORY, rootUri, storageOptions, Map.of(), "");
        }

        /** Directory registration named after its root, carrying mapping overrides. */
        public Entry(String rootUri, StorageOptions storageOptions, String overridesJson) {
            this(rootUri, TYPE_DIRECTORY, rootUri, storageOptions, Map.of(), overridesJson);
        }

        /** Registration without mapping overrides. */
        public Entry(String name, String type, String rootUri, StorageOptions storageOptions, Map<String, String> config) {
            this(name, type, rootUri, storageOptions, config, "");
        }

        public Entry(
            String name,
            String type,
            String rootUri,
            StorageOptions storageOptions,
            Map<String, String> config,
            String overridesJson
        ) {
            this.name = Objects.requireNonNull(name, "name");
            this.type = Objects.requireNonNull(type, "type");
            if (!ACCEPTED_TYPES.contains(type)) {
                throw new IllegalArgumentException("unknown namespace type [" + type + "]; accepted values are " + ACCEPTED_TYPES);
            }
            if (TYPE_DIRECTORY.equals(type) && (rootUri == null || rootUri.isEmpty())) {
                throw new IllegalArgumentException("a directory namespace requires a root path");
            }
            this.rootUri = rootUri;
            this.storageOptions = Objects.requireNonNull(storageOptions, "storageOptions");
            // TreeMap so equals / toXContent do not depend on caller order.
            this.config = Collections.unmodifiableMap(new TreeMap<>(Objects.requireNonNull(config, "config")));
            this.overridesJson = overridesJson == null ? "" : overridesJson;
        }

        public Entry(StreamInput in) throws IOException {
            this.name = in.readString();
            String readType = in.readString();
            if (!ACCEPTED_TYPES.contains(readType)) {
                // The primary constructor validates the same way; a wire
                // value outside the accepted set means a corrupted stream
                // or a sender with a type this build does not know.
                throw new IOException("unknown namespace type [" + readType + "] on the wire; accepted values are " + ACCEPTED_TYPES);
            }
            this.type = readType;
            this.rootUri = in.readOptionalString();
            this.storageOptions = StorageOptions.readFromStream(in);
            this.config = Collections.unmodifiableMap(new TreeMap<>(in.readMap(StreamInput::readString, StreamInput::readString)));
            this.overridesJson = in.readString();
        }

        /** Registration key, unique across all types. Defaults to the root path for directory entries. */
        public String name() {
            return name;
        }

        /** The catalog type, one of {@link #ACCEPTED_TYPES}. */
        public String type() {
            return type;
        }

        /** Directory root path, or {@code null} for catalog types whose root lives in {@link #config()}. */
        public String rootUri() {
            return rootUri;
        }

        public StorageOptions storageOptions() {
            return storageOptions;
        }

        /** Raw initialize properties, secrets included. Use {@link #redactedConfig()} for anything user-facing. */
        public Map<String, String> config() {
            return config;
        }

        /** Canonical overrides JSON applied to every surfaced table, or empty. */
        public String overridesJson() {
            return overridesJson;
        }

        /** True when {@code identifier} is this entry's name, or its root for a directory entry. */
        boolean matchesIdentifier(String identifier) {
            return name.equals(identifier) || (TYPE_DIRECTORY.equals(type) && Objects.equals(rootUri, identifier));
        }

        /**
         * True for config keys whose value must never appear in logs,
         * listings, or {@code toString}: anything whose name contains
         * {@code secret}, {@code password}, {@code token}, {@code key},
         * {@code authorization} or {@code credential} (case-insensitive).
         * {@code authorization} covers the {@code header.Authorization}
         * property the REST catalog client reads its bearer credential
         * from; {@code credential} covers the Iceberg REST client's
         * {@code credential} property (an OAuth client id and secret
         * pair), which none of the other substrings match.
         */
        public static boolean isSensitiveConfigKey(String key) {
            String lower = key.toLowerCase(Locale.ROOT);
            return lower.contains("secret")
                || lower.contains("password")
                || lower.contains("token")
                || lower.contains("key")
                || lower.contains("authorization")
                || lower.contains("credential");
        }

        /** The config map with every sensitive value replaced by {@code ***}. */
        public Map<String, String> redactedConfig() {
            if (config.isEmpty()) {
                return config;
            }
            Map<String, String> redacted = new LinkedHashMap<>(config.size());
            for (Map.Entry<String, String> entry : config.entrySet()) {
                redacted.put(entry.getKey(), isSensitiveConfigKey(entry.getKey()) ? "***" : entry.getValue());
            }
            return Collections.unmodifiableMap(redacted);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(type);
            out.writeOptionalString(rootUri);
            storageOptions.writeTo(out);
            out.writeMap(config, StreamOutput::writeString, StreamOutput::writeString);
            out.writeString(overridesJson);
        }

        public XContentBuilder toXContent(XContentBuilder builder, boolean redact) throws IOException {
            builder.startObject();
            builder.field(NAME.getPreferredName(), name);
            builder.field(CATALOG_TYPE.getPreferredName(), type);
            if (rootUri != null) {
                builder.field(ROOT_URI.getPreferredName(), rootUri);
            }
            builder.field(STORAGE_OPTIONS.getPreferredName(), storageOptions.asMap());
            if (!overridesJson.isEmpty()) {
                builder.field(OVERRIDES.getPreferredName(), overridesJson);
            }
            builder.field(CONFIG.getPreferredName(), redact ? redactedConfig() : config);
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
            return name.equals(other.name)
                && type.equals(other.type)
                && Objects.equals(rootUri, other.rootUri)
                && storageOptions.equals(other.storageOptions)
                && config.equals(other.config)
                && overridesJson.equals(other.overridesJson);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, rootUri, storageOptions, config, overridesJson);
        }

        @Override
        public String toString() {
            return "Entry{name="
                + name
                + ", type="
                + type
                + ", rootUri="
                + rootUri
                + ", storageOptions="
                + storageOptions
                + ", config="
                + redactedConfig()
                + ", overrides="
                + overridesJson
                + "}";
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

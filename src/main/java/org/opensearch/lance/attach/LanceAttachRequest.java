/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.WireVersion;

/**
 * Request for {@link LanceAttachAction}: the parsed body of
 * {@code POST /_plugins/_lance/attach}.
 *
 * <p>{@link #PARSER} declares the body's top level fields once:
 * {@code table}, {@code name}, {@code version}, {@code tag},
 * {@code storage_options}, {@code overrides} and {@code multi_fields}.
 * A field outside that list is an {@link org.opensearch.core.xcontent.XContentParseException}
 * naming it, which the REST layer answers with 400; {@code number_of_shards}
 * is declared only to refuse it with the reason (a Lance index is always
 * one shard and the fragment fan out does not depend on the shard count).
 * The parser fills a {@link Builder}; {@link Builder#build} runs the
 * checks that need more than one field ({@code version} and {@code tag}
 * are exclusive) and the parsing of the object valued fields.
 *
 * <p>A {@link ClusterManagerNodeRequest} so the transport action can
 * forward it to the elected cluster manager; the inherited
 * {@code clusterManagerNodeTimeout} bounds how long the node that
 * received the REST call waits for a manager to be known. Opens with
 * {@link #WIRE_VERSION} (see {@link WireVersion}).
 */
public final class LanceAttachRequest extends ClusterManagerNodeRequest<LanceAttachRequest> {

    /** The wire format's version, the first field the request writes after its base class. */
    public static final int WIRE_VERSION = 1;

    /** The name the parser reports in its messages, the same as the REST handler's. */
    public static final String PARSER_NAME = "lance_attach";

    /** The body of {@code POST /_plugins/_lance/attach}; see the class comment for the fields. */
    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(PARSER_NAME, Builder::new);

    static {
        PARSER.declareString(Builder::table, new ParseField("table"));
        PARSER.declareString(Builder::indexName, new ParseField("name"));
        PARSER.declareLong(Builder::pinnedVersion, new ParseField("version"));
        PARSER.declareString(Builder::tag, new ParseField("tag"));
        PARSER.declareObject(Builder::storageOptions, (p, c) -> p.map(), new ParseField("storage_options"));
        PARSER.declareObject(Builder::overrides, (p, c) -> p.map(), new ParseField("overrides"));
        PARSER.declareObject(Builder::multiFields, (p, c) -> p.map(), new ParseField("multi_fields"));
        // Declared so the operator reads why the key is refused, not just
        // that it is unknown. Any value shape is accepted up to this
        // point; the exception carries the reason.
        PARSER.declareField((XContentParser p, Builder b, Void c) -> {
            throw new IllegalArgumentException(
                "[number_of_shards] is no longer accepted by /_plugins/_lance/attach; the fragment path fans out at the fragment "
                    + "level regardless of shard count, and Lance-backed indices are always single-shard"
            );
        }, new ParseField("number_of_shards"), ObjectParser.ValueType.VALUE_OBJECT_ARRAY);
    }

    /**
     * Collects the fields of an attach body as {@link #PARSER} reads them
     * and turns them into a {@link LanceAttachRequest}. {@link #build}
     * throws {@link IllegalArgumentException} with the message the
     * operator sees as a 400 when {@code table} is missing, {@code version}
     * is negative, {@code tag} is empty, both {@code version} and
     * {@code tag} are given, or {@code storage_options} / {@code overrides}
     * / {@code multi_fields} fail their structural checks.
     */
    public static final class Builder {

        private String table;
        private String indexName;
        private Long pinnedVersion;
        private String tag;
        private Map<String, Object> storageOptions;
        private Map<String, Object> overrides;
        private Map<String, Object> multiFields;

        public Builder() {}

        private void table(String table) {
            this.table = table;
        }

        private void indexName(String indexName) {
            this.indexName = indexName;
        }

        private void pinnedVersion(Long pinnedVersion) {
            this.pinnedVersion = pinnedVersion;
        }

        private void tag(String tag) {
            this.tag = tag;
        }

        private void storageOptions(Map<String, Object> storageOptions) {
            this.storageOptions = storageOptions;
        }

        private void overrides(Map<String, Object> overrides) {
            this.overrides = overrides;
        }

        private void multiFields(Map<String, Object> multiFields) {
            this.multiFields = multiFields;
        }

        public LanceAttachRequest build() {
            if (table == null || table.isEmpty()) {
                throw new IllegalArgumentException("[table] is required");
            }
            if (pinnedVersion != null && pinnedVersion < 0) {
                throw new IllegalArgumentException("[version] must be a non-negative integer");
            }
            if (tag != null && tag.isEmpty()) {
                throw new IllegalArgumentException("[tag] must not be empty");
            }
            if (pinnedVersion != null && tag != null) {
                // `version` is a fixed pin, `tag` follows wherever the tag
                // points. The engine can honour only one of them per index.
                throw new IllegalArgumentException("[version] and [tag] are mutually exclusive");
            }
            StorageOptions parsedStorageOptions = StorageOptions.parseFromRequestField(storageOptions, "[" + PARSER_NAME + "]");
            // `overrides` is the forward-looking clause; the legacy
            // `multi_fields` clause folds into `overrides.[col].fields`
            // at parse time so everything downstream sees one shape. Both
            // are handed over together so a column declared through both
            // is refused as ambiguous.
            LanceOverrides parsedOverrides = LanceOverrides.parseAttachClauses(overrides, multiFields);
            return new LanceAttachRequest(table, indexName, pinnedVersion, tag, parsedStorageOptions, parsedOverrides);
        }
    }

    private final String table;
    private final String indexName;
    private final Long pinnedVersion;
    private final String tag;
    private final StorageOptions storageOptions;
    private final LanceOverrides overrides;

    /**
     * @param table          Lance table URI to attach.
     * @param indexName      explicit index name, or {@code null} to derive it
     *                       from the table directory name.
     * @param pinnedVersion  manifest version to pin, or {@code null} to
     *                       follow the latest version.
     * @param tag            Lance tag to follow, or {@code null}. Not
     *                       accepted together with {@code pinnedVersion}.
     * @param storageOptions object-store options for the table.
     * @param overrides      per-column mapping overrides, already merged
     *                       from the {@code overrides} and legacy
     *                       {@code multi_fields} clauses.
     */
    public LanceAttachRequest(
        String table,
        String indexName,
        Long pinnedVersion,
        String tag,
        StorageOptions storageOptions,
        LanceOverrides overrides
    ) {
        this.table = table;
        this.indexName = indexName;
        this.pinnedVersion = pinnedVersion;
        this.tag = tag;
        this.storageOptions = storageOptions == null ? StorageOptions.empty() : storageOptions;
        this.overrides = overrides == null ? LanceOverrides.EMPTY : overrides;
    }

    public LanceAttachRequest(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceAttachRequest", WIRE_VERSION);
        this.table = in.readString();
        this.indexName = in.readOptionalString();
        this.pinnedVersion = in.readOptionalLong();
        this.tag = in.readOptionalString();
        this.storageOptions = StorageOptions.readFromStream(in);
        // The overrides travel as their canonical JSON: one string
        // instead of a hand-rolled nested map encoding, and the same
        // bytes that end up in the index setting. Declaration order
        // survives because the JSON object preserves it.
        this.overrides = LanceOverrides.parse(in.readString());
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        out.writeString(table);
        out.writeOptionalString(indexName);
        out.writeOptionalLong(pinnedVersion);
        out.writeOptionalString(tag);
        storageOptions.writeTo(out);
        out.writeString(overrides.toJson());
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException ex = null;
        if (table == null || table.isEmpty()) {
            ex = new ActionRequestValidationException();
            ex.addValidationError("[table] is required");
        }
        if (pinnedVersion != null && pinnedVersion < 0) {
            if (ex == null) {
                ex = new ActionRequestValidationException();
            }
            ex.addValidationError("[version] must be a non-negative integer");
        }
        if (tag != null && tag.isEmpty()) {
            if (ex == null) {
                ex = new ActionRequestValidationException();
            }
            ex.addValidationError("[tag] must not be empty");
        }
        if (pinnedVersion != null && tag != null) {
            // A version is an immutable pin and a tag is a moving one; the
            // engine cannot honour both, so refuse instead of picking.
            if (ex == null) {
                ex = new ActionRequestValidationException();
            }
            ex.addValidationError("[version] and [tag] are mutually exclusive");
        }
        return ex;
    }

    public String table() {
        return table;
    }

    /** Explicit index name, or {@code null} when it should derive from the table name. */
    public String indexName() {
        return indexName;
    }

    public Optional<Long> pinnedVersion() {
        return Optional.ofNullable(pinnedVersion);
    }

    /** Lance tag the index should follow, or empty when it does not follow a tag. */
    public Optional<String> tag() {
        return Optional.ofNullable(tag);
    }

    public StorageOptions storageOptions() {
        return storageOptions;
    }

    public LanceOverrides overrides() {
        return overrides;
    }
}

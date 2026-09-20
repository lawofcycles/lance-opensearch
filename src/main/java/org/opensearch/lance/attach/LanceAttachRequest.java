/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.lance.StorageOptions;

/**
 * Request for {@link LanceAttachAction}: the parsed body of
 * {@code POST /_lance/attach}.
 */
public final class LanceAttachRequest extends ActionRequest {

    private final String table;
    private final String indexName;
    private final Long pinnedVersion;
    private final StorageOptions storageOptions;
    private final Map<String, LinkedHashMap<String, String>> multiFields;

    /**
     * @param table          Lance table URI to attach.
     * @param indexName      explicit index name, or {@code null} to derive it
     *                       from the table directory name.
     * @param pinnedVersion  manifest version to pin, or {@code null} to
     *                       follow the latest version.
     * @param storageOptions object-store options for the table.
     * @param multiFields    base column to (sub-field name to sub-field
     *                       type), already merged from the {@code multi_fields}
     *                       and {@code overrides} clauses.
     */
    public LanceAttachRequest(
        String table,
        String indexName,
        Long pinnedVersion,
        StorageOptions storageOptions,
        Map<String, LinkedHashMap<String, String>> multiFields
    ) {
        this.table = table;
        this.indexName = indexName;
        this.pinnedVersion = pinnedVersion;
        this.storageOptions = storageOptions == null ? StorageOptions.empty() : storageOptions;
        this.multiFields = multiFields == null ? Collections.emptyMap() : copyMultiFields(multiFields);
    }

    public LanceAttachRequest(StreamInput in) throws IOException {
        super(in);
        this.table = in.readString();
        this.indexName = in.readOptionalString();
        this.pinnedVersion = in.readOptionalLong();
        this.storageOptions = StorageOptions.readFromStream(in);
        int columns = in.readVInt();
        LinkedHashMap<String, LinkedHashMap<String, String>> read = new LinkedHashMap<>();
        for (int i = 0; i < columns; i++) {
            String column = in.readString();
            int subFields = in.readVInt();
            LinkedHashMap<String, String> subs = new LinkedHashMap<>();
            for (int j = 0; j < subFields; j++) {
                subs.put(in.readString(), in.readString());
            }
            read.put(column, subs);
        }
        this.multiFields = Collections.unmodifiableMap(read);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(table);
        out.writeOptionalString(indexName);
        out.writeOptionalLong(pinnedVersion);
        storageOptions.writeTo(out);
        // Written by hand rather than through writeMap so the sub-field
        // order the operator declared survives the wire; the mapping
        // emits fields in that order.
        out.writeVInt(multiFields.size());
        for (Map.Entry<String, LinkedHashMap<String, String>> entry : multiFields.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (Map.Entry<String, String> sub : entry.getValue().entrySet()) {
                out.writeString(sub.getKey());
                out.writeString(sub.getValue());
            }
        }
    }

    private static Map<String, LinkedHashMap<String, String>> copyMultiFields(Map<String, LinkedHashMap<String, String>> source) {
        LinkedHashMap<String, LinkedHashMap<String, String>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, LinkedHashMap<String, String>> entry : source.entrySet()) {
            copy.put(entry.getKey(), new LinkedHashMap<>(entry.getValue()));
        }
        return Collections.unmodifiableMap(copy);
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

    public StorageOptions storageOptions() {
        return storageOptions;
    }

    public Map<String, LinkedHashMap<String, String>> multiFields() {
        return multiFields;
    }
}

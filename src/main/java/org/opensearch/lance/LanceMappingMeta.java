/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * Readers of the Lance identity metadata the mapping deriver stores on
 * every field: {@code meta.lance_field_id} (the Lance immutable field
 * id), {@code meta.lance_arrow_type} (the Arrow type fingerprint), and
 * {@code meta.lance_dropped} (the field's column no longer exists in
 * the Lance table under this name). The namespace poll writes the
 * dropped marker when a rename, reset or drop leaves a stale name in
 * the mapping, because PutMapping cannot remove properties.
 */
public final class LanceMappingMeta {

    private LanceMappingMeta() {}

    /**
     * One rename recorded in the mapping: a field marked
     * {@code lance_dropped} whose field id lives on under another name
     * with the same Arrow type. {@code from} is the stale name,
     * {@code to} the name the Lance table uses now.
     */
    public record RenamedField(String from, String to, int fieldId) implements Writeable {

        public RenamedField(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(from);
            out.writeString(to);
            out.writeVInt(fieldId);
        }
    }

    /**
     * The renames the mapping records, in mapping order of the stale
     * name: every top-level field carrying {@code lance_dropped} whose
     * {@code lance_field_id} also belongs to a live field with the same
     * {@code lance_arrow_type}. A dropped field whose id is gone from
     * the mapping (the column was dropped, not renamed) and a dropped
     * field whose id lives on under a different Arrow type (a schema
     * reset, not a rename) contribute nothing.
     *
     * @param mapping the index's mapping metadata, nullable
     */
    public static List<RenamedField> renamedFields(MappingMetadata mapping) {
        if (mapping == null) {
            return List.of();
        }
        Map<String, Object> source = mapping.sourceAsMap();
        Object properties = source == null ? null : source.get("properties");
        if (!(properties instanceof Map<?, ?> propsMap)) {
            return List.of();
        }
        // id -> live field's (name, arrow type)
        Map<Integer, String[]> liveById = new LinkedHashMap<>();
        // stale name -> (id, arrow type), in mapping order
        Map<String, int[]> droppedIds = new LinkedHashMap<>();
        Map<String, String> droppedArrowTypes = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : propsMap.entrySet()) {
            if (!(entry.getKey() instanceof String name) || !(entry.getValue() instanceof Map<?, ?> field)) {
                continue;
            }
            Object meta = field.get("meta");
            if (!(meta instanceof Map<?, ?> metaMap)) {
                continue;
            }
            Integer fieldId = parseFieldId(metaMap.get("lance_field_id"));
            if (fieldId == null) {
                continue;
            }
            String arrowType = metaMap.get("lance_arrow_type") instanceof String s ? s : null;
            if ("true".equals(metaMap.get("lance_dropped"))) {
                droppedIds.put(name, new int[] { fieldId });
                droppedArrowTypes.put(name, arrowType);
            } else {
                liveById.put(fieldId, new String[] { name, arrowType });
            }
        }
        List<RenamedField> renamed = new ArrayList<>();
        for (Map.Entry<String, int[]> dropped : droppedIds.entrySet()) {
            int fieldId = dropped.getValue()[0];
            String[] live = liveById.get(fieldId);
            if (live == null) {
                continue;
            }
            String droppedArrowType = droppedArrowTypes.get(dropped.getKey());
            if (droppedArrowType == null || !droppedArrowType.equals(live[1])) {
                continue;
            }
            renamed.add(new RenamedField(dropped.getKey(), live[0], fieldId));
        }
        return renamed;
    }

    /** Whether a top-level mapping entry (name -> options map) carries the dropped marker. */
    public static boolean isDropped(Map<String, Object> fieldEntry) {
        Object meta = fieldEntry.get("meta");
        return meta instanceof Map<?, ?> metaMap && "true".equals(metaMap.get("lance_dropped"));
    }

    private static Integer parseFieldId(Object raw) {
        if (!(raw instanceof String s)) {
            return null;
        }
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

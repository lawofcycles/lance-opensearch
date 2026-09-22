/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.List;
import java.util.Map;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Reading the Lance identity meta off a mapping: which
 * {@code lance_dropped} fields count as renames (field id lives on
 * under a new name with the same Arrow type) and which do not (id gone,
 * or id reused with a different Arrow type).
 */
public class LanceMappingMetaTests extends OpenSearchTestCase {

    private static MappingMetadata mapping(Map<String, Object> properties) {
        return new MappingMetadata("_doc", Map.of("properties", properties));
    }

    private static Map<String, Object> field(String type, String fieldId, String arrowType, boolean dropped) {
        Map<String, Object> meta = dropped
            ? Map.of("lance_field_id", fieldId, "lance_arrow_type", arrowType, "lance_dropped", "true")
            : Map.of("lance_field_id", fieldId, "lance_arrow_type", arrowType);
        return Map.of("type", type, "meta", meta);
    }

    public void testRenamedFieldPairsDroppedWithLiveOfSameIdAndType() {
        MappingMetadata metadata = mapping(
            Map.of(
                "ts",
                field("date", "1", "Int(64, true)", true),
                "event_ts",
                field("date", "1", "Int(64, true)", false),
                "id",
                field("integer", "0", "Int(32, true)", false)
            )
        );
        List<LanceMappingMeta.RenamedField> renamed = LanceMappingMeta.renamedFields(metadata);
        assertEquals(1, renamed.size());
        assertEquals(new LanceMappingMeta.RenamedField("ts", "event_ts", 1), renamed.get(0));
    }

    public void testDroppedFieldWithoutLiveIdIsNotARename() {
        MappingMetadata metadata = mapping(Map.of("gone", field("keyword", "5", "Utf8", true)));
        assertTrue(LanceMappingMeta.renamedFields(metadata).isEmpty());
    }

    public void testResetIdWithDifferentArrowTypeIsNotARename() {
        MappingMetadata metadata = mapping(
            Map.of("old", field("long", "2", "Int(64, true)", true), "recast", field("date", "2", "Timestamp(MILLISECOND, null)", false))
        );
        assertTrue(LanceMappingMeta.renamedFields(metadata).isEmpty());
    }

    public void testNullMappingAndFieldsWithoutMetaContributeNothing() {
        assertTrue(LanceMappingMeta.renamedFields(null).isEmpty());
        MappingMetadata metadata = mapping(Map.of("plain", Map.of("type", "keyword")));
        assertTrue(LanceMappingMeta.renamedFields(metadata).isEmpty());
    }

    public void testIsDroppedReadsTheMarker() {
        assertTrue(LanceMappingMeta.isDropped(field("keyword", "3", "Utf8", true)));
        assertFalse(LanceMappingMeta.isDropped(field("keyword", "3", "Utf8", false)));
        assertFalse(LanceMappingMeta.isDropped(Map.of("type", "keyword")));
    }
}

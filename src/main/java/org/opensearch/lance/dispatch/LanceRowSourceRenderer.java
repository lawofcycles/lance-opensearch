/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.Base64;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Convert a single row of an Arrow {@link VectorSchemaRoot} into the
 * JSON bytes that OpenSearch's {@code _source} field carries.
 *
 * <p>The dispatch-mode filter needs this so a fragment-mode
 * {@code _search} response can populate the {@code _source} of every
 * {@link org.opensearch.search.SearchHit} it emits without going
 * through OpenSearch's shard-scoped stored-fields machinery.
 * {@code LanceFragmentLeafReader.materialiseStoredFields} does the
 * same work in shard mode but reads out of the per-fragment on-heap
 * arrays it eagerly loads at construction; this class does it row by
 * row against a live Arrow batch instead, so the shard-free
 * executor can scan the dataset directly and skip the eager-load
 * detour.
 *
 * <p>Type coverage matches the existing shard-mode behaviour:
 * signed {@code Int} (int8 / int16 / int32 / int64), {@code Bool},
 * {@code Utf8}, {@code Date}, {@code Timestamp}, {@code List<Utf8>},
 * {@code Binary} / {@code LargeBinary}. Everything else (unsigned
 * ints, {@code Float} / {@code Double}, {@code FixedSizeList},
 * {@code Struct}, {@code Decimal}, {@code LargeUtf8}) is skipped
 * silently, mirroring {@code RestAttachAction.derive}'s decision to
 * keep those columns out of the mapping and {@code _source}.
 *
 * <p>Metadata columns ({@code _rowaddr}, {@code _id}, {@code _source})
 * are dropped so a scan run with {@code withRowAddress=true} does not
 * leak the synthetic address field into the user-facing document.
 */
public final class LanceRowSourceRenderer {

    private LanceRowSourceRenderer() {}

    /**
     * Render the {@code rowIndex}-th row of {@code root} as JSON
     * bytes suitable for {@code SearchHit.sourceRef}. Never throws
     * on missing values or unsupported types; unsupported types are
     * silently skipped so a table with a mixed schema still produces
     * a valid, if partial, source document.
     */
    public static byte[] renderJson(VectorSchemaRoot root, int rowIndex) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (FieldVector vector : root.getFieldVectors()) {
                Field field = vector.getField();
                String name = field.getName();
                if (isMetadataColumn(name)) {
                    continue;
                }
                if (vector.isNull(rowIndex)) {
                    continue;
                }
                appendField(builder, vector, rowIndex);
            }
            builder.endObject();
            return BytesReference.toBytes(BytesReference.bytes(builder));
        }
    }

    private static boolean isMetadataColumn(String name) {
        return "_rowaddr".equals(name) || "_id".equals(name) || "_source".equals(name);
    }

    private static void appendField(XContentBuilder builder, FieldVector vector, int rowIndex) throws IOException {
        Field field = vector.getField();
        String name = field.getName();
        ArrowType type = field.getType();

        if (type instanceof ArrowType.Int intType) {
            if (!intType.getIsSigned()) {
                // Unsigned ints are dropped in the shard-mode reader
                // because Java's long cannot represent 64-bit
                // unsigned values without loss; keep the two paths
                // aligned by dropping them here too.
                return;
            }
            builder.field(name, readAsLong(vector, rowIndex));
            return;
        }
        if (type instanceof ArrowType.Bool) {
            builder.field(name, ((BitVector) vector).get(rowIndex) != 0);
            return;
        }
        if (type instanceof ArrowType.Utf8) {
            byte[] bytes = ((VarCharVector) vector).get(rowIndex);
            builder.field(name, new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
            return;
        }
        if (type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp) {
            builder.field(name, readAsLong(vector, rowIndex));
            return;
        }
        if (type instanceof ArrowType.List) {
            appendListField(builder, name, field, (ListVector) vector, rowIndex);
            return;
        }
        if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
            byte[] bytes;
            if (vector instanceof VarBinaryVector vb) {
                bytes = vb.get(rowIndex);
            } else if (vector instanceof LargeVarBinaryVector lb) {
                bytes = lb.get(rowIndex);
            } else {
                return;
            }
            builder.field(name, Base64.getEncoder().encodeToString(bytes));
            return;
        }
        // Every other type (Float, Double, FixedSizeList, Struct,
        // Decimal, LargeUtf8, etc.) is left out. RestAttachAction's
        // derive() keeps them out of the mapping so callers that
        // want them today have to consult the Lance table directly.
    }

    private static void appendListField(XContentBuilder builder, String name, Field field, ListVector list, int rowIndex)
        throws IOException {
        if (field.getChildren().isEmpty()) {
            return;
        }
        ArrowType childType = field.getChildren().get(0).getType();
        if (!(childType instanceof ArrowType.Utf8)) {
            // Only List<Utf8> is on the supported mapping surface
            // today (surfaced as multi-valued keyword). Any other
            // element type (List<Int>, List<Struct>, etc.) is dropped
            // to match shard mode.
            return;
        }
        VarCharVector elements = (VarCharVector) list.getDataVector();
        int start = list.getElementStartIndex(rowIndex);
        int end = list.getElementEndIndex(rowIndex);
        builder.startArray(name);
        for (int e = start; e < end; e++) {
            if (elements.isNull(e)) {
                builder.nullValue();
            } else {
                byte[] bytes = elements.get(e);
                builder.value(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        builder.endArray();
    }

    /**
     * Widen the {@code rowIndex}-th value of {@code vector} into a
     * long. Handles the surfaced numeric and temporal Arrow types the
     * same way {@code LanceFragmentLeafReader.readAsLong} does. Kept
     * separate rather than reused so the dispatch package stays
     * decoupled from the engine's fragment reader.
     */
    private static long readAsLong(FieldVector vector, int rowIndex) {
        if (vector instanceof TinyIntVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof SmallIntVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof IntVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof BigIntVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof DateDayVector v) {
            return v.get(rowIndex) * 86_400_000L;
        }
        if (vector instanceof DateMilliVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof TimeStampSecVector v) {
            return v.get(rowIndex) * 1000L;
        }
        if (vector instanceof TimeStampSecTZVector v) {
            return v.get(rowIndex) * 1000L;
        }
        if (vector instanceof TimeStampMilliVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof TimeStampMilliTZVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof TimeStampMicroVector v) {
            return v.get(rowIndex) / 1000L;
        }
        if (vector instanceof TimeStampMicroTZVector v) {
            return v.get(rowIndex) / 1000L;
        }
        if (vector instanceof TimeStampNanoVector v) {
            return v.get(rowIndex) / 1_000_000L;
        }
        if (vector instanceof TimeStampNanoTZVector v) {
            return v.get(rowIndex) / 1_000_000L;
        }
        throw new IllegalStateException("unsupported vector type: " + vector.getClass().getName());
    }
}

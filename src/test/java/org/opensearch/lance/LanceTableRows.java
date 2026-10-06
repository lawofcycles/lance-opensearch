/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.lance.Dataset;
import org.lance.OpenDatasetBuilder;
import org.lance.ReadOptions;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

/**
 * Reads the rows of a Lance table the test fixtures wrote back as JSON
 * values, for the oracle index {@link LanceRestTestCase#oracleIndexFor}
 * loads next to a Lance index. Lives in the test source set rather than
 * next to the writers of {@link LanceTableFactory} because a scan needs
 * {@code arrow-dataset} on the compile classpath, which the fixtures
 * source set does not carry.
 */
final class LanceTableRows {

    /** The field metadata key Lance uses to declare a primary key column. */
    static final String PRIMARY_KEY_METADATA = "lance-schema:unenforced-primary-key";

    /**
     * One row as {@link #readRows} returns it: the {@code _id} the plugin
     * renders for the row (the declared primary key's value, or
     * {@code "<fragment>-<offset>"} when the table declares none) and the
     * row's columns as JSON values in schema order.
     */
    record TableRow(String id, Map<String, Object> values) {
    }

    private LanceTableRows() {}

    /** The manifest version the tag {@code tag} of the table at {@code tableUri} points at. */
    static long tagVersion(String tableUri, String tag) throws Exception {
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open().allocator(allocator).uri(tableUri).build()
        ) {
            return dataset.tags().getVersion(tag);
        }
    }

    /**
     * Every live row of the table at {@code tableUri}, at its latest
     * version when {@code version} is null and at that manifest version
     * otherwise, as JSON values: integers as {@link Long} (an unsigned 64
     * bit integer as {@link BigInteger}), {@code Float32} as {@link Float},
     * {@code Float64} as {@link Double}, booleans as {@link Boolean},
     * strings as {@link String}, dates and timestamps of every unit as
     * epoch millis, binary as a Base64 string, a list as a {@link List}
     * of its converted elements (a fixed size list included), and a
     * struct as a {@link Map} of its children in declaration order whose
     * null children are explicit {@code null} entries. A top level null
     * cell leaves its key out of the map. The rows come in scan order:
     * fragment by fragment, each in offset order.
     */
    static List<TableRow> readRows(String tableUri, Long version) throws Exception {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            OpenDatasetBuilder open = Dataset.open().allocator(allocator).uri(tableUri);
            if (version != null) {
                open = open.readOptions(new ReadOptions.Builder().setVersion(version).build());
            }
            try (Dataset dataset = open.build()) {
                String primaryKey = null;
                for (Field field : dataset.getSchema().getFields()) {
                    if (field.getMetadata() != null && field.getMetadata().containsKey(PRIMARY_KEY_METADATA)) {
                        primaryKey = field.getName();
                    }
                }
                List<TableRow> rows = new ArrayList<>();
                ScanOptions options = new ScanOptions.Builder().withRowAddress(true).build();
                try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                        for (int i = 0; i < root.getRowCount(); i++) {
                            Map<String, Object> values = new LinkedHashMap<>();
                            for (FieldVector vector : root.getFieldVectors()) {
                                if (vector.getName().equals("_rowaddr")) {
                                    continue;
                                }
                                Object value = jsonValue(vector, i);
                                if (value != null) {
                                    values.put(vector.getName(), value);
                                }
                            }
                            long address = rowAddr.get(i);
                            String fallbackId = (address >>> 32) + "-" + (address & 0xFFFFFFFFL);
                            Object key = primaryKey == null ? null : values.get(primaryKey);
                            rows.add(new TableRow(key == null ? fallbackId : String.valueOf(key), values));
                        }
                    }
                }
                return rows;
            }
        }
    }

    /** The JSON value of one Arrow cell; null for an Arrow null or an unsupported type. */
    private static Object jsonValue(FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return null;
        }
        if (vector instanceof BitVector bits) {
            return bits.get(index) == 1;
        }
        if (vector instanceof UInt8Vector unsigned) {
            return new BigInteger(Long.toUnsignedString(unsigned.get(index)));
        }
        if (vector instanceof BaseIntVector ints) {
            return ints.getValueAsLong(index);
        }
        if (vector instanceof Float4Vector floats) {
            return floats.get(index);
        }
        if (vector instanceof Float8Vector doubles) {
            return doubles.get(index);
        }
        if (vector instanceof VarCharVector strings) {
            return new String(strings.get(index), StandardCharsets.UTF_8);
        }
        if (vector instanceof LargeVarCharVector strings) {
            return new String(strings.get(index), StandardCharsets.UTF_8);
        }
        if (vector instanceof VarBinaryVector bytes) {
            return Base64.getEncoder().encodeToString(bytes.get(index));
        }
        if (vector instanceof LargeVarBinaryVector bytes) {
            return Base64.getEncoder().encodeToString(bytes.get(index));
        }
        if (vector instanceof DateDayVector days) {
            return days.get(index) * 86_400_000L;
        }
        if (vector instanceof DateMilliVector millis) {
            return millis.get(index);
        }
        if (vector instanceof TimeStampVector timestamps) {
            long raw = timestamps.get(index);
            return switch (((ArrowType.Timestamp) vector.getField().getType()).getUnit()) {
                case SECOND -> raw * 1000L;
                case MILLISECOND -> raw;
                case MICROSECOND -> raw / 1000L;
                case NANOSECOND -> raw / 1_000_000L;
            };
        }
        if (vector instanceof StructVector struct) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (FieldVector child : struct.getChildrenFromFields()) {
                out.put(child.getName(), jsonValue(child, index));
            }
            return out;
        }
        if (vector instanceof ListVector list) {
            return listValues(list.getDataVector(), list.getElementStartIndex(index), list.getElementEndIndex(index));
        }
        if (vector instanceof FixedSizeListVector list) {
            return listValues(list.getDataVector(), list.getElementStartIndex(index), list.getElementEndIndex(index));
        }
        return null;
    }

    private static List<Object> listValues(FieldVector elements, int start, int end) {
        List<Object> out = new ArrayList<>(end - start);
        for (int e = start; e < end; e++) {
            out.add(jsonValue(elements, e));
        }
        return out;
    }
}

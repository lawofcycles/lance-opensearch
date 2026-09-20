/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query.substrait;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Cast;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Expression;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.FieldReference;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.ScalarType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Feeds the encoded plans to a real Lance dataset through
 * {@code ScanOptions.Builder#substraitAggregate} and checks the rows Lance
 * returns. No protobuf decoder is involved: Lance accepting the bytes and
 * producing the expected groups is the check. The hint fixture has 600
 * rows in 3 fragments with {@code rating = (i * 37) % 1000} (null when
 * {@code i % 5 == 4}), {@code category = "c" + (i % 3)} (null when
 * {@code i % 4 == 3}) and {@code flag = i % 2 == 0} (null when
 * {@code i % 7 == 6}).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SubstraitAggregatePlanTests extends OpenSearchTestCase {

    public void testMetricsOnlyOverEveryRow() throws Exception {
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(dir, "metrics", 3, 200);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Expression rating = new FieldReference(fieldIndex(dataset, "rating"));
            Expression flag = new FieldReference(fieldIndex(dataset, "flag"));
            ByteBuffer plan = new SubstraitAggregatePlan.Builder().measure("count", List.of(), ScalarType.I64, "n")
                .measure("count", List.of(rating), ScalarType.I64, "n_rating")
                .measure("sum", List.of(rating), ScalarType.I64, "s")
                .measure("min", List.of(rating), ScalarType.I64, "lo")
                .measure("max", List.of(rating), ScalarType.I64, "hi")
                .measure("sum", List.of(new Cast(flag, ScalarType.I64)), ScalarType.I64, "flags")
                .build();
            List<Map<String, Object>> rows = scan(dataset, plan, null, null);
            assertEquals(1, rows.size());
            Map<String, Object> row = rows.get(0);
            long expectedSum = 0;
            long expectedCount = 0;
            long expectedFlags = 0;
            long lo = Long.MAX_VALUE;
            long hi = Long.MIN_VALUE;
            for (int i = 0; i < 600; i++) {
                if (i % 5 != 4) {
                    long r = (i * 37L) % 1000L;
                    expectedSum += r;
                    expectedCount++;
                    lo = Math.min(lo, r);
                    hi = Math.max(hi, r);
                }
                if (i % 7 != 6 && i % 2 == 0) {
                    expectedFlags++;
                }
            }
            assertEquals(600L, row.get("n"));
            assertEquals(expectedCount, row.get("n_rating"));
            assertEquals(expectedSum, row.get("s"));
            // min / max keep the column's own width (int32 here).
            assertEquals(lo, ((Number) row.get("lo")).longValue());
            assertEquals(hi, ((Number) row.get("hi")).longValue());
            assertEquals(expectedFlags, row.get("flags"));
        }
    }

    public void testGroupByKeywordWithCountAndSubMetric() throws Exception {
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(dir, "group-category", 3, 200);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Expression category = new FieldReference(fieldIndex(dataset, "category"));
            Expression rating = new FieldReference(fieldIndex(dataset, "rating"));
            ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(category, "k")
                .measure("count", List.of(), ScalarType.I64, "n")
                .measure("sum", List.of(rating), ScalarType.I64, "s")
                .measure("count", List.of(rating), ScalarType.I64, "c")
                .build();
            List<Map<String, Object>> rows = scan(dataset, plan, null, null);
            // The null category forms its own group; the caller drops it.
            assertEquals(4, rows.size());
            Map<String, Long> counts = new TreeMap<>();
            Map<String, Long> sums = new TreeMap<>();
            for (Map<String, Object> row : rows) {
                String key = row.get("k") == null ? "<null>" : (String) row.get("k");
                counts.put(key, (Long) row.get("n"));
                sums.put(key, (Long) row.get("s"));
            }
            Map<String, Long> expectedSums = new TreeMap<>();
            for (int i = 0; i < 600; i++) {
                String key = i % 4 == 3 ? "<null>" : "c" + (i % 3);
                if (i % 5 != 4) {
                    expectedSums.merge(key, (i * 37L) % 1000L, Long::sum);
                }
            }
            assertEquals(Map.of("<null>", 150L, "c0", 150L, "c1", 150L, "c2", 150L), counts);
            assertEquals(expectedSums, sums);
        }
    }

    public void testGroupByBooleanAndInteger() throws Exception {
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(dir, "group-flag", 3, 200);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Expression flag = new FieldReference(fieldIndex(dataset, "flag"));
            ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(flag, "k")
                .measure("count", List.of(), ScalarType.I64, "n")
                .build();
            List<Map<String, Object>> rows = scan(dataset, plan, null, null);
            Map<String, Long> counts = new TreeMap<>();
            for (Map<String, Object> row : rows) {
                counts.put(String.valueOf(row.get("k")), (Long) row.get("n"));
            }
            long trues = 0;
            long falses = 0;
            long nulls = 0;
            for (int i = 0; i < 600; i++) {
                if (i % 7 == 6) {
                    nulls++;
                } else if (i % 2 == 0) {
                    trues++;
                } else {
                    falses++;
                }
            }
            assertEquals(Map.of("true", trues, "false", falses, "null", nulls), counts);

            Expression rating = new FieldReference(fieldIndex(dataset, "rating"));
            ByteBuffer byRating = new SubstraitAggregatePlan.Builder().groupBy(rating, "k")
                .measure("count", List.of(), ScalarType.I64, "n")
                .build();
            List<Map<String, Object>> ratingRows = scan(dataset, byRating, null, null);
            // 480 non-null ratings in 0..999 with i * 37 % 1000: i and
            // i + 1000 never both appear below 600, so every rating is
            // distinct and the key keeps the int32 type.
            assertEquals(481, ratingRows.size());
            long total = 0;
            for (Map<String, Object> row : ratingRows) {
                total += (Long) row.get("n");
                if (row.get("k") != null) {
                    assertEquals(1L, row.get("n"));
                    assertTrue(row.get("k") instanceof Integer);
                }
            }
            assertEquals(600L, total);
        }
    }

    public void testFilterAndFragmentSubsetApplyBeforeTheAggregate() throws Exception {
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(dir, "filtered", 3, 200);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Expression category = new FieldReference(fieldIndex(dataset, "category"));
            ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(category, "k")
                .measure("count", List.of(), ScalarType.I64, "n")
                .build();
            // Fragment 1 holds i in 200..399; rating >= 500 keeps the
            // rows whose (i * 37) % 1000 >= 500 and rating is not null.
            List<Map<String, Object>> rows = scan(dataset, plan, "rating >= 500", List.of(1));
            Map<String, Long> counts = new TreeMap<>();
            for (Map<String, Object> row : rows) {
                counts.put(row.get("k") == null ? "<null>" : (String) row.get("k"), (Long) row.get("n"));
            }
            Map<String, Long> expected = new TreeMap<>();
            for (int i = 200; i < 400; i++) {
                if (i % 5 != 4 && (i * 37L) % 1000L >= 500L) {
                    expected.merge(i % 4 == 3 ? "<null>" : "c" + (i % 3), 1L, Long::sum);
                }
            }
            assertEquals(expected, counts);
        }
    }

    public void testHistogramKeyFloorsNegativeQuotients() throws Exception {
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeSignedValuesTable(dir, "signed");
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Expression v = new FieldReference(fieldIndex(dataset, "v"));
            Expression f = new FieldReference(fieldIndex(dataset, "f"));
            ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(SubstraitExpressions.floorFp64(v, 0d, 100d), "k")
                .measure("count", List.of(), ScalarType.I64, "n")
                .build();
            Map<Long, Long> expected = new TreeMap<>();
            for (long value : LanceTableFactory.SIGNED_VALUES) {
                expected.merge((long) Math.floor(((double) value - 0d) / 100d), 1L, Long::sum);
            }
            assertEquals(expected, keyCounts(scan(dataset, plan, null, null)));

            // Same on the float column and with an offset, against the
            // histogram aggregator's own double arithmetic.
            ByteBuffer floatPlan = new SubstraitAggregatePlan.Builder().groupBy(SubstraitExpressions.floorFp64(f, 25d, 100d), "k")
                .measure("count", List.of(), ScalarType.I64, "n")
                .build();
            Map<Long, Long> expectedFloat = new TreeMap<>();
            for (long value : LanceTableFactory.SIGNED_VALUES) {
                expectedFloat.merge((long) Math.floor(((value + 0.5d) - 25d) / 100d), 1L, Long::sum);
            }
            assertEquals(expectedFloat, keyCounts(scan(dataset, floatPlan, null, null)));
        }
    }

    public void testDateHistogramKeyFloorsMillisecondTimestamps() throws Exception {
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeSignedValuesTable(dir, "signed-ts");
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            ArrowType tsType = fieldType(dataset, "ts");
            Expression ts = new FieldReference(fieldIndex(dataset, "ts"));
            long interval = 30L * 86_400_000L;
            ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(
                SubstraitExpressions.floorDivInt64(SubstraitExpressions.epochMillis(ts, tsType), 0L, interval),
                "k"
            ).measure("count", List.of(), ScalarType.I64, "n").build();
            Map<Long, Long> expected = new TreeMap<>();
            for (long value : LanceTableFactory.SIGNED_VALUES) {
                expected.merge(Math.floorDiv(value * 86_400_000L, interval), 1L, Long::sum);
            }
            assertEquals(expected, keyCounts(scan(dataset, plan, null, null)));
        }
    }

    public void testDateHistogramKeyOnMicrosecondTimestamps() throws Exception {
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeDatedTable(dir, "dated");
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            ArrowType tsType = fieldType(dataset, "ts");
            assertTrue(tsType instanceof ArrowType.Timestamp);
            Expression ts = new FieldReference(fieldIndex(dataset, "ts"));
            long interval = 30L * 86_400_000L;
            ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(
                SubstraitExpressions.floorDivInt64(SubstraitExpressions.epochMillis(ts, tsType), 0L, interval),
                "k"
            )
                .measure("count", List.of(), ScalarType.I64, "n")
                .measure("min", List.of(SubstraitExpressions.epochMillis(ts, tsType)), ScalarType.I64, "lo")
                .build();
            String[] days = { "2024-01-15", "2024-02-20", "2024-03-10", "2024-03-25", "2024-04-05", "2024-05-30" };
            Map<Long, Long> expected = new TreeMap<>();
            Map<Long, Long> expectedMin = new TreeMap<>();
            for (String day : days) {
                long millis = Instant.parse(day + "T00:00:00Z").toEpochMilli();
                long key = Math.floorDiv(millis, interval);
                expected.merge(key, 1L, Long::sum);
                expectedMin.merge(key, millis, Math::min);
            }
            List<Map<String, Object>> rows = scan(dataset, plan, null, null);
            assertEquals(expected, keyCounts(rows));
            Map<Long, Long> mins = new TreeMap<>();
            for (Map<String, Object> row : rows) {
                mins.put((Long) row.get("k"), (Long) row.get("lo"));
            }
            assertEquals(expectedMin, mins);
        }
    }

    private static Map<Long, Long> keyCounts(List<Map<String, Object>> rows) {
        Map<Long, Long> counts = new TreeMap<>();
        for (Map<String, Object> row : rows) {
            counts.put((Long) row.get("k"), (Long) row.get("n"));
        }
        return counts;
    }

    static int fieldIndex(Dataset dataset, String column) {
        List<Field> fields = dataset.getSchema().getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(column)) {
                return i;
            }
        }
        throw new IllegalArgumentException("no column " + column);
    }

    static ArrowType fieldType(Dataset dataset, String column) {
        return dataset.getSchema().getFields().get(fieldIndex(dataset, column)).getType();
    }

    static List<Map<String, Object>> scan(Dataset dataset, ByteBuffer plan, String filter, List<Integer> fragmentIds) throws Exception {
        ScanOptions.Builder options = new ScanOptions.Builder().substraitAggregate(plan);
        if (filter != null) {
            options.filter(filter);
        }
        if (fragmentIds != null) {
            options.fragmentIds(fragmentIds);
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        try (LanceScanner scanner = dataset.newScan(options.build()); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                for (int i = 0; i < root.getRowCount(); i++) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (FieldVector vector : root.getFieldVectors()) {
                        row.put(vector.getName(), value(vector, i));
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private static Object value(FieldVector vector, int i) {
        if (vector.isNull(i)) {
            return null;
        }
        if (vector instanceof BigIntVector v) {
            return v.get(i);
        }
        if (vector instanceof IntVector v) {
            return v.get(i);
        }
        if (vector instanceof Float8Vector v) {
            return v.get(i);
        }
        if (vector instanceof BitVector v) {
            return v.get(i) == 1;
        }
        if (vector instanceof VarCharVector v) {
            return new String(v.get(i), StandardCharsets.UTF_8);
        }
        return vector.getObject(i);
    }
}

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
import java.time.LocalDate;
import java.time.ZoneOffset;
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
import org.opensearch.common.Rounding;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Cast;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Expression;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.FieldReference;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Float64Literal;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.IfThen;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Int64Literal;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.ScalarFunction;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.ScalarType;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.StringLiteral;
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

    public void testIfThenGroupsByTheFirstMatchingBranch() throws Exception {
        // CASE WHEN rating < 300 THEN 0 WHEN rating < 700 THEN 1 ELSE 2
        // END: the consumer turns the IfThen into DataFusion's Case, so
        // the rows fall into the three ranges; a null rating fails every
        // comparison and takes the ELSE branch. Without an ELSE the
        // rows no branch matches (null rating, rating >= 700) yield a
        // null key.
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(dir, "if-then", 3, 200);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Expression rating = new Cast(new FieldReference(fieldIndex(dataset, "rating")), ScalarType.FP64);
            List<IfThen.Branch> branches = List.of(
                new IfThen.Branch(ScalarFunction.of("lt", rating, new Float64Literal(300d)), new Int64Literal(0L)),
                new IfThen.Branch(ScalarFunction.of("lt", rating, new Float64Literal(700d)), new Int64Literal(1L))
            );
            Map<Long, Long> expected = new TreeMap<>();
            Map<Long, Long> expectedWithoutElse = new TreeMap<>();
            long nulls = 0;
            long unmatched = 0;
            for (int i = 0; i < 600; i++) {
                if (i % 5 == 4) {
                    nulls++;
                    continue;
                }
                long r = (i * 37L) % 1000L;
                long branch = r < 300 ? 0L : r < 700 ? 1L : 2L;
                expected.merge(branch, 1L, Long::sum);
                if (branch < 2L) {
                    expectedWithoutElse.merge(branch, 1L, Long::sum);
                } else {
                    unmatched++;
                }
            }
            expected.merge(2L, nulls, Long::sum);

            ByteBuffer withElse = new SubstraitAggregatePlan.Builder().groupBy(new IfThen(branches, new Int64Literal(2L)), "k")
                .measure("count", List.of(), ScalarType.I64, "n")
                .build();
            assertEquals(expected, keyCounts(scan(dataset, withElse, null, null)));

            ByteBuffer withoutElse = new SubstraitAggregatePlan.Builder().groupBy(new IfThen(branches, null), "k")
                .measure("count", List.of(), ScalarType.I64, "n")
                .build();
            List<Map<String, Object>> rows = scan(dataset, withoutElse, null, null);
            Map<Long, Long> counts = new TreeMap<>();
            long nullKeyed = 0;
            for (Map<String, Object> row : rows) {
                if (row.get("k") == null) {
                    nullKeyed += (Long) row.get("n");
                } else {
                    counts.put((Long) row.get("k"), (Long) row.get("n"));
                }
            }
            assertEquals(expectedWithoutElse, counts);
            assertEquals(nulls + unmatched, nullKeyed);
        }
    }

    public void testMatchMaskCountsARowTowardEveryOverlappingCondition() throws Exception {
        // Three overlapping conditions: rating < 500 (bit 1), rating >=
        // 300 (bit 2) and category IS NULL (bit 4). The mask groups the
        // rows by the set of conditions they satisfy; a null rating
        // fails both comparisons and only sets bit 4 when its category
        // is null too. NOT (flag IS TRUE) keeps the rows whose flag is
        // false or null, the way a must_not clause does.
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(dir, "mask", 3, 200);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Expression rating = new Cast(new FieldReference(fieldIndex(dataset, "rating")), ScalarType.FP64);
            Expression category = new FieldReference(fieldIndex(dataset, "category"));
            Expression flag = new FieldReference(fieldIndex(dataset, "flag"));
            Expression mask = SubstraitExpressions.matchMask(
                List.of(
                    ScalarFunction.of("lt", rating, new Float64Literal(500d)),
                    ScalarFunction.of("gte", rating, new Float64Literal(300d)),
                    SubstraitExpressions.isNull(category)
                )
            );
            ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(mask, "k")
                .measure("count", List.of(), ScalarType.I64, "n")
                .measure("sum", List.of(new Cast(SubstraitExpressions.notTrue(flag), ScalarType.I64)), ScalarType.I64, "not_flagged")
                .build();
            Map<Long, Long> expected = new TreeMap<>();
            Map<Long, Long> expectedNotFlagged = new TreeMap<>();
            for (int i = 0; i < 600; i++) {
                long bits = 0;
                if (i % 5 != 4) {
                    long r = (i * 37L) % 1000L;
                    bits |= r < 500 ? 1 : 0;
                    bits |= r >= 300 ? 2 : 0;
                }
                bits |= i % 4 == 3 ? 4 : 0;
                expected.merge(bits, 1L, Long::sum);
                boolean flagged = i % 7 != 6 && i % 2 == 0;
                expectedNotFlagged.merge(bits, flagged ? 0L : 1L, Long::sum);
            }
            List<Map<String, Object>> rows = scan(dataset, plan, null, null);
            Map<Long, Long> notFlagged = new TreeMap<>();
            for (Map<String, Object> row : rows) {
                notFlagged.put((Long) row.get("k"), (Long) row.get("not_flagged"));
            }
            assertEquals(expected, keyCounts(rows));
            assertEquals(expectedNotFlagged, notFlagged);
        }
    }

    public void testSquareAndBooleanOperatorsInMeasures() throws Exception {
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(dir, "square", 3, 200);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Expression rating = new FieldReference(fieldIndex(dataset, "rating"));
            Expression category = new FieldReference(fieldIndex(dataset, "category"));
            Expression flag = new FieldReference(fieldIndex(dataset, "flag"));
            // AND / OR of a comparison and an equality, summed as 0 / 1.
            Expression c1AndHigh = SubstraitExpressions.and(
                ScalarFunction.of("equal", category, new StringLiteral("c1")),
                ScalarFunction.of("gte", new Cast(rating, ScalarType.I64), new Int64Literal(500L))
            );
            Expression c1OrFlag = SubstraitExpressions.or(
                ScalarFunction.of("equal", category, new StringLiteral("c1")),
                ScalarFunction.of("equal", new Cast(flag, ScalarType.I64), new Int64Literal(1L))
            );
            ByteBuffer plan = new SubstraitAggregatePlan.Builder().measure("count", List.of(), ScalarType.I64, "n")
                .measure("sum", List.of(SubstraitExpressions.square(rating)), ScalarType.FP64, "sq")
                .measure("sum", List.of(new Cast(c1AndHigh, ScalarType.I64)), ScalarType.I64, "c1_high")
                .measure("sum", List.of(new Cast(c1OrFlag, ScalarType.I64)), ScalarType.I64, "c1_or_flag")
                .measure("sum", List.of(new Cast(SubstraitExpressions.isNotNull(rating), ScalarType.I64)), ScalarType.I64, "rated")
                .build();
            double expectedSquares = 0;
            long expectedC1High = 0;
            long expectedC1OrFlag = 0;
            long expectedRated = 0;
            for (int i = 0; i < 600; i++) {
                boolean rated = i % 5 != 4;
                long r = (i * 37L) % 1000L;
                boolean c1 = i % 4 != 3 && i % 3 == 1;
                boolean flagged = i % 7 != 6 && i % 2 == 0;
                if (rated) {
                    expectedSquares += (double) r * r;
                    expectedRated++;
                }
                if (c1 && rated && r >= 500) {
                    expectedC1High++;
                }
                if (c1 || flagged) {
                    expectedC1OrFlag++;
                }
            }
            Map<String, Object> row = scan(dataset, plan, null, null).get(0);
            assertEquals(600L, row.get("n"));
            assertEquals(expectedSquares, (Double) row.get("sq"), 0d);
            assertEquals(expectedC1High, row.get("c1_high"));
            assertEquals(expectedC1OrFlag, row.get("c1_or_flag"));
            assertEquals(expectedRated, row.get("rated"));
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

    public void testDateTruncGroupsByCalendarMonthAndDay() throws Exception {
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeDatedTable(dir, "dated-calendar");
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            ArrowType tsType = fieldType(dataset, "ts");
            Expression ts = new FieldReference(fieldIndex(dataset, "ts"));
            String[] days = { "2024-01-15", "2024-02-20", "2024-03-10", "2024-03-25", "2024-04-05", "2024-05-30" };
            for (String unit : new String[] { "month", "day" }) {
                ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(
                    SubstraitExpressions.epochMillis(SubstraitExpressions.dateTrunc(unit, ts), tsType),
                    "k"
                )
                    .measure("count", List.of(), ScalarType.I64, "n")
                    .measure("sum", List.of(new FieldReference(fieldIndex(dataset, "id"))), ScalarType.I64, "s")
                    .build();
                Map<Long, Long> expected = new TreeMap<>();
                Map<Long, Long> expectedSums = new TreeMap<>();
                for (int id = 0; id < days.length; id++) {
                    LocalDate day = LocalDate.parse(days[id]);
                    LocalDate start = unit.equals("month") ? day.withDayOfMonth(1) : day;
                    long key = start.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
                    expected.merge(key, 1L, Long::sum);
                    expectedSums.merge(key, (long) id, Long::sum);
                }
                List<Map<String, Object>> rows = scan(dataset, plan, null, null);
                assertEquals(unit, expected, keyCounts(rows));
                Map<Long, Long> sums = new TreeMap<>();
                for (Map<String, Object> row : rows) {
                    sums.put((Long) row.get("k"), (Long) row.get("s"));
                }
                assertEquals(unit, expectedSums, sums);
            }
            // The March bucket of the month plan holds two rows.
            assertEquals(
                Map.of(
                    Instant.parse("2024-01-01T00:00:00Z").toEpochMilli(),
                    1L,
                    Instant.parse("2024-02-01T00:00:00Z").toEpochMilli(),
                    1L,
                    Instant.parse("2024-03-01T00:00:00Z").toEpochMilli(),
                    2L,
                    Instant.parse("2024-04-01T00:00:00Z").toEpochMilli(),
                    1L,
                    Instant.parse("2024-05-01T00:00:00Z").toEpochMilli(),
                    1L
                ),
                keyCounts(
                    scan(
                        dataset,
                        new SubstraitAggregatePlan.Builder().groupBy(
                            SubstraitExpressions.epochMillis(SubstraitExpressions.dateTrunc("month", ts), tsType),
                            "k"
                        ).measure("count", List.of(), ScalarType.I64, "n").build(),
                        null,
                        null
                    )
                )
            );
        }
    }

    public void testDateTruncRoundsWeeksQuartersAndYearsLikeTheAggregator() throws Exception {
        // Millisecond timestamps on the days around the epoch: 1969
        // rows have negative millis, the week of the epoch starts on
        // Monday 1969-12-29, and the quarter and year starts fall in
        // 1969 for the negative rows. Hour, minute and second are
        // identities on these midnight values and confirm the units
        // resolve and keep the key.
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeSignedValuesTable(dir, "signed-calendar");
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            ArrowType tsType = fieldType(dataset, "ts");
            Expression ts = new FieldReference(fieldIndex(dataset, "ts"));
            Map<String, Rounding.DateTimeUnit> units = Map.of(
                "week",
                Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR,
                "quarter",
                Rounding.DateTimeUnit.QUARTER_OF_YEAR,
                "year",
                Rounding.DateTimeUnit.YEAR_OF_CENTURY,
                "hour",
                Rounding.DateTimeUnit.HOUR_OF_DAY,
                "minute",
                Rounding.DateTimeUnit.MINUTES_OF_HOUR,
                "second",
                Rounding.DateTimeUnit.SECOND_OF_MINUTE
            );
            for (Map.Entry<String, Rounding.DateTimeUnit> entry : units.entrySet()) {
                ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(
                    SubstraitExpressions.epochMillis(SubstraitExpressions.dateTrunc(entry.getKey(), ts), tsType),
                    "k"
                ).measure("count", List.of(), ScalarType.I64, "n").build();
                Rounding rounding = Rounding.builder(entry.getValue()).build();
                Map<Long, Long> expected = new TreeMap<>();
                for (long value : LanceTableFactory.SIGNED_VALUES) {
                    expected.merge(rounding.round(value * 86_400_000L), 1L, Long::sum);
                }
                assertEquals(entry.getKey(), expected, keyCounts(scan(dataset, plan, null, null)));
            }
        }
    }

    private static Map<Long, Long> keyCounts(List<Map<String, Object>> rows) {
        Map<Long, Long> counts = new TreeMap<>();
        for (Map<String, Object> row : rows) {
            counts.put((Long) row.get("k"), (Long) row.get("n"));
        }
        return counts;
    }

    public void testTopKEncodesFetchAndSortAboveTheAggregate() {
        ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(new FieldReference(2), "k")
            .measure("count", List.of(), ScalarType.I64, "n")
            .topK(new FieldReference(1), false, 25L)
            .build();
        byte[] bytes = new byte[plan.remaining()];
        plan.duplicate().get(bytes);
        // Plan: relations = 3. PlanRel: root = 2. RelRoot: input = 1.
        byte[] relRoot = message(message(bytes, 3), 2);
        byte[] rel = message(relRoot, 1);
        // Rel: fetch = 3. FetchRel: input = 2, count = 4.
        byte[] fetch = message(rel, 3);
        assertEquals(25L, varint(fetch, 4));
        // FetchRel.input is Rel { sort = 5 }; SortRel: input = 2, sorts = 3.
        byte[] sort = message(message(fetch, 2), 5);
        byte[] sortField = message(sort, 3);
        // SortField: expr = 1 (present), direction = 2
        // (SORT_DIRECTION_DESC_NULLS_LAST = 4).
        assertNotNull(message(sortField, 1));
        assertEquals(4L, varint(sortField, 2));
        // SortRel.input is Rel { aggregate = 4 } with the groupings (3)
        // and measures (4) of the plain plan.
        byte[] aggregate = message(message(sort, 2), 4);
        assertNotNull(message(aggregate, 3));
        assertNotNull(message(aggregate, 4));

        ByteBuffer ascending = new SubstraitAggregatePlan.Builder().measure("count", List.of(), ScalarType.I64, "n")
            .topK(new FieldReference(0), true, 7L)
            .build();
        byte[] ascendingBytes = new byte[ascending.remaining()];
        ascending.duplicate().get(ascendingBytes);
        byte[] ascendingFetch = message(message(message(message(ascendingBytes, 3), 2), 1), 3);
        assertEquals(7L, varint(ascendingFetch, 4));
        byte[] ascendingSort = message(message(ascendingFetch, 2), 5);
        // SORT_DIRECTION_ASC_NULLS_LAST = 2.
        assertEquals(2L, varint(message(ascendingSort, 3), 2));
    }

    public void testLanceRejectsATopKPlan() throws Exception {
        // Lance 12's Substrait consumer takes an AggregateRel root only;
        // the executor therefore never sends a topK plan today, and this
        // pins the rejection that keeps it that way.
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(dir, "topk-reject", 1, 100);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            ByteBuffer plan = new SubstraitAggregatePlan.Builder().groupBy(new FieldReference(fieldIndex(dataset, "category")), "k")
                .measure("count", List.of(), ScalarType.I64, "n")
                .topK(new FieldReference(1), false, 10L)
                .build();
            Exception rejected = expectThrows(Exception.class, () -> scan(dataset, plan, null, null));
            assertTrue(rejected.getMessage(), rejected.getMessage().contains("Expected Substrait AggregateRel"));
        }
    }

    /** The first {@code fieldNumber} length delimited field of {@code bytes}, or null. */
    private static byte[] message(byte[] bytes, int fieldNumber) {
        int i = 0;
        while (i < bytes.length) {
            long[] tag = readVarint(bytes, i);
            i = (int) tag[1];
            int field = (int) (tag[0] >>> 3);
            int wire = (int) (tag[0] & 7);
            if (wire == 2) {
                long[] length = readVarint(bytes, i);
                i = (int) length[1];
                if (field == fieldNumber) {
                    byte[] value = new byte[(int) length[0]];
                    System.arraycopy(bytes, i, value, 0, value.length);
                    return value;
                }
                i += (int) length[0];
            } else if (wire == 0) {
                i = (int) readVarint(bytes, i)[1];
            } else if (wire == 1) {
                i += 8;
            } else {
                fail("unexpected wire type " + wire);
            }
        }
        return null;
    }

    /** The first {@code fieldNumber} varint field of {@code bytes}, or -1. */
    private static long varint(byte[] bytes, int fieldNumber) {
        int i = 0;
        while (i < bytes.length) {
            long[] tag = readVarint(bytes, i);
            i = (int) tag[1];
            int field = (int) (tag[0] >>> 3);
            int wire = (int) (tag[0] & 7);
            if (wire == 0) {
                long[] value = readVarint(bytes, i);
                i = (int) value[1];
                if (field == fieldNumber) {
                    return value[0];
                }
            } else if (wire == 2) {
                long[] length = readVarint(bytes, i);
                i = (int) (length[1] + length[0]);
            } else if (wire == 1) {
                i += 8;
            } else {
                fail("unexpected wire type " + wire);
            }
        }
        return -1L;
    }

    /** Reads a varint at {@code offset}: {value, next offset}. */
    private static long[] readVarint(byte[] bytes, int offset) {
        long value = 0L;
        int shift = 0;
        int i = offset;
        while (true) {
            byte b = bytes[i++];
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return new long[] { value, i };
            }
            shift += 7;
        }
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.prune;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.NlsString;
import org.lance.index.IndexType;
import org.lance.index.scalar.ZoneStats;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.RequestPlanner;
import org.opensearch.lance.plan.metadata.ColumnStatistics;
import org.opensearch.lance.plan.metadata.ColumnStatistics.IndexSummary;
import org.opensearch.lance.plan.metadata.TableStatistics;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * {@link ZoneMapPruner} over a fixture of zone maps: which fragments a
 * range, term, terms, exists, bool and temporal predicate exclude, and
 * which it keeps because the zone map cannot prove them empty (a
 * fragment the map does not cover, unknown bounds, a negation, a
 * floating point zone with a {@code NaN} bound). The end of the class
 * pins that {@link RequestPlanner#plan} carries the exclusions on the
 * {@link FragmentPlan} for every shape and leaves a {@code post_filter}
 * alone.
 */
public class ZoneMapPrunerTests extends OpenSearchTestCase {

    private static final long DAY_2024_01_01 = 19723L;
    private static final long MICROS_2024_01_01 = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli() * 1000L;
    private static final long MICROS_PER_DAY = 24L * 60L * 60L * 1_000_000L;

    /**
     * The fixture table over {@link PlanTestFixtures#SCHEMA}: six
     * fragments of 200 rows.
     *
     * <ul>
     *   <li>{@code rating} (int32): fragment 0 zones {@code [0, 99]} and
     *       {@code [100, 199]}; fragment 1 {@code [200, 399]}; fragment
     *       2 all null; fragment 3 {@code [400, 500]} with five nulls;
     *       fragments 4 and 5 not covered</li>
     *   <li>{@code category} (utf8): fragment 0 {@code ["a", "c"]};
     *       fragment 1 {@code ["d", "f"]}; fragment 5
     *       {@code ["b", "b"]}; fragment 6 {@code [U+E000, U+1F600]};
     *       others not covered</li>
     *   <li>{@code price} (float64): fragment 0 {@code [1.5, 9.5]};
     *       fragment 1 {@code [2.0, NaN]} (a zone holding a NaN)</li>
     *   <li>{@code flag} (bool): fragment 0 {@code [false, false]}</li>
     *   <li>{@code day} (date32): fragment 0 the first eight days of
     *       2024</li>
     *   <li>{@code ts} (microsecond timestamp): fragment 0 from
     *       2024-01-01T00:00:00Z to 2024-01-02T00:00:00Z inclusive</li>
     * </ul>
     */
    private static TableStatistics statistics() {
        Map<String, ColumnStatistics> columns = new LinkedHashMap<>();
        columns.put(
            "rating",
            zoneMapped(
                "rating",
                new ZoneStats(0, 0L, 100L, 0L, 99L, 0L),
                new ZoneStats(0, 100L, 100L, 100L, 199L, 0L),
                new ZoneStats(1, 0L, 200L, 200L, 399L, 0L),
                new ZoneStats(2, 0L, 200L, null, null, 200L),
                new ZoneStats(3, 0L, 200L, 400L, 500L, 5L)
            )
        );
        columns.put(
            "category",
            zoneMapped(
                "category",
                new ZoneStats(0, 0L, 200L, "a", "c", 0L),
                new ZoneStats(1, 0L, 200L, "d", "f", 0L),
                new ZoneStats(5, 0L, 200L, "b", "b", 0L),
                new ZoneStats(6, 0L, 200L, "\uE000", "\uD83D\uDE00", 0L)
            )
        );
        columns.put(
            "price",
            zoneMapped("price", new ZoneStats(0, 0L, 200L, 1.5d, 9.5d, 0L), new ZoneStats(1, 0L, 200L, 2.0d, Double.NaN, 0L))
        );
        columns.put("flag", zoneMapped("flag", new ZoneStats(0, 0L, 200L, Boolean.FALSE, Boolean.FALSE, 0L)));
        columns.put("day", zoneMapped("day", new ZoneStats(0, 0L, 200L, DAY_2024_01_01, DAY_2024_01_01 + 7L, 0L)));
        columns.put("ts", zoneMapped("ts", new ZoneStats(0, 0L, 200L, MICROS_2024_01_01, MICROS_2024_01_01 + MICROS_PER_DAY, 0L)));
        List<TableStatistics.FragmentStats> fragments = List.of(
            new TableStatistics.FragmentStats(0, 200L, 1),
            new TableStatistics.FragmentStats(1, 200L, 1),
            new TableStatistics.FragmentStats(2, 200L, 1),
            new TableStatistics.FragmentStats(3, 200L, 1),
            new TableStatistics.FragmentStats(4, 200L, 1),
            new TableStatistics.FragmentStats(5, 200L, 1),
            new TableStatistics.FragmentStats(6, 200L, 1)
        );
        return new TableStatistics(1400L, 0L, fragments, columns, 3L, Instant.EPOCH);
    }

    private static ColumnStatistics zoneMapped(String column, ZoneStats... zones) {
        IndexSummary index = new IndexSummary(
            column + "_zonemap",
            Optional.of(IndexType.ZONEMAP),
            7,
            7,
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            true
        );
        return new ColumnStatistics(column, List.of(index), List.of(zones));
    }

    private static LanceSchemas.IndexModel model() {
        LinkedHashMap<String, String> bodySubs = new LinkedHashMap<>();
        bodySubs.put("raw", "keyword");
        return LanceSchemas.model("idx", PlanTestFixtures.SCHEMA, Map.of("body", bodySubs), Map.of(), "", Set.of(), statistics());
    }

    /** The fragments the top level {@code query} of {@code json} excludes. */
    private static SortedSet<Integer> excluded(String json) throws IOException {
        SearchSourceBuilder source = PlanTestFixtures.parse(json);
        LanceSchemas.IndexModel model = model();
        RelNode logical = SearchRequestToRel.translateQuery(source.query(), model, PlanTestFixtures.factory());
        return ZoneMapPruner.prune(logical, model.table());
    }

    private static SortedSet<Integer> ids(Integer... fragmentIds) {
        return new TreeSet<>(List.of(fragmentIds));
    }

    public void testRangeLowerBoundExcludesZonesBelowIt() throws IOException {
        // [0, 199] and the all null fragment cannot hold a rating of 300
        // or more; [200, 399] and [400, 500] can; 4 and 5 are not covered.
        assertEquals(ids(0, 2), excluded("{\"query\":{\"range\":{\"rating\":{\"gte\":300}}}}"));
        assertEquals(ids(0, 1, 2), excluded("{\"query\":{\"range\":{\"rating\":{\"gt\":399}}}}"));
        assertEquals(ids(0, 2), excluded("{\"query\":{\"range\":{\"rating\":{\"gte\":399}}}}"));
    }

    public void testRangeUpperBoundExcludesZonesAboveIt() throws IOException {
        assertEquals(ids(1, 2, 3), excluded("{\"query\":{\"range\":{\"rating\":{\"lt\":150}}}}"));
        assertEquals(ids(1, 2, 3), excluded("{\"query\":{\"range\":{\"rating\":{\"lt\":200}}}}"));
        assertEquals(ids(2, 3), excluded("{\"query\":{\"range\":{\"rating\":{\"lte\":200}}}}"));
    }

    public void testRangeOutsideEveryZoneExcludesEveryCoveredFragmentOnly() throws IOException {
        // Every covered fragment is excluded; the uncovered fragments 4
        // and 5 stay, because the zone map says nothing about them.
        assertEquals(ids(0, 1, 2, 3), excluded("{\"query\":{\"range\":{\"rating\":{\"lt\":0}}}}"));
        assertEquals(ids(0, 1, 2, 3), excluded("{\"query\":{\"range\":{\"rating\":{\"gt\":500}}}}"));
    }

    public void testBoundedRangeKeepsTheZonesItOverlaps() throws IOException {
        assertEquals(ids(0, 2, 3), excluded("{\"query\":{\"range\":{\"rating\":{\"gte\":250,\"lte\":350}}}}"));
        assertEquals(ids(2), excluded("{\"query\":{\"range\":{\"rating\":{\"gte\":150,\"lte\":450}}}}"));
    }

    public void testTermExcludesZonesWhoseBoundsMissTheValue() throws IOException {
        assertEquals(ids(1, 2, 3), excluded("{\"query\":{\"term\":{\"rating\":150}}}"));
        assertEquals(ids(0, 1, 2), excluded("{\"query\":{\"term\":{\"rating\":500}}}"));
        assertEquals(ids(0, 1, 2, 3), excluded("{\"query\":{\"term\":{\"rating\":501}}}"));
    }

    public void testTermsIsADisjunctionAndKeepsAFragmentAnyValueMayHit() throws IOException {
        assertEquals(ids(1, 2), excluded("{\"query\":{\"terms\":{\"rating\":[150,450]}}}"));
    }

    public void testExistsExcludesTheAllNullFragmentOnly() throws IOException {
        assertEquals(ids(2), excluded("{\"query\":{\"exists\":{\"field\":\"rating\"}}}"));
    }

    public void testNegationIsNeverRead() throws IOException {
        // NOT (x IS TRUE) keeps rows without a value, and the pruner does
        // not reason about it: nothing is excluded.
        assertEquals(ids(), excluded("{\"query\":{\"bool\":{\"must_not\":[{\"exists\":{\"field\":\"rating\"}}]}}}"));
        assertEquals(ids(), excluded("{\"query\":{\"bool\":{\"must_not\":[{\"range\":{\"rating\":{\"gte\":300}}}]}}}"));
    }

    public void testConjunctionExcludesWhatAnyBranchExcludes() throws IOException {
        // rating >= 300 excludes 0 and 2; category = b excludes 1 (d..f)
        // and 6 (U+E000..U+1F600), keeps 0 (a..c) and 5 (b..b).
        String body =
            "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"rating\":{\"gte\":300}}}],\"filter\":[{\"term\":{\"category\":\"b\"}}]}}}";
        assertEquals(ids(0, 1, 2, 6), excluded(body));
    }

    public void testDisjunctionExcludesOnlyWhatEveryBranchExcludes() throws IOException {
        String body = "{\"query\":{\"bool\":{\"should\":[{\"range\":{\"rating\":{\"lt\":100}}},{\"range\":{\"rating\":{\"gt\":450}}}]}}}";
        assertEquals(ids(1, 2), excluded(body));
    }

    public void testStringBoundsCompareByCodePoint() throws IOException {
        // U+FF01 lies between U+E000 and U+1F600 in code point (and
        // UTF-8) order, so fragment 6 may hold it; UTF-16 unit order
        // would place it above the surrogate pair and wrongly exclude
        // the fragment. Fragments 0, 1 and 5 miss it. The literal is
        // built directly: the translator's literal encoding does not
        // accept a character outside ISO-8859-1.
        LanceSchemas.IndexModel model = model();
        RelBuilder relBuilder = PlanTestFixtures.factory().relBuilder(model.schema());
        relBuilder.scan(LancePlannerFactory.SCHEMA_NAME, model.indexName());
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        RelDataType utf8 = rexBuilder.getTypeFactory()
            .createTypeWithCharsetAndCollation(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                StandardCharsets.UTF_8,
                SqlCollation.IMPLICIT
            );
        RexNode literal = rexBuilder.makeLiteral(new NlsString("\uFF01", "UTF-8", SqlCollation.IMPLICIT), utf8, false);
        RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, relBuilder.field("category"), literal);
        assertEquals(ids(0, 1, 5), ZoneMapPruner.prune(predicate, relBuilder.peek().getRowType(), PlanTestFixtures.SCHEMA, statistics()));
        // A value outside every zone excludes every covered fragment.
        assertEquals(ids(0, 1, 5, 6), excluded("{\"query\":{\"term\":{\"category\":\"zz\"}}}"));
    }

    public void testFloatingPointZoneWithANaNBoundIsUnknown() throws IOException {
        // Fragment 0 [1.5, 9.5] cannot hold 1000; fragment 1's NaN
        // maximum hides its true maximum, so it stays.
        assertEquals(ids(0), excluded("{\"query\":{\"range\":{\"price\":{\"gt\":1000}}}}"));
        assertEquals(ids(), excluded("{\"query\":{\"range\":{\"price\":{\"gte\":2.5}}}}"));
    }

    public void testBooleanTermReadsTheBoolZone() throws IOException {
        assertEquals(ids(0), excluded("{\"query\":{\"term\":{\"flag\":true}}}"));
        assertEquals(ids(), excluded("{\"query\":{\"term\":{\"flag\":false}}}"));
    }

    public void testDateColumnComparesInEnclosingMillis() throws IOException {
        // The zone spans 2024-01-01 to 2024-01-08; a bound past it
        // excludes, a bound inside or at its edge keeps.
        assertEquals(ids(0), excluded("{\"query\":{\"range\":{\"day\":{\"gte\":\"2024-02-01\"}}}}"));
        assertEquals(ids(0), excluded("{\"query\":{\"range\":{\"day\":{\"lt\":\"2024-01-01\"}}}}"));
        assertEquals(ids(), excluded("{\"query\":{\"range\":{\"day\":{\"lte\":\"2024-01-01\"}}}}"));
        assertEquals(ids(), excluded("{\"query\":{\"range\":{\"day\":{\"gte\":\"2024-01-08\"}}}}"));
        assertEquals(ids(0), excluded("{\"query\":{\"range\":{\"day\":{\"gt\":\"2024-01-08\"}}}}"));
    }

    public void testTimestampColumnComparesInEnclosingMillis() throws IOException {
        assertEquals(ids(0), excluded("{\"query\":{\"range\":{\"ts\":{\"gt\":\"2024-01-03T00:00:00Z\"}}}}"));
        assertEquals(ids(), excluded("{\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-01-02T00:00:00.000Z\"}}}}"));
        assertEquals(ids(0), excluded("{\"query\":{\"range\":{\"ts\":{\"gt\":\"2024-01-02T00:00:00.000Z\"}}}}"));
        assertEquals(ids(), excluded("{\"query\":{\"term\":{\"ts\":\"2024-01-01T12:00:00Z\"}}}"));
    }

    public void testColumnWithoutAReadZoneMapExcludesNothing() throws IOException {
        // id carries no zone map in the fixture.
        assertEquals(ids(), excluded("{\"query\":{\"range\":{\"id\":{\"gte\":100000}}}}"));
        assertEquals(ids(), excluded("{\"query\":{\"match_all\":{}}}"));
    }

    public void testMillisRangeWidensEveryUnit() {
        assertArrayEquals(
            new long[] { DAY_2024_01_01 * 86_400_000L, (DAY_2024_01_01 + 1L) * 86_400_000L - 1L },
            ZoneMapPruner.millisRange(new ArrowType.Date(DateUnit.DAY), DAY_2024_01_01, DAY_2024_01_01)
        );
        assertArrayEquals(new long[] { 5L, 9L }, ZoneMapPruner.millisRange(new ArrowType.Date(DateUnit.MILLISECOND), 5L, 9L));
        assertArrayEquals(new long[] { 5000L, 9999L }, ZoneMapPruner.millisRange(new ArrowType.Timestamp(TimeUnit.SECOND, null), 5L, 9L));
        assertArrayEquals(new long[] { 5L, 9L }, ZoneMapPruner.millisRange(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), 5L, 9L));
        assertArrayEquals(
            new long[] { 1L, 3L },
            ZoneMapPruner.millisRange(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), 1_500L, 2_001L)
        );
        assertArrayEquals(
            new long[] { -2L, 3L },
            ZoneMapPruner.millisRange(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), -1_500_000L, 2_000_001L)
        );
        assertNull(ZoneMapPruner.millisRange(new ArrowType.Int(32, true), 1L, 2L));
        assertNull(ZoneMapPruner.millisRange(new ArrowType.Date(DateUnit.DAY), Long.MAX_VALUE / 2L, Long.MAX_VALUE / 2L));
    }

    // ---------------------------------------------------------------
    // Through the planner
    // ---------------------------------------------------------------

    private static ExecutionShape shape(String json) throws IOException {
        SearchSourceBuilder source = PlanTestFixtures.parse(json);
        int size = source.size() < 0 ? 10 : source.size();
        return new ExecutionShape(
            source.query(),
            source.postFilter(),
            source.sorts() == null ? List.of() : source.sorts(),
            source.searchAfter(),
            0,
            size,
            source.aggregations(),
            false
        );
    }

    private static FragmentPlan planned(String json) throws IOException {
        return RequestPlanner.plan(shape(json), model(), Set.of(), PlanTestFixtures.factory()).plan();
    }

    public void testPlannerCarriesTheExclusionsOnEveryShape() throws IOException {
        int[] expected = { 0, 2 };
        String range = "{\"range\":{\"rating\":{\"gte\":300}}}";
        // A pushed aggregate over the filter.
        FragmentPlan aggregate = planned("{\"size\":0,\"query\":" + range + ",\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}");
        assertNotNull(aggregate.aggregate());
        assertArrayEquals(expected, aggregate.excludedFragmentIds());
        // A pushed page over the filter.
        FragmentPlan page = planned("{\"size\":10,\"query\":" + range + ",\"sort\":[{\"price\":\"desc\"}]}");
        assertNotNull(page.topK());
        assertArrayEquals(expected, page.excludedFragmentIds());
        // The count route.
        FragmentPlan count = planned("{\"size\":0,\"query\":" + range + "}");
        assertEquals(FragmentPlan.Kind.LUCENE_COUNT, count.kind());
        assertArrayEquals(expected, count.excludedFragmentIds());
        // A full text clause with the filter as its prefilter.
        FragmentPlan fts = planned(
            "{\"size\":10,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],\"filter\":["
                + range
                + "]}}}"
        );
        assertNotNull(fts.lanceClause());
        assertArrayEquals(expected, fts.excludedFragmentIds());
        // A knn clause with the filter as its prefilter.
        FragmentPlan knn = planned(
            "{\"size\":5,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[0.1,0.2],\"k\":5,\"filter\":" + range + "}}}"
        );
        assertTrue(knn.isKnn());
        assertArrayEquals(expected, knn.excludedFragmentIds());
    }

    public void testPlannerLeavesAPostFilterAndAnUnprunableQueryAlone() throws IOException {
        FragmentPlan postFiltered = planned("{\"size\":10,\"post_filter\":{\"range\":{\"rating\":{\"gte\":300}}}}");
        assertFalse(postFiltered.excludesFragments());
        FragmentPlan matchAll = planned("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}");
        assertFalse(matchAll.excludesFragments());
        FragmentPlan luceneQuery = planned("{\"size\":10,\"query\":{\"match\":{\"body\":\"hello\"}}}");
        assertFalse(luceneQuery.excludesFragments());
    }

    public void testExclusionsSurviveRefinementAndTheWire() throws IOException {
        FragmentPlan aggregate = planned(
            "{\"size\":0,\"query\":{\"range\":{\"rating\":{\"gte\":300}}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}"
        );
        assertArrayEquals(new int[] { 0, 2 }, aggregate.withoutAggregate().excludedFragmentIds());
        assertArrayEquals(new int[] { 0, 2 }, aggregate.withoutTopK().excludedFragmentIds());
        assertEquals(List.of(1, 3, 4), aggregate.retainedFragments(List.of(0, 1, 2, 3, 4)));
        assertEquals(List.of(), aggregate.retainedFragments(List.of(0, 2)));
        List<Integer> untouched = List.of(1, 3);
        assertSame(untouched, aggregate.retainedFragments(untouched));
        assertTrue(aggregate.toString().endsWith(" excluded=[0, 2]"));
    }

    public void testExcludedIdsMustBeAscendingAndDistinct() {
        FragmentPlan plan = FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_COUNT, null);
        expectThrows(IllegalArgumentException.class, () -> plan.withExcludedFragments(new int[] { 2, 1 }));
        expectThrows(IllegalArgumentException.class, () -> plan.withExcludedFragments(new int[] { 1, 1 }));
        assertSame(plan, plan.withExcludedFragments(new int[0]));
    }
}

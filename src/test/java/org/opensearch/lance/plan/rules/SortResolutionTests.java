/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.GeoDistanceSortBuilder;
import org.opensearch.search.sort.NestedSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortMode;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import org.lance.ipc.ColumnOrdering;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Sort resolution between the request's builders, the plan's
 * collations and the Lance orderings: field and direction mapping,
 * {@code missing} to the null direction, the multi-field sub-field to
 * its base column, the score sort to the FTS score column, and the
 * refusal message of every unpushable clause.
 */
public class SortResolutionTests extends OpenSearchTestCase {

    private static LanceSchemas.IndexModel model() {
        LinkedHashMap<String, String> bodySubs = new LinkedHashMap<>();
        bodySubs.put("raw", "keyword");
        return LanceSchemas.model("idx", PlanTestFixtures.SCHEMA, Map.of("body", bodySubs), Map.of("old_price", "price"), () -> 512L);
    }

    private static LanceTableScan scan(LanceSchemas.IndexModel model) {
        return (LanceTableScan) PlanTestFixtures.factory().relBuilder(model.schema()).scan(LancePlannerFactory.SCHEMA_NAME, "idx").build();
    }

    private static String refusal(SortBuilder<?> sort) {
        LanceSchemas.IndexModel model = model();
        RelDataType rowType = scan(model).getRowType();
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> SortResolution.collationsOf(List.of(sort), rowType, model)
        );
        return e.getMessage();
    }

    public void testDirectionsAndMissingMapToTheCollation() {
        LanceSchemas.IndexModel model = model();
        RelDataType rowType = scan(model).getRowType();
        List<RelFieldCollation> collations = SortResolution.collationsOf(
            List.of(
                new FieldSortBuilder("rating").order(SortOrder.DESC),
                new FieldSortBuilder("category").order(SortOrder.ASC).missing("_first"),
                new FieldSortBuilder("price").missing("_last")
            ),
            rowType,
            model
        );
        assertEquals(1, collations.get(0).getFieldIndex());
        assertEquals(RelFieldCollation.Direction.DESCENDING, collations.get(0).getDirection());
        assertEquals(RelFieldCollation.NullDirection.LAST, collations.get(0).nullDirection);
        assertEquals(4, collations.get(1).getFieldIndex());
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(1).getDirection());
        assertEquals(RelFieldCollation.NullDirection.FIRST, collations.get(1).nullDirection);
        assertEquals(2, collations.get(2).getFieldIndex());
        assertEquals(RelFieldCollation.NullDirection.LAST, collations.get(2).nullDirection);
    }

    public void testKeywordSubFieldResolvesToTheBaseColumn() {
        LanceSchemas.IndexModel model = model();
        RelDataType rowType = scan(model).getRowType();
        List<RelFieldCollation> collations = SortResolution.collationsOf(
            List.of(new FieldSortBuilder("body.raw").order(SortOrder.DESC)),
            rowType,
            model
        );
        assertEquals(rowType.getFieldNames().indexOf("body"), collations.get(0).getFieldIndex());
    }

    public void testScoreSortResolvesToTheFtsScoreColumn() {
        LanceSchemas.IndexModel model = model();
        LanceTableScan scan = scan(model);
        LanceFtsMatch fts = new LanceFtsMatch(
            scan.getCluster(),
            scan.getCluster().traitSetOf(Convention.NONE),
            scan,
            LanceFtsMatch.Kind.MATCH,
            List.of("body"),
            new LanceMatchQueryBuilder("body", "hello")
        );
        List<RelFieldCollation> collations = SortResolution.collationsOf(List.of(new ScoreSortBuilder()), fts.getRowType(), model);
        assertEquals(fts.getRowType().getFieldNames().indexOf("_score"), collations.get(0).getFieldIndex());
        assertEquals(RelFieldCollation.Direction.DESCENDING, collations.get(0).getDirection());
        assertTrue(SortResolution.isScoreCollation(fts.getRowType(), collations.get(0)));
    }

    public void testRefusalMessages() {
        assertEquals("sort type [_geo_distance]", refusal(new GeoDistanceSortBuilder("location", 35.0, 139.0)));
        assertEquals(
            "sort on field [rating] with a nested sort",
            refusal(new FieldSortBuilder("rating").setNestedSort(new NestedSortBuilder("n")))
        );
        assertEquals("sort on field [rating] with mode [avg]", refusal(new FieldSortBuilder("rating").sortMode(SortMode.AVG)));
        assertEquals("sort on field [rating] with numeric_type [long]", refusal(new FieldSortBuilder("rating").setNumericType("long")));
        assertEquals("sort on field [rating] with missing [7]", refusal(new FieldSortBuilder("rating").missing(7)));
        assertEquals("sort on metadata field [_doc]", refusal(new FieldSortBuilder("_doc")));
        assertEquals("sort by [_score] ascending", refusal(new ScoreSortBuilder().order(SortOrder.ASC)));
        assertEquals("sort by [_score] without a full text or knn query", refusal(new ScoreSortBuilder()));
        assertEquals("sort field [old_price] was renamed to [price] in the Lance table", refusal(new FieldSortBuilder("old_price")));
        assertEquals("sort field [nope] does not map to a Lance column", refusal(new FieldSortBuilder("nope")));
        assertEquals("sort field [meta.child] does not map to a Lance column", refusal(new FieldSortBuilder("meta.child")));
    }

    public void testToOrderingsMapsDirectionAndNulls() {
        LanceSchemas.IndexModel model = model();
        RelDataType rowType = scan(model).getRowType();
        List<RelFieldCollation> collations = SortResolution.collationsOf(
            List.of(
                new FieldSortBuilder("rating").order(SortOrder.DESC),
                new FieldSortBuilder("category").order(SortOrder.ASC).missing("_first"),
                new FieldSortBuilder("ts"),
                new FieldSortBuilder("day"),
                new FieldSortBuilder("flag"),
                new FieldSortBuilder("weight")
            ),
            rowType,
            model
        );
        List<ColumnOrdering> orderings = SortResolution.toOrderings(rowType, collations, model.arrowSchema()).orElseThrow();
        assertEquals("rating", orderings.get(0).getColumnName());
        assertFalse(orderings.get(0).isAscending());
        assertFalse(orderings.get(0).isNullFirst());
        assertEquals("category", orderings.get(1).getColumnName());
        assertTrue(orderings.get(1).isAscending());
        assertTrue(orderings.get(1).isNullFirst());
        assertEquals(List.of("ts", "day", "flag", "weight"), orderings.subList(2, 6).stream().map(ColumnOrdering::getColumnName).toList());
    }

    public void testToOrderingsRefusesAScoreCollationAndAnUnknownColumn() {
        LanceSchemas.IndexModel model = model();
        LanceTableScan scan = scan(model);
        LanceFtsMatch fts = new LanceFtsMatch(
            scan.getCluster(),
            scan.getCluster().traitSetOf(Convention.NONE),
            scan,
            LanceFtsMatch.Kind.MATCH,
            List.of("body"),
            new LanceMatchQueryBuilder("body", "hello")
        );
        List<RelFieldCollation> score = SortResolution.collationsOf(List.of(new ScoreSortBuilder()), fts.getRowType(), model);
        Optional<List<ColumnOrdering>> orderings = SortResolution.toOrderings(fts.getRowType(), score, model.arrowSchema());
        assertTrue("no Arrow column stands behind _score", orderings.isEmpty());
    }
}

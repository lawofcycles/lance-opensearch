/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * The hit shape node: its row type is the hits envelope (one synthetic
 * column per rendered part), and the columns and flags print into the
 * digest so two shapes over the same input stay distinct.
 */
public class LanceHitShapeTests extends OpenSearchTestCase {

    private LanceTableScan scan() {
        return (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
    }

    private static LanceHitShape shape(RelNode input, boolean source, boolean id, boolean score, boolean sortValues) {
        return new LanceHitShape(
            input.getCluster(),
            input.getCluster().traitSetOf(Convention.NONE),
            input,
            List.of("id", "category"),
            source,
            id,
            score,
            sortValues
        );
    }

    public void testRowTypeTogglesTheEnvelopeParts() {
        LanceTableScan scan = scan();
        assertEquals(List.of("_id", "_source", "_score", "_sort"), shape(scan, true, true, true, true).getRowType().getFieldNames());
        assertEquals(List.of("_id", "_source", "_score"), shape(scan, true, true, true, false).getRowType().getFieldNames());
        assertEquals(List.of("_id", "_source"), shape(scan, true, true, false, false).getRowType().getFieldNames());
        assertEquals(List.of("_source"), shape(scan, true, false, false, false).getRowType().getFieldNames());
    }

    public void testDigestCarriesColumnsAndFlags() {
        LanceTableScan scan = scan();
        String scored = shape(scan, true, true, true, false).getDigest().toString();
        String sorted = shape(scan, true, true, false, true).getDigest().toString();
        assertNotEquals(scored, sorted);
        assertTrue(scored, scored.contains("columns=[id, category]"));
        assertTrue(scored, scored.contains("score=true"));
        assertTrue(sorted, sorted.contains("sortValues=true"));
    }

    public void testCopyKeepsTheShape() {
        LanceTableScan scan = scan();
        LanceHitShape shape = shape(scan, true, true, true, false);
        LanceHitShape copy = (LanceHitShape) shape.copy(shape.getTraitSet(), List.of(scan));
        assertEquals(shape.getRowType(), copy.getRowType());
        assertEquals(shape.outputColumns(), copy.outputColumns());
        assertTrue(copy.includeScore());
        assertFalse(copy.includeSortValues());
    }
}

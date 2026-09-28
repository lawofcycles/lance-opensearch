/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.opensearch.lance.LanceOverrides;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Set;

/**
 * {@link PlanExecutor#sqlExcludedColumns}: the columns whose
 * predicates never travel to Lance SQL.
 */
public class SqlExcludedColumnsTests extends OpenSearchTestCase {

    public void testIpAndGeoPointColumnsAreExcluded() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            Map.of("addr", Map.of("type", "ip"), "loc", Map.of("type", "geo_point")),
            null
        );
        assertEquals(Set.of("addr", "loc"), PlanExecutor.sqlExcludedColumns(overrides));
    }

    public void testNoOverridesExcludesNothing() {
        assertTrue(PlanExecutor.sqlExcludedColumns(LanceOverrides.EMPTY).isEmpty());
    }
}

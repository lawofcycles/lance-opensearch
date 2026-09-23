/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.opensearch.lance.LanceOverrides;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link PlanExecutor#sqlExcludedColumns}: the columns whose
 * predicates never travel to Lance SQL. A derived analyzer tokens
 * column is a Lance column the mapping does not expose, so a query
 * naming it would count through Lance while Lucene answers no
 * documents; excluding it keeps {@code hits.total} and the hits on the
 * same side.
 */
public class SqlExcludedColumnsTests extends OpenSearchTestCase {

    public void testIpAndGeoPointColumnsAreExcluded() {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            Map.of("addr", Map.of("type", "ip"), "loc", Map.of("type", "geo_point")),
            null
        );
        assertEquals(Set.of("addr", "loc"), PlanExecutor.sqlExcludedColumns(overrides));
    }

    public void testDerivedAnalyzerTokensColumnsAreExcluded() {
        Map<String, Object> clauses = new LinkedHashMap<>();
        clauses.put("body", Map.of("type", "text_analyzer", "analyzer", "english"));
        clauses.put("title", Map.of("type", "text_analyzer", "analyzer", "standard", "derived_column_name", "title_tokens"));
        clauses.put("addr", Map.of("type", "ip"));
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(clauses, null);
        Set<String> excluded = PlanExecutor.sqlExcludedColumns(overrides);
        assertEquals(Set.of("addr", "body" + LanceOverrides.DERIVED_COLUMN_SUFFIX, "title_tokens"), excluded);
        assertFalse("the base column stays queryable through Lance SQL", excluded.contains("body"));
        assertFalse(excluded.contains("title"));
        assertEquals("declaration order", List.of("addr", "body__lance_tokens", "title_tokens"), List.copyOf(excluded));
    }

    public void testNoOverridesExcludesNothing() {
        assertTrue(PlanExecutor.sqlExcludedColumns(LanceOverrides.EMPTY).isEmpty());
    }
}

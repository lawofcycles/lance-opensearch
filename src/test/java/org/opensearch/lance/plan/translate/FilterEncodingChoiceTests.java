/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.cost.StorageKind;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFilter;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Which encoding of a pushed filter the Volcano planner keeps: the two
 * pushdown rules register the SQL and the Substrait form of one
 * predicate in the same equivalence set and the scan's cost orders
 * them. A short predicate answers with the Substrait form on any fan
 * out; a long term list, whose Substrait message outweighs its SQL by
 * tens of KB, answers with the SQL form once the wire term over the fan
 * out passes the tie break.
 */
public class FilterEncodingChoiceTests extends OpenSearchTestCase {

    private static PushedFilter pushedFilter(String body, CostInputs inputs) throws IOException {
        SearchSourceBuilder source = PlanTestFixtures.parse(body);
        ExecutionShape shape = new ExecutionShape(source.query(), null, List.of(), null, 0, 0, null, false);
        LancePlannerFactory factory = PlanTestFixtures.factory();
        RelNode logical = SearchRequestToRel.translateForExecution(shape, PlanTestFixtures.model(), factory);
        RelNode physical = factory.plan(logical, inputs);
        assertTrue("the pushed scan answers the shape: " + physical, physical instanceof LanceTableScan);
        return ((LanceTableScan) physical).pushedFilter().orElseThrow();
    }

    private static CostInputs nodes(int nodes) {
        return new CostInputs(nodes, StorageKind.LOCAL, 8, 4, 4);
    }

    public void testShortPredicateAnswersWithTheSubstraitEncodingAndCarriesTheSql() throws IOException {
        PushedFilter pushed = pushedFilter("{\"size\":0,\"query\":{\"term\":{\"category\":\"c0\"}}}", nodes(1));
        assertTrue(pushed.usesSubstrait());
        assertEquals("category = 'c0'", pushed.sql());
    }

    public void testShortPredicateStaysSubstraitOnAWideFanOut() throws IOException {
        PushedFilter pushed = pushedFilter(
            "{\"size\":0,\"query\":{\"bool\":{\"filter\":[{\"term\":{\"category\":\"c0\"}},{\"range\":{\"rating\":{\"gte\":3}}}]}}}",
            nodes(50)
        );
        assertTrue(pushed.usesSubstrait());
    }

    public void testLongTermListOnAWideFanOutAnswersWithTheSqlEncoding() throws IOException {
        StringBuilder terms = new StringBuilder("{\"size\":0,\"query\":{\"terms\":{\"category\":[");
        for (int i = 0; i < 500; i++) {
            terms.append(i == 0 ? "" : ",").append("\"category-value-").append(i).append('"');
        }
        String body = terms.append("]}}}").toString();
        PushedFilter single = pushedFilter(body, nodes(1));
        assertTrue("one node: the Substrait bytes' excess is under the tie break", single.usesSubstrait());
        PushedFilter wide = pushedFilter(body, nodes(50));
        assertFalse("fifty nodes: the shorter SQL ships", wide.usesSubstrait());
        assertTrue(wide.sql().startsWith("category IN ("));
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.cost.CostCoefficients;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.cost.StorageKind;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFilter;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * Which encoding of a pushed filter the Volcano planner keeps: the two
 * pushdown rules register the SQL and the Substrait form of one
 * predicate in the same equivalence set and the scan's cost orders
 * them. The Substrait form is the default and answers a short predicate
 * on any fan out; the SQL form answers once the Substrait bytes times
 * the data node count pass 50 KB, the point where the wire term of the
 * Substrait form ({@code FILTER_WIRE_MS_PER_KB_PER_NODE}) exceeds the
 * tie break charged to the SQL form ({@code FILTER_SQL_TIE_BREAK_MS}).
 */
public class FilterEncodingChoiceTests extends OpenSearchTestCase {

    /** Substrait bytes times data nodes above which the SQL encoding wins: 0.05 ms over 0.001 ms per KB per node. */
    private static final long TIE_BREAK_BYTES_TIMES_NODES = Math.round(
        CostCoefficients.FILTER_SQL_TIE_BREAK_MS / CostCoefficients.FILTER_WIRE_MS_PER_KB_PER_NODE * 1024
    );

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
        assertTrue("one node: the Substrait bytes are under the tie break", single.usesSubstrait());
        PushedFilter wide = pushedFilter(body, nodes(50));
        assertFalse("fifty nodes: the shorter SQL ships", wide.usesSubstrait());
        assertTrue(wide.sql().startsWith("category IN ("));
    }

    public void testTheSqlEncodingWinsOnceTheSubstraitBytesTimesTheNodesPassTheTieBreak() throws IOException {
        assertEquals(51_200L, TIE_BREAK_BYTES_TIMES_NODES);

        // A 200 value list of short values, the shape the issue measured:
        // a few KB of Substrait, so it ships as Substrait up to a fan out
        // in the tens of nodes, four nodes included.
        StringBuilder twoHundred = new StringBuilder("{\"size\":0,\"query\":{\"terms\":{\"category\":[");
        for (int i = 0; i < 200; i++) {
            twoHundred.append(i == 0 ? "" : ",").append("\"cat").append(String.format(Locale.ROOT, "%03d", i)).append('"');
        }
        String twoHundredBody = twoHundred.append("]}}}").toString();
        PushedFilter twoHundredSingle = pushedFilter(twoHundredBody, nodes(1));
        assertTrue(twoHundredSingle.usesSubstrait());
        int twoHundredBytes = twoHundredSingle.substraitLength();
        assertTrue("2 to 4 KB of Substrait for 200 short values: " + twoHundredBytes, twoHundredBytes > 2_000 && twoHundredBytes < 4_096);
        assertTrue("four nodes stay under the tie break", pushedFilter(twoHundredBody, nodes(4)).usesSubstrait());

        // The boundary itself: the largest fan out whose product stays
        // under 51,200 bytes keeps Substrait, the smallest fan out whose
        // product passes it takes SQL.
        int under = (int) ((TIE_BREAK_BYTES_TIMES_NODES - 1) / twoHundredBytes);
        int over = (int) (TIE_BREAK_BYTES_TIMES_NODES / twoHundredBytes) + 1;
        assertTrue("the boundary lies at a fan out above one node: " + under, under >= 1);
        assertTrue(
            under + " nodes times " + twoHundredBytes + " bytes stays Substrait",
            pushedFilter(twoHundredBody, nodes(under)).usesSubstrait()
        );
        assertFalse(
            over + " nodes times " + twoHundredBytes + " bytes ships SQL",
            pushedFilter(twoHundredBody, nodes(over)).usesSubstrait()
        );

        // On one node the same rule needs the Substrait message itself
        // over 50 KB: 320 values of 200 characters pass it, 200 short
        // values do not.
        StringBuilder wide = new StringBuilder("{\"size\":0,\"query\":{\"terms\":{\"category\":[");
        String padding = "x".repeat(200);
        for (int i = 0; i < 320; i++) {
            wide.append(i == 0 ? "" : ",").append("\"v").append(i).append('-').append(padding).append('"');
        }
        PushedFilter wideSingle = pushedFilter(wide.append("]}}}").toString(), nodes(1));
        assertFalse("over 50 KB of Substrait on one node ships as SQL", wideSingle.usesSubstrait());
        assertTrue(wideSingle.sql().startsWith("category IN ("));
    }
}

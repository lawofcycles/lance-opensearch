/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Pins the structural contract of the {@link LanceAggregate} the
 * translator builds, beyond the plan text the fixtures compare.
 */
public class LanceAggregateNodeTests extends OpenSearchTestCase {

    private static LanceAggregate translate(String json) throws IOException {
        return (LanceAggregate) PlanTestFixtures.translate(PlanTestFixtures.parse(json));
    }

    private static List<RexInputRef> inputRefsOf(LanceAggregate aggregate) {
        List<RexInputRef> refs = new ArrayList<>();
        RexVisitorImpl<Void> collector = new RexVisitorImpl<>(true) {
            @Override
            public Void visitInputRef(RexInputRef ref) {
                refs.add(ref);
                return null;
            }
        };
        for (List<RexNode> perKey : aggregate.filterPredicates()) {
            for (RexNode predicate : perKey) {
                predicate.accept(collector);
            }
        }
        return refs;
    }

    public void testFilterPredicateReferencesResolveAgainstTheAggregateInput() throws IOException {
        LanceAggregate aggregate = translate("{\"size\":0,\"aggs\":{\"f\":{\"filter\":{\"term\":{\"category\":\"a\"}}}}}");
        RelDataType inputType = aggregate.getInput().getRowType();
        List<RexInputRef> refs = inputRefsOf(aggregate);
        assertFalse("the term predicate references its column", refs.isEmpty());
        for (RexInputRef ref : refs) {
            assertTrue("reference " + ref + " is a column of the input " + inputType, ref.getIndex() < inputType.getFieldCount());
            assertEquals("category", inputType.getFieldList().get(ref.getIndex()).getName());
        }
    }

    public void testFiltersPredicateReferencesResolveAgainstTheAggregateInput() throws IOException {
        LanceAggregate aggregate = translate(
            "{\"size\":0,\"aggs\":{\"fs\":{\"filters\":{\"filters\":{"
                + "\"low\":{\"range\":{\"rating\":{\"lt\":100}}},"
                + "\"named\":{\"term\":{\"category\":\"a\"}}}}}}}"
        );
        RelDataType inputType = aggregate.getInput().getRowType();
        List<RexInputRef> refs = inputRefsOf(aggregate);
        assertFalse(refs.isEmpty());
        for (RexInputRef ref : refs) {
            assertTrue("reference " + ref + " is a column of the input " + inputType, ref.getIndex() < inputType.getFieldCount());
            String name = inputType.getFieldList().get(ref.getIndex()).getName();
            assertTrue(
                "reference " + ref + " resolves to a requested column, not [" + name + "]",
                name.equals("rating") || name.equals("category")
            );
        }
    }
}

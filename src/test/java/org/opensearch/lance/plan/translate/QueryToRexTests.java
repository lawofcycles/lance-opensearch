/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The edges of {@link QueryToRex} the JSON fixtures cannot carry: the
 * terms value cap, {@code ids} against a missing or unparseable
 * primary key, and the regexp operators only Lucene's grammar has.
 */
public class QueryToRexTests extends OpenSearchTestCase {

    private static String messageOf(QueryBuilder query, LanceSchemas.IndexModel model) {
        RelBuilder relBuilder = PlanTestFixtures.factory().relBuilder(model.schema()).transform(config -> config.withSimplify(false));
        relBuilder.scan(LancePlannerFactory.SCHEMA_NAME, model.indexName());
        UnsupportedOperationException refusal = expectThrows(
            UnsupportedOperationException.class,
            () -> QueryToRex.translate(query, model, relBuilder)
        );
        return refusal.getMessage();
    }

    private static RexNode predicateOf(QueryBuilder query, LanceSchemas.IndexModel model) {
        RelBuilder relBuilder = PlanTestFixtures.factory().relBuilder(model.schema()).transform(config -> config.withSimplify(false));
        relBuilder.scan(LancePlannerFactory.SCHEMA_NAME, model.indexName());
        return QueryToRex.translate(query, model, relBuilder);
    }

    public void testTermsAboveTheCapThrows() {
        List<Object> values = new ArrayList<>(QueryToRex.MAX_TERMS_VALUES + 1);
        for (int i = 0; i <= QueryToRex.MAX_TERMS_VALUES; i++) {
            values.add(i);
        }
        assertEquals(
            "terms on field [rating] with [" + (QueryToRex.MAX_TERMS_VALUES + 1) + "] values (max " + QueryToRex.MAX_TERMS_VALUES + ")",
            messageOf(new TermsQueryBuilder("rating", values), PlanTestFixtures.queryModel())
        );
    }

    public void testIdsWithoutPrimaryKeyThrows() {
        LanceSchemas.IndexModel noPk = LanceSchemas.model("idx", PlanTestFixtures.SCHEMA, Map.of(), () -> 512L);
        assertEquals("ids query without a primary key column", messageOf(new IdsQueryBuilder().addIds("1"), noPk));
    }

    public void testIdsWithNoValuesIsFalse() {
        assertEquals("false", predicateOf(new IdsQueryBuilder(), PlanTestFixtures.queryModel()).toString());
    }

    public void testIdsUnparseableOnIntegerPrimaryKeyThrows() {
        assertEquals("value [abc] on column [id]", messageOf(new IdsQueryBuilder().addIds("abc"), PlanTestFixtures.queryModel()));
    }

    public void testRegexpLuceneOnlyOperatorThrows() {
        String message = messageOf(new RegexpQueryBuilder("category", "a&b"), PlanTestFixtures.queryModel());
        assertTrue(message, message.startsWith("regexp on field [category] uses the Lucene only operator & (intersection)"));
    }
}

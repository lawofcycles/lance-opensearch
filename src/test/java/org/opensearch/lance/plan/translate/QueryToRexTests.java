/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The edges of {@link QueryToRex} the JSON fixtures cannot carry: the
 * terms value cap, {@code ids} against a missing or unparseable
 * primary key, the regexp operators only Lucene's grammar has, the
 * score shaping compounds on a scored request, and the fields the
 * compounds report for the override column pre-flight.
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

    public void testIdsOnANumericPrimaryKeySortByTheCoercedValue() {
        // 2 sorts before 10 on the integer key; a lexicographic sort
        // would put "10" first.
        assertEquals(
            "OR(=(CAST($0):BIGINT NOT NULL, 2), =(CAST($0):BIGINT NOT NULL, 10))",
            predicateOf(new IdsQueryBuilder().addIds("10", "2"), PlanTestFixtures.queryModel()).toString()
        );
    }

    public void testIdsUnparseableOnIntegerPrimaryKeyThrows() {
        assertEquals("value [abc] on column [id]", messageOf(new IdsQueryBuilder().addIds("abc"), PlanTestFixtures.queryModel()));
    }

    public void testRegexpLuceneOnlyOperatorThrows() {
        String message = messageOf(new RegexpQueryBuilder("category", "a&b"), PlanTestFixtures.queryModel());
        assertTrue(message, message.startsWith("regexp on field [category] uses the Lucene only operator & (intersection)"));
    }

    /**
     * The score shaping compounds reduce to the rows they match only
     * when nothing reads the score; on a scored request each names
     * itself, so the Lucene composition keeps the score order the query
     * asks for.
     */
    public void testScoreShapingCompoundsRefuseOnAScoredRequest() {
        LanceSchemas.IndexModel model = PlanTestFixtures.queryModel();
        TermQueryBuilder term = new TermQueryBuilder("category", "c0");
        assertEquals("query type [constant_score] on a scored request", scoredMessageOf(new ConstantScoreQueryBuilder(term), model));
        assertEquals("query type [dis_max] on a scored request", scoredMessageOf(new DisMaxQueryBuilder().add(term), model));
        assertEquals(
            "query type [boosting] on a scored request",
            scoredMessageOf(new BoostingQueryBuilder(term, new TermQueryBuilder("flag", true)).negativeBoost(0.2f), model)
        );
        assertEquals("query type [function_score] on a scored request", scoredMessageOf(new FunctionScoreQueryBuilder(term), model));
        // A bool whose optional clauses would score its matches apart is
        // refused too; one that scores every match alike translates.
        TermQueryBuilder flag = new TermQueryBuilder("flag", true);
        assertEquals(
            "bool with optional should clauses on a scored request",
            scoredMessageOf(new BoolQueryBuilder().should(term).should(flag), model)
        );
        assertEquals(
            "bool with optional should clauses on a scored request",
            scoredMessageOf(new BoolQueryBuilder().filter(term).should(flag), model)
        );
        assertEquals(
            "bool with optional should clauses on a scored request",
            scoredMessageOf(new BoolQueryBuilder().must(new BoolQueryBuilder().should(term).should(flag)), model)
        );
        assertEquals("=($4, 'c0')", scoredPredicateOf(new BoolQueryBuilder().should(term), model).toString());
        assertEquals("AND(=($4, 'c0'), =($6, true))", scoredPredicateOf(new BoolQueryBuilder().must(term).must(flag), model).toString());
        assertEquals(
            "AND(=($4, 'c0'), NOT(IS TRUE(=($6, true))))",
            scoredPredicateOf(new BoolQueryBuilder().should(term).mustNot(flag), model).toString()
        );
        // A leaf query does not read scores and translates either way.
        assertEquals("=($4, 'c0')", scoredPredicateOf(term, model).toString());
    }

    public void testEmptyDisMaxIsFalse() {
        assertEquals("false", predicateOf(new DisMaxQueryBuilder(), PlanTestFixtures.queryModel()).toString());
    }

    /**
     * The compounds descend for the field pre-flight the coordinator
     * runs against the {@code ip} and {@code geo_point} override
     * columns, so a predicate on such a column inside one of them keeps
     * the query on the Lucene side.
     */
    public void testReferencedFieldsDescendIntoTheCompounds() {
        TermQueryBuilder term = new TermQueryBuilder("category", "c0");
        assertEquals(Set.of("category"), QueryToRex.referencedFields(new ConstantScoreQueryBuilder(term)));
        assertEquals(
            Set.of("category", "rating"),
            QueryToRex.referencedFields(new DisMaxQueryBuilder().add(term).add(new RangeQueryBuilder("rating").gte(1)))
        );
        assertEquals(
            Set.of("category", "flag"),
            QueryToRex.referencedFields(new BoostingQueryBuilder(term, new TermQueryBuilder("flag", true)).negativeBoost(0.2f))
        );
        assertEquals(
            Set.of("category", "flag", "rating"),
            QueryToRex.referencedFields(
                new FunctionScoreQueryBuilder(
                    term,
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                            new TermQueryBuilder("flag", true),
                            new FieldValueFactorFunctionBuilder("rating")
                        ) }
                )
            )
        );
    }

    private static String scoredMessageOf(QueryBuilder query, LanceSchemas.IndexModel model) {
        RelBuilder relBuilder = PlanTestFixtures.factory().relBuilder(model.schema()).transform(config -> config.withSimplify(false));
        relBuilder.scan(LancePlannerFactory.SCHEMA_NAME, model.indexName());
        UnsupportedOperationException refusal = expectThrows(
            UnsupportedOperationException.class,
            () -> QueryToRex.translate(query, model, relBuilder, QueryToRex.Scores.USED)
        );
        return refusal.getMessage();
    }

    private static RexNode scoredPredicateOf(QueryBuilder query, LanceSchemas.IndexModel model) {
        RelBuilder relBuilder = PlanTestFixtures.factory().relBuilder(model.schema()).transform(config -> config.withSimplify(false));
        relBuilder.scan(LancePlannerFactory.SCHEMA_NAME, model.indexName());
        return QueryToRex.translate(query, model, relBuilder, QueryToRex.Scores.USED);
    }
}

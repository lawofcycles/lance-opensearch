/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.mapper.LanceTextFieldMapper;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceScanFilterQuery;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Shape check for collapsing {@code bool { must: [FTS], filter, must_not }}
 * into a single Lance FTS scan with a SQL prefilter
 * ({@link TransportLanceFragmentQueryAction#resolveFtsPrefilterShape}).
 */
public class FtsPrefilterPushdownTests extends OpenSearchTestCase {

    private static final Function<String, String> MAPPING = Map.of("body", "lance_text", "id", "integer", "rating", "integer")::get;

    private static BoolQueryBuilder ftsWithFilters() {
        return QueryBuilders.boolQuery()
            .must(new LanceMatchQueryBuilder("body", "hello"))
            .filter(QueryBuilders.termQuery("rating", 5))
            .filter(QueryBuilders.rangeQuery("id").gte(4).lt(8))
            .mustNot(QueryBuilders.termQuery("id", 6));
    }

    public void testTargetShapeProducesPrefilterForFilterAndMustNot() {
        BoolQueryBuilder bool = ftsWithFilters();

        TransportLanceFragmentQueryAction.FtsPrefilterShape shape = TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(
            bool,
            MAPPING
        );

        assertNotNull("bool { must: [lance_match], filter, must_not } must collapse", shape);
        assertSame("the FTS clause is handed through untouched", bool.must().get(0), shape.ftsClause());
        assertEquals("(rating = 5 AND (id >= 4 AND id < 8) AND NOT (id = 6))", shape.prefilterSql());
    }

    public void testFtsClauseBoostDoesNotBlockPushdown() {
        // The clause boost stays with the clause (AbstractQueryBuilder
        // wraps it in a BoostQuery); only a boost on the bool itself
        // disables the collapse.
        BoolQueryBuilder bool = QueryBuilders.boolQuery()
            .must(new LanceMatchQueryBuilder("body", "hello").boost(2.0f))
            .filter(QueryBuilders.termQuery("rating", 5));
        assertNotNull(TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(bool, MAPPING));
    }

    public void testShouldClauseBlocksPushdown() {
        BoolQueryBuilder bool = ftsWithFilters().should(QueryBuilders.termQuery("rating", 4));
        assertNull(TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(bool, MAPPING));
    }

    public void testTwoMustClausesBlockPushdown() {
        BoolQueryBuilder bool = ftsWithFilters().must(new LanceMatchQueryBuilder("body", "lance"));
        assertNull(TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(bool, MAPPING));
    }

    public void testUntranslatableFilterClauseBlocksPushdown() {
        // A stock match query has no Lance SQL form, so the translator
        // refuses and the whole bool stays on the Lucene tree.
        BoolQueryBuilder bool = ftsWithFilters().filter(new MatchQueryBuilder("body", "lance"));
        assertNull(TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(bool, MAPPING));
    }

    public void testUnmappedFieldInFilterBlocksPushdown() {
        BoolQueryBuilder bool = QueryBuilders.boolQuery()
            .must(new LanceMatchQueryBuilder("body", "hello"))
            .filter(QueryBuilders.termQuery("nosuchfield", 5));
        assertNull(TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(bool, MAPPING));
    }

    public void testBoolBoostBlocksPushdown() {
        BoolQueryBuilder bool = ftsWithFilters().boost(1.5f);
        assertNull(TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(bool, MAPPING));
    }

    public void testScanLimitAppliesToBareClauseAndNotToBoostedClause() throws IOException {
        // The FTS clause is turned into its Lucene query through the
        // real LanceMatchQueryBuilder against a lance_text mapping;
        // the QueryShardContext only has to answer fieldMapper("body").
        QueryShardContext qsc = mock(QueryShardContext.class);
        MappedFieldType bodyType = new LanceTextFieldMapper.Builder("body").build(
            new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0))
        ).fieldType();
        when(qsc.fieldMapper("body")).thenReturn(bodyType);
        String sql = "(rating = 5 AND (id >= 4 AND id < 8) AND NOT (id = 6))";

        // Bare clause: the prefilter and the request's top-k both land
        // on the LanceFtsQuery, and the unbounded sentinel passes through.
        TransportLanceFragmentQueryAction.FtsPrefilterShape bare = TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(
            ftsWithFilters(),
            MAPPING
        );
        Query limited = bare.toQuery(qsc, 10);
        assertTrue("bare clause stays a LanceFtsQuery, saw " + limited, limited instanceof LanceFtsQuery);
        assertEquals(10, ((LanceFtsQuery) limited).scanLimit());
        assertEquals(sql, ((LanceFtsQuery) limited).prefilterSql());
        Query unbounded = bare.toQuery(qsc, LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED);
        assertEquals(LanceFtsQuery.SCAN_LIMIT_UNBOUNDED, ((LanceFtsQuery) unbounded).scanLimit());
        assertEquals(sql, ((LanceFtsQuery) unbounded).prefilterSql());

        // Boosted clause: the BoostQuery wrapper is kept, the prefilter
        // moves inside it, and the scan stays unbounded like the
        // top-level boosted FTS path.
        BoolQueryBuilder boostedBool = QueryBuilders.boolQuery()
            .must(new LanceMatchQueryBuilder("body", "hello").boost(2.0f))
            .filter(QueryBuilders.termQuery("rating", 5))
            .filter(QueryBuilders.rangeQuery("id").gte(4).lt(8))
            .mustNot(QueryBuilders.termQuery("id", 6));
        Query boosted = TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(boostedBool, MAPPING).toQuery(qsc, 10);
        assertTrue("boosted clause keeps its BoostQuery, saw " + boosted, boosted instanceof BoostQuery);
        assertEquals(2.0f, ((BoostQuery) boosted).getBoost(), 0f);
        Query inner = ((BoostQuery) boosted).getQuery();
        assertTrue("BoostQuery wraps the LanceFtsQuery, saw " + inner, inner instanceof LanceFtsQuery);
        assertEquals(LanceFtsQuery.SCAN_LIMIT_UNBOUNDED, ((LanceFtsQuery) inner).scanLimit());
        assertEquals(sql, ((LanceFtsQuery) inner).prefilterSql());
    }

    public void testShapesWithNothingToPushStayOnLucene() {
        // No scalar clause: nothing to prefilter, the plain FTS path
        // (with its own top-k handling) keeps the query.
        BoolQueryBuilder onlyMust = QueryBuilders.boolQuery().must(new LanceMatchQueryBuilder("body", "hello"));
        assertNull(TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(onlyMust, MAPPING));
        // minimum_should_match set: left to Lucene even without should clauses.
        BoolQueryBuilder msm = ftsWithFilters().minimumShouldMatch(1);
        assertNull(TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(msm, MAPPING));
        // A non-FTS must clause (stock match) is not a Lance FTS scan.
        BoolQueryBuilder stockMatch = QueryBuilders.boolQuery()
            .must(new MatchQueryBuilder("body", "hello"))
            .filter(QueryBuilders.termQuery("rating", 5));
        assertNull(TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(stockMatch, MAPPING));
        // Not a bool at all.
        assertNull(TransportLanceFragmentQueryAction.resolveFtsPrefilterShape(new LanceMatchQueryBuilder("body", "hello"), MAPPING));
    }
}

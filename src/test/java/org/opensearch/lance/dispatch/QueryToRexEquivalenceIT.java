/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.lance.Dataset;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.lancesql.RexToLanceSql;
import org.opensearch.lance.plan.translate.QueryToRex;
import org.opensearch.lance.query.LanceKnnFilterTranslator;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * For every query shape the planner pushes, runs the request twice
 * against a real Lance table: once through the current execution path
 * ({@code _search}, whose count travels as the SQL the string
 * translator builds, or the Lucene fallback where it refuses), and once
 * by planning the request, reading the SQL out of the pushed filter and
 * executing {@code Dataset.countRows(sql)}. The two counts must be
 * equal. Where the old translator supports the shape and spells it the
 * same way, the pushed SQL must also equal
 * {@link LanceKnnFilterTranslator#toLanceSql} byte for byte; the shapes whose
 * spelling legitimately changed (date bounds as
 * {@code to_timestamp_millis}, {@code must_not} as
 * {@code NOT (... IS TRUE)}, an {@code OR} of equal terms as
 * {@code IN}) are asserted on the count only.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class QueryToRexEquivalenceIT extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
    }

    // ---------------------------------------------------------------
    // Hint fixture: rating int32 (nulls), category utf8 (nulls),
    // flag bool (nulls), body lance_text with a raw keyword sub-field
    // ---------------------------------------------------------------

    public void testScalarShapesOverTheHintFixture() throws Exception {
        String index = "qrex-eq-hint";
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(dir, index, 3, 200);
        LinkedHashMap<String, String> bodySubs = new LinkedHashMap<>();
        bodySubs.put("raw", "keyword");
        attach(index, uri, Map.of("body", bodySubs));

        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceSchemas.IndexModel model = modelOf(index, dataset);
            Function<String, String> lookup = TransportLanceCoordinatorAction.buildFieldTypeLookup(metadataOf(index));

            checkPinned(index, dataset, model, lookup, QueryBuilders.termQuery("category", "c0"));
            checkPinned(index, dataset, model, lookup, QueryBuilders.termQuery("rating", 37));
            checkPinned(index, dataset, model, lookup, QueryBuilders.termQuery("flag", true));
            checkPinned(index, dataset, model, lookup, new TermsQueryBuilder("category", List.of("c0", "c2")));
            checkPinned(index, dataset, model, lookup, new TermsQueryBuilder("rating", List.of(37, 74, 111)));
            checkPinned(index, dataset, model, lookup, QueryBuilders.existsQuery("category"));
            checkPinned(index, dataset, model, lookup, QueryBuilders.rangeQuery("rating").gte(100).lt(900));
            checkPinned(index, dataset, model, lookup, QueryBuilders.rangeQuery("rating").gt(500));
            checkPinned(
                index,
                dataset,
                model,
                lookup,
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("category", "c0")).must(QueryBuilders.termQuery("flag", true))
            );
            // The old translator spells the filter clauses before the
            // must clauses and keeps a nested range parenthesised where
            // the printer flattens; the count is what must agree.
            checkCount(
                index,
                dataset,
                model,
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("category", "c0"))
                    .filter(QueryBuilders.rangeQuery("rating").gte(100))
            );
            checkPinned(index, dataset, model, lookup, QueryBuilders.wildcardQuery("category", "c*"));
            checkPinned(index, dataset, model, lookup, QueryBuilders.wildcardQuery("category", "C?").caseInsensitive(true));
            checkPinned(index, dataset, model, lookup, QueryBuilders.regexpQuery("category", "c[02]"));
            checkPinned(index, dataset, model, lookup, QueryBuilders.regexpQuery("category", "C.").caseInsensitive(true));
            checkPinned(index, dataset, model, lookup, QueryBuilders.prefixQuery("category", "c"));
            checkPinned(index, dataset, model, lookup, QueryBuilders.prefixQuery("category", "C").caseInsensitive(true));

            // must_not spells NOT (... IS TRUE) now; the count agrees on
            // a column without nulls (id), where the two null semantics
            // coincide.
            checkCount(index, dataset, model, QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("id", 5)));
            // A pure should of terms on one column collapses to IN.
            checkCount(
                index,
                dataset,
                model,
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("category", "c0"))
                    .should(QueryBuilders.termQuery("category", "c1"))
            );
            // The old translator refuses multi-field sub-fields; the
            // planner resolves them to the base column.
            checkCount(index, dataset, model, QueryBuilders.termQuery("body.raw", "hello tok0 grp0 sp0 lance"));
            checkCount(index, dataset, model, new MatchNoneQueryBuilder());

            // match_all yields the true literal; the count agrees.
            assertEquals("true", pushedSql(model, QueryBuilders.matchAllQuery()));
            assertEquals(currentHits(index, QueryBuilders.matchAllQuery()), dataset.countRows());

            // An unmapped field refuses at translation; the current path
            // answers the request through the Lucene fallback.
            RelBuilder builder = relBuilder(model);
            UnsupportedOperationException refusal = expectThrows(
                UnsupportedOperationException.class,
                () -> QueryToRex.translate(QueryBuilders.termQuery("missing", 1), model, builder)
            );
            assertEquals("field [missing] does not map to a Lance column", refusal.getMessage());
            assertEquals(0L, currentHits(index, QueryBuilders.termQuery("missing", 1)));
        }
    }

    // ---------------------------------------------------------------
    // Dated fixture: ts timestamp[us]
    // ---------------------------------------------------------------

    public void testDateShapesOverTheDatedFixture() throws Exception {
        String index = "qrex-eq-dated";
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeDatedTable(dir, index);
        attach(index, uri, null);

        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceSchemas.IndexModel model = modelOf(index, dataset);

            // The date bounds print as to_timestamp_millis instead of
            // the old timestamp '...' literal; counts must agree.
            checkCount(index, dataset, model, QueryBuilders.termQuery("ts", "2024-01-15T00:00:00Z"));
            checkCount(index, dataset, model, QueryBuilders.rangeQuery("ts").gte("2024-02-01").lt("2024-04-01"));
            checkCount(index, dataset, model, QueryBuilders.rangeQuery("ts").gt("2024-03-10T00:00:00Z"));
            checkCount(index, dataset, model, new TermsQueryBuilder("category", List.of("even", "odd")));
        }
    }

    // ---------------------------------------------------------------
    // Struct fixture: meta.region utf8, meta.score float64,
    // meta.flags.active bool, id int32 primary key
    // ---------------------------------------------------------------

    public void testStructAndIdsShapesOverTheStructFixture() throws Exception {
        String index = "qrex-eq-struct";
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeStructTable(dir, index, 0);
        attach(index, uri, null);

        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceSchemas.IndexModel model = modelOf(index, dataset);
            Function<String, String> lookup = TransportLanceCoordinatorAction.buildFieldTypeLookup(metadataOf(index));

            checkPinned(index, dataset, model, lookup, QueryBuilders.termQuery("meta.region", "east"));
            checkPinned(index, dataset, model, lookup, QueryBuilders.rangeQuery("meta.score").gte(1.5));
            checkPinned(index, dataset, model, lookup, QueryBuilders.termQuery("meta.flags.active", true));
            checkPinned(index, dataset, model, lookup, QueryBuilders.existsQuery("meta.score"));

            // ids resolves the primary key column, the semantics GET by
            // id has. The current Lucene path finds no hits for an ids
            // query on a Lance index (no indexed _id terms), so the
            // count pins against the fixture rows instead.
            String idsSql = pushedSql(model, new IdsQueryBuilder().addIds("1", "3"));
            assertEquals("id IN (1, 3)", idsSql);
            assertEquals(2L, dataset.countRows(idsSql));
        }
    }

    // ---------------------------------------------------------------
    // The two comparisons
    // ---------------------------------------------------------------

    /**
     * Count equivalence plus the byte for byte SQL pin against the old
     * translator. The one spelling difference tolerated here is the old
     * path's outer parentheses around a whole range predicate, which
     * the printer only emits around a conjunction; both sides are
     * compared with one outer pair removed.
     */
    private void checkPinned(
        String index,
        Dataset dataset,
        LanceSchemas.IndexModel model,
        Function<String, String> lookup,
        QueryBuilder query
    ) {
        String sql = pushedSql(model, query);
        assertEquals("SQL of " + query, unwrapped(LanceKnnFilterTranslator.toLanceSql(query, lookup)), unwrapped(sql));
        assertEquals("count of " + query, currentHits(index, query), dataset.countRows(sql));
    }

    private static String unwrapped(String sql) {
        return sql.startsWith("(") && sql.endsWith(")") ? sql.substring(1, sql.length() - 1) : sql;
    }

    /** Count equivalence only, for the shapes whose SQL spelling legitimately changed or that the old translator refuses. */
    private void checkCount(String index, Dataset dataset, LanceSchemas.IndexModel model, QueryBuilder query) {
        String sql = pushedSql(model, query);
        assertEquals("count of " + query + " via [" + sql + "]", currentHits(index, query), dataset.countRows(sql));
    }

    /**
     * The SQL {@link QueryToRex} + {@link RexToLanceSql} produce for
     * {@code query}, on the plan the request would build. This is what
     * the filter pushdown would put onto the scan as its pushed SQL
     * when the aggregate rule does not first absorb the filter into a
     * pushed aggregate; the count equivalence check uses it to compare
     * against the current path.
     */
    private String pushedSql(LanceSchemas.IndexModel model, QueryBuilder query) {
        RelBuilder builder = relBuilder(model);
        RexNode predicate = QueryToRex.translate(query, model, builder);
        return RexToLanceSql.print(predicate, builder.build().getRowType())
            .orElseThrow(() -> new AssertionError("the printer must accept " + query));
    }

    private RelBuilder relBuilder(LanceSchemas.IndexModel model) {
        RelBuilder builder = factory().relBuilder(model.schema()).transform(config -> config.withSimplify(false));
        builder.scan(LancePlannerFactory.SCHEMA_NAME, model.indexName());
        return builder;
    }

    private static LancePlannerFactory factory() {
        return new LancePlannerFactory(1L << 30, 1L << 30);
    }

    private long currentHits(String index, QueryBuilder query) {
        SearchResponse response = client().search(new SearchRequest(index).source(new SearchSourceBuilder().size(0).query(query)))
            .actionGet();
        return response.getHits().getTotalHits().value();
    }

    // ---------------------------------------------------------------
    // Fixture plumbing
    // ---------------------------------------------------------------

    private void attach(String index, String uri, Map<String, LinkedHashMap<String, String>> multiFields) {
        LanceAttachRequest request = new LanceAttachRequest(
            uri,
            index,
            null,
            null,
            StorageOptions.empty(),
            LanceOverrides.fromSubFields(multiFields)
        );
        client().execute(LanceAttachAction.INSTANCE, request).actionGet();
        ensureGreen(index);
    }

    private IndexMetadata metadataOf(String index) {
        return getInstanceFromNode(ClusterService.class).state().metadata().index(index);
    }

    private LanceSchemas.IndexModel modelOf(String index, Dataset dataset) {
        IndexMetadata metadata = metadataOf(index);
        String primaryKey = metadata.getSettings().get(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, "");
        Map<String, LinkedHashMap<String, String>> multiFields = LanceOverrides.of(metadata.getSettings()).subFields();
        long rows = dataset.countRows();
        return LanceSchemas.model(index, dataset.getSchema(), multiFields, primaryKey, () -> rows);
    }
}

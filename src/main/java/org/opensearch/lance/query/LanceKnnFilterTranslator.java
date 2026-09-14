/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.Locale;
import java.util.stream.Collectors;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

/**
 * Translates a subset of OpenSearch {@link QueryBuilder}s to a Lance SQL
 * expression that {@link org.lance.ipc.ScanOptions.Builder#filter(String)}
 * accepts. The translation is used by {@code lance_knn} to push a pre-filter
 * into the Lance scanner: without it, {@code lance_knn} returns the top-K
 * rows first and the OpenSearch layer filters afterwards, which can leave
 * fewer than K matching results.
 *
 * <p>Supported clauses are the ones that are safe to lower into Lance's
 * DataFusion SQL parser:
 * <ul>
 *   <li>{@link RangeQueryBuilder} — numeric ranges only; date / string
 *       ranges reject with 400 because their Lance representation differs
 *       from what OpenSearch feeds in.</li>
 *   <li>{@link TermQueryBuilder} / {@link TermsQueryBuilder} — numeric,
 *       string, or boolean literals.</li>
 *   <li>{@link ExistsQueryBuilder}.</li>
 *   <li>{@link BoolQueryBuilder} with {@code filter} / {@code must} /
 *       {@code must_not} / {@code should}.</li>
 *   <li>{@link MatchAllQueryBuilder} (translated to {@code true}).</li>
 * </ul>
 *
 * <p>Any other builder — {@code match}, geo queries, script queries,
 * nested / has_parent, etc. — throws {@link IllegalArgumentException} so
 * the REST layer returns 400. Adding a clause here later is preferable to
 * silently degrading to the post-filter path.
 */
final class LanceKnnFilterTranslator {

    private LanceKnnFilterTranslator() {}

    /**
     * Convert {@code builder} to a Lance SQL expression. Never returns null.
     *
     * @throws IllegalArgumentException when the builder or one of its
     *     sub-builders is not supported.
     */
    static String toLanceSql(QueryBuilder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("[lance_knn] filter must not be null");
        }
        if (builder instanceof MatchAllQueryBuilder) {
            return "true";
        }
        if (builder instanceof TermQueryBuilder t) {
            return t.fieldName() + " = " + literal(t.value(), t.fieldName());
        }
        if (builder instanceof TermsQueryBuilder t) {
            java.util.List<?> values = t.values();
            if (values == null || values.isEmpty()) {
                // `terms {"col": []}` matches nothing; translate to `false`
                // rather than emitting `col IN ()` which the SQL parser
                // rejects.
                return "false";
            }
            String elements = values.stream().map(v -> literal(v, t.fieldName())).collect(Collectors.joining(", "));
            return t.fieldName() + " IN (" + elements + ")";
        }
        if (builder instanceof ExistsQueryBuilder e) {
            return e.fieldName() + " IS NOT NULL";
        }
        if (builder instanceof RangeQueryBuilder r) {
            return translateRange(r);
        }
        if (builder instanceof BoolQueryBuilder b) {
            return translateBool(b);
        }
        throw new IllegalArgumentException(
            "[lance_knn] filter type [" + builder.getClass().getSimpleName() + "] is not supported by the pre-filter translator"
        );
    }

    private static String translateRange(RangeQueryBuilder r) {
        Object from = r.from();
        Object to = r.to();
        if (from == null && to == null) {
            throw new IllegalArgumentException("[lance_knn] range filter on [" + r.fieldName() + "] needs at least one bound");
        }
        StringBuilder sb = new StringBuilder();
        if (from != null) {
            String cmp = r.includeLower() ? " >= " : " > ";
            sb.append(r.fieldName()).append(cmp).append(literal(from, r.fieldName()));
        }
        if (to != null) {
            if (sb.length() > 0) {
                sb.append(" AND ");
            }
            String cmp = r.includeUpper() ? " <= " : " < ";
            sb.append(r.fieldName()).append(cmp).append(literal(to, r.fieldName()));
        }
        return "(" + sb.toString() + ")";
    }

    private static String translateBool(BoolQueryBuilder b) {
        // Must and filter are ANDed together; should is ORed with the AND of
        // must / filter; must_not is negated. The BooleanQuery.rewrite path
        // handles the same combination in Lucene, so this mirrors what the
        // post-filter would already do — just pushed down.
        java.util.List<String> andClauses = new java.util.ArrayList<>();
        for (QueryBuilder q : b.filter()) {
            andClauses.add(toLanceSql(q));
        }
        for (QueryBuilder q : b.must()) {
            andClauses.add(toLanceSql(q));
        }
        for (QueryBuilder q : b.mustNot()) {
            andClauses.add("NOT (" + toLanceSql(q) + ")");
        }
        java.util.List<String> orClauses = new java.util.ArrayList<>();
        for (QueryBuilder q : b.should()) {
            orClauses.add(toLanceSql(q));
        }
        if (andClauses.isEmpty() && orClauses.isEmpty()) {
            return "true";
        }
        String andSql = andClauses.isEmpty() ? null : andClauses.stream().collect(Collectors.joining(" AND "));
        String orSql = orClauses.isEmpty() ? null : "(" + orClauses.stream().collect(Collectors.joining(" OR ")) + ")";
        if (andSql != null && orSql != null) {
            return "(" + andSql + " AND " + orSql + ")";
        }
        return "(" + (andSql != null ? andSql : orSql) + ")";
    }

    private static String literal(Object value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException("[lance_knn] filter value on [" + fieldName + "] must not be null");
        }
        if (value instanceof Boolean) {
            return value.toString().toLowerCase(Locale.ROOT);
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof String s) {
            // Escape single quotes by doubling them, which is the standard
            // SQL literal escape that DataFusion accepts.
            return "'" + s.replace("'", "''") + "'";
        }
        if (value instanceof org.apache.lucene.util.BytesRef b) {
            return "'" + b.utf8ToString().replace("'", "''") + "'";
        }
        throw new IllegalArgumentException(
            "[lance_knn] filter value on ["
                + fieldName
                + "] has unsupported type ["
                + value.getClass().getSimpleName()
                + "]; wrap it as a string, number, or boolean"
        );
    }
}

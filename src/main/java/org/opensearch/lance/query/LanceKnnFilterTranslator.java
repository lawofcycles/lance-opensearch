/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.Locale;
import java.util.function.Function;
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
 * accepts. The translation has two callers today:
 * <ul>
 *   <li>{@code lance_knn} uses it to push a pre-filter into the Lance
 *       scanner. Without it, {@code lance_knn} returns the top-K rows
 *       first and the OpenSearch layer filters afterwards, which can
 *       leave fewer than K matching results.</li>
 *   <li>{@code LanceDispatchActionFilter} uses it in fragment dispatch
 *       mode to translate the request's top-level query into a filter
 *       fed directly to {@link org.lance.Dataset#countRows(String)} and
 *       to the {@link org.lance.ipc.ScanOptions} that populate
 *       {@code hits}. That path bypasses the shard executor entirely
 *       and needs the same subset of query types this translator
 *       already covers.</li>
 * </ul>
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
 * nested / has_parent, etc. — throws {@link IllegalArgumentException}.
 * The {@code lance_knn} caller lets this bubble as a 400 to the REST
 * layer, while the dispatch filter catches it and falls back to the
 * standard shard path so the request still gets an answer.
 */
public final class LanceKnnFilterTranslator {

    private LanceKnnFilterTranslator() {}

    /**
     * Field-type lookup that returns {@code null} for every field. Callers
     * without mapping context (unit tests, tools that only exercise the
     * SQL syntax) can pass this to {@link #toLanceSql(QueryBuilder,
     * Function)} instead of a real mapping resolver. The translator falls
     * back to shape-based heuristics on the literal side (the
     * {@link #ISO_DATE_LIKE} pattern for date ranges) when the lookup
     * returns {@code null} for the queried field, so simple usages still
     * work with the known false-positive risk documented on the pattern.
     */
    public static final Function<String, String> NO_MAPPING = name -> null;

    /**
     * Return {@code true} when any leaf in {@code builder} names a
     * field that {@code fieldTypeLookup} reports as unmapped
     * ({@code null} return). Callers that pass {@link #NO_MAPPING}
     * always get {@code false} here — every field looks unmapped,
     * so the walker treats "no mapping context" as "cannot say" and
     * defers to whatever the translator or the shape heuristic
     * would emit.
     *
     * <p>This is the pre-flight the coordinator uses to skip
     * emitting a Lance SQL filter for a query that references an
     * unmapped field. Without it the translator happily generates
     * {@code unmapped >= 1}, the per-node counter reaches
     * {@code Dataset.countRows(sql)}, and Lance rejects with
     * {@code SchemaError(No field named unmapped)}. Skipping
     * emission drops the coordinator's coarse count path and lets
     * the per-node executor fall back to
     * {@link org.apache.lucene.search.IndexSearcher#count} against
     * the rewritten Lucene {@link org.apache.lucene.search.Query}
     * (which resolves the unmapped range to {@code MatchNoDocsQuery}
     * via {@code RangeQueryBuilder.doRewrite}). See issue #50.
     *
     * <p>Only the {@link QueryBuilder} shapes this translator
     * already supports at
     * {@link #toLanceSql(QueryBuilder, Function)} are inspected;
     * shapes outside the whitelist (match, knn, ...) are ignored
     * here because {@code resolveFilterSql} would already return
     * {@code null} for them via the
     * {@link IllegalArgumentException} catch in the coordinator.
     */
    public static boolean hasUnmappedField(QueryBuilder builder, Function<String, String> fieldTypeLookup) {
        if (fieldTypeLookup == null || fieldTypeLookup == NO_MAPPING) {
            return false;
        }
        if (builder == null) {
            return false;
        }
        if (builder instanceof MatchAllQueryBuilder) {
            return false;
        }
        if (builder instanceof TermQueryBuilder t) {
            return isFieldUnmapped(t.fieldName(), fieldTypeLookup);
        }
        if (builder instanceof TermsQueryBuilder t) {
            return isFieldUnmapped(t.fieldName(), fieldTypeLookup);
        }
        if (builder instanceof ExistsQueryBuilder e) {
            return isFieldUnmapped(e.fieldName(), fieldTypeLookup);
        }
        if (builder instanceof RangeQueryBuilder r) {
            return isFieldUnmapped(r.fieldName(), fieldTypeLookup);
        }
        if (builder instanceof BoolQueryBuilder b) {
            for (QueryBuilder q : b.filter()) {
                if (hasUnmappedField(q, fieldTypeLookup)) {
                    return true;
                }
            }
            for (QueryBuilder q : b.must()) {
                if (hasUnmappedField(q, fieldTypeLookup)) {
                    return true;
                }
            }
            for (QueryBuilder q : b.mustNot()) {
                if (hasUnmappedField(q, fieldTypeLookup)) {
                    return true;
                }
            }
            for (QueryBuilder q : b.should()) {
                if (hasUnmappedField(q, fieldTypeLookup)) {
                    return true;
                }
            }
            return false;
        }
        // Shape outside the translator's whitelist. Return false so
        // the caller does not fall into the "skip SQL translation"
        // branch for something the translator would refuse anyway.
        return false;
    }

    private static boolean isFieldUnmapped(String fieldName, Function<String, String> lookup) {
        // rejectMultiFieldPath will fire inside toLanceSql for
        // dotted names, so no need to guard against them here — the
        // walker is a strict subset of the translator's shape gate.
        if (fieldName == null) {
            return false;
        }
        if (fieldName.indexOf('.') >= 0) {
            // multi_field sub-fields (body.raw) resolve at Lucene
            // scan time even though the mapping only carries the
            // base field. Do not treat them as unmapped; the
            // translator itself will reject them with
            // rejectMultiFieldPath and the coordinator's
            // IllegalArgumentException catch will kick in.
            return false;
        }
        return lookup.apply(fieldName) == null;
    }

    /**
     * Convert {@code builder} to a Lance SQL expression. Never returns null.
     *
     * <p>Equivalent to {@link #toLanceSql(QueryBuilder, Function)}
     * with {@link #NO_MAPPING}. Kept for callers that do not have a
     * field-type resolver at hand; the numeric-epoch-millis branch on
     * date columns and the ISO-8601 shape branch on non-date columns
     * both need a lookup to be handled cleanly and will fall back to
     * pre-mapping-aware behaviour here.
     *
     * @throws IllegalArgumentException when the builder or one of its
     *     sub-builders is not supported.
     */
    public static String toLanceSql(QueryBuilder builder) {
        return toLanceSql(builder, NO_MAPPING);
    }

    /**
     * Convert {@code builder} to a Lance SQL expression with a mapping
     * resolver in hand. Never returns null.
     *
     * <p>{@code fieldTypeLookup} is called with the OpenSearch field name
     * of every literal-carrying leaf and must return the mapping type
     * name (for example {@code "date"}, {@code "long"}, {@code "keyword"})
     * or {@code null} when the field is unmapped or the caller cannot
     * resolve it. The translator uses the type to decide whether a
     * literal on a date column is a numeric epoch-millis
     * (wrapped in {@code to_timestamp_millis(...)}) or an ISO-8601
     * string (wrapped in {@code timestamp '...'}). Without the lookup
     * the translator falls back to shape heuristics on strings only,
     * so a numeric epoch-millis on a date column still reaches
     * DataFusion as a bare Int64 and is rejected — that is the pre-#48
     * behaviour.
     *
     * @throws IllegalArgumentException when the builder or one of its
     *     sub-builders is not supported.
     */
    public static String toLanceSql(QueryBuilder builder, Function<String, String> fieldTypeLookup) {
        if (builder == null) {
            throw new IllegalArgumentException("[lance_knn] filter must not be null");
        }
        Function<String, String> lookup = fieldTypeLookup == null ? NO_MAPPING : fieldTypeLookup;
        if (builder instanceof MatchAllQueryBuilder) {
            return "true";
        }
        if (builder instanceof TermQueryBuilder t) {
            rejectMultiFieldPath(t.fieldName());
            return t.fieldName() + " = " + literal(t.value(), t.fieldName(), lookup);
        }
        if (builder instanceof TermsQueryBuilder t) {
            rejectMultiFieldPath(t.fieldName());
            java.util.List<?> values = t.values();
            if (values == null || values.isEmpty()) {
                // `terms {"col": []}` matches nothing; translate to `false`
                // rather than emitting `col IN ()` which the SQL parser
                // rejects.
                return "false";
            }
            String elements = values.stream().map(v -> literal(v, t.fieldName(), lookup)).collect(Collectors.joining(", "));
            return t.fieldName() + " IN (" + elements + ")";
        }
        if (builder instanceof ExistsQueryBuilder e) {
            rejectMultiFieldPath(e.fieldName());
            return e.fieldName() + " IS NOT NULL";
        }
        if (builder instanceof RangeQueryBuilder r) {
            rejectMultiFieldPath(r.fieldName());
            return translateRange(r, lookup);
        }
        if (builder instanceof BoolQueryBuilder b) {
            return translateBool(b, lookup);
        }
        throw new IllegalArgumentException(
            "[lance_knn] filter type [" + builder.getClass().getSimpleName() + "] is not supported by the pre-filter translator"
        );
    }

    /**
     * Refuse a dotted field name so multi-field sub-fields
     * ({@code body.raw}) do not fall through to the Lance SQL
     * pre-filter path. Sub-fields share the base column's data but
     * exist only as Lucene {@code FieldInfo} entries; Lance does not
     * know about them and would interpret the dot as a struct field
     * access, returning a 500 like "type Utf8 is not Struct, Map, or
     * Null". Sub-field queries belong on the Lucene searcher path
     * (SortedSetDocValues), which the coordinator picks when
     * {@code filterSql} is null.
     *
     * <p>Nested Lance columns (Struct / List&lt;Struct&gt;) are not
     * surfaced yet (issues #4 / #5), so a dot in the mapping today
     * means multi-field unambiguously. Revisit this guard when those
     * shapes land.
     */
    private static void rejectMultiFieldPath(String fieldName) {
        if (fieldName != null && fieldName.indexOf('.') >= 0) {
            throw new IllegalArgumentException(
                "[lance_knn] filter cannot push down to Lance for dotted field ["
                    + fieldName
                    + "]; multi-field sub-fields resolve via Lucene doc values instead"
            );
        }
    }

    private static String translateRange(RangeQueryBuilder r, Function<String, String> fieldTypeLookup) {
        Object from = r.from();
        Object to = r.to();
        if (from == null && to == null) {
            throw new IllegalArgumentException("[lance_knn] range filter on [" + r.fieldName() + "] needs at least one bound");
        }
        StringBuilder sb = new StringBuilder();
        if (from != null) {
            String cmp = r.includeLower() ? " >= " : " > ";
            sb.append(r.fieldName()).append(cmp).append(literal(from, r.fieldName(), fieldTypeLookup));
        }
        if (to != null) {
            if (sb.length() > 0) {
                sb.append(" AND ");
            }
            String cmp = r.includeUpper() ? " <= " : " < ";
            sb.append(r.fieldName()).append(cmp).append(literal(to, r.fieldName(), fieldTypeLookup));
        }
        return "(" + sb.toString() + ")";
    }

    private static String translateBool(BoolQueryBuilder b, Function<String, String> fieldTypeLookup) {
        // Must and filter are ANDed together; should is ORed with the AND of
        // must / filter; must_not is negated. The BooleanQuery.rewrite path
        // handles the same combination in Lucene, so this mirrors what the
        // post-filter would already do — just pushed down.
        java.util.List<String> andClauses = new java.util.ArrayList<>();
        for (QueryBuilder q : b.filter()) {
            andClauses.add(toLanceSql(q, fieldTypeLookup));
        }
        for (QueryBuilder q : b.must()) {
            andClauses.add(toLanceSql(q, fieldTypeLookup));
        }
        for (QueryBuilder q : b.mustNot()) {
            andClauses.add("NOT (" + toLanceSql(q, fieldTypeLookup) + ")");
        }
        java.util.List<String> orClauses = new java.util.ArrayList<>();
        for (QueryBuilder q : b.should()) {
            orClauses.add(toLanceSql(q, fieldTypeLookup));
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

    /**
     * ISO-8601 date and date-time literals the range translator
     * lifts into Lance's SQL {@code timestamp '...'} form.
     *
     * <p>DataFusion (Lance's SQL evaluator) rejects a bare
     * {@code Utf8("2024-03-01")} literal against a
     * {@code Timestamp(Microsecond, None)} column with
     * "could not convert to literal of type 'Timestamp(...)'", so a
     * range query on a mapped {@code date} field returned 400
     * unless the caller opened a raw Lance SQL escape hatch. This
     * pattern lets the translator recognise the shape of a date
     * literal and wrap it in {@code timestamp '...'} instead. The
     * three accepted forms cover what OpenSearch date_math and the
     * REST DSL emit:
     *
     * <ul>
     *   <li>{@code YYYY-MM-DD} — date-only (Lance parses this as
     *       midnight, matching OpenSearch semantics).</li>
     *   <li>{@code YYYY-MM-DDTHH:MM:SS(.frac)?} — date and time,
     *       no zone.</li>
     *   <li>{@code YYYY-MM-DDTHH:MM:SS(.frac)?(Z|+HH:MM|-HH:MM|+HHMM|-HHMM)}
     *       — date and time with UTC offset.</li>
     * </ul>
     *
     * <p>When a {@link #toLanceSql(QueryBuilder, Function)} caller
     * supplies a field-type lookup, the translator prefers the
     * mapping type over the shape: an ISO-8601-shaped string on a
     * non-date column stays a plain SQL string literal (fixing the
     * documented false positive) and a numeric literal on a date
     * column gets wrapped in {@code to_timestamp_millis(...)} so
     * epoch-millis literals also work. Without a lookup the
     * translator still applies the shape heuristic on strings for
     * backwards compatibility.
     */
    private static final java.util.regex.Pattern ISO_DATE_LIKE = java.util.regex.Pattern.compile(
        "^\\d{4}-\\d{2}-\\d{2}"                       // date
            + "(?:T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?"  // optional time
            + "(?:Z|[+-]\\d{2}:?\\d{2})?)?$"          // optional zone
    );

    private static String literal(Object value, String fieldName, Function<String, String> fieldTypeLookup) {
        if (value == null) {
            throw new IllegalArgumentException("[lance_knn] filter value on [" + fieldName + "] must not be null");
        }
        String fieldType = fieldTypeLookup == null ? null : fieldTypeLookup.apply(fieldName);
        if (value instanceof Boolean) {
            return value.toString().toLowerCase(Locale.ROOT);
        }
        if (value instanceof Number n) {
            if ("date".equals(fieldType)) {
                // OpenSearch's date field emits epoch milliseconds for
                // numeric input (the default `strict_date_optional_time
                // ||epoch_millis` format). DataFusion rejects a bare
                // Int64 literal against a Timestamp column
                // ("could not convert to literal of type
                // 'Timestamp(...)'"), so lift the value into a
                // Timestamp of millisecond precision. Coercion to
                // whatever unit / TZ the Lance column carries happens
                // during comparison.
                return "to_timestamp_millis(" + n.longValue() + ")";
            }
            return n.toString();
        }
        if (value instanceof String s) {
            if (ISO_DATE_LIKE.matcher(s).matches()) {
                if ("date".equals(fieldType)) {
                    return "timestamp '" + s + "'";
                }
                if (fieldType == null) {
                    // No mapping context: fall back to the shape
                    // heuristic. Documented false-positive risk: a
                    // Utf8 column whose stored value happens to be
                    // an ISO date shape ends up compared to a
                    // timestamp literal, which DataFusion rejects at
                    // evaluation time. Callers that can supply a
                    // {@link Function} lookup (see
                    // {@link #toLanceSql(QueryBuilder, Function)})
                    // avoid the false positive entirely.
                    return "timestamp '" + s + "'";
                }
                // Field type is known and it is not `date`: emit
                // the literal as a plain SQL string. This is the
                // fix for the pre-#48 false positive.
            }
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

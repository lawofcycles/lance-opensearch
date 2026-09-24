/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.DecayFunctionBuilder;
import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.search.DocValueFormat;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.lance.plan.translate.AggregationToRel.dateDocValueFormat;
import static org.opensearch.lance.plan.translate.AggregationToRel.unsupported;

/**
 * Turns a {@link QueryBuilder} into a {@link RexNode} predicate over
 * the scan row type, with the semantics the Lucene query the shard
 * path would run has: {@code term} and {@code terms} compare in the
 * field's own type (a float literal in single precision, a date
 * through the field's date format as the {@code [floor, ceiling]} of
 * the value's precision), {@code range} bounds likewise, {@code exists}
 * is {@code IS NOT NULL}, and a {@code bool} ANDs its {@code must} /
 * {@code filter} clauses, negates {@code must_not} without excluding
 * rows that have no value ({@code NOT (x IS TRUE)}), and requires one
 * {@code should} only when there is no {@code must} / {@code filter},
 * as {@code BooleanQuery} does with no {@code minimum_should_match}.
 * {@code wildcard}, {@code regexp} and {@code prefix} on a string
 * column become the pattern calls
 * {@code LIKE} / {@code ILIKE} (with the Lucene pattern rewritten to a
 * backslash escaped SQL pattern), {@code RLIKE} (with the pattern
 * anchored the way Lucene matches the whole term) and
 * {@code STARTS_WITH}; {@code ids} compares the index's primary key
 * column; {@code match_none} is the {@code false} literal. The compound
 * queries that only shape scores reduce to the rows they match when
 * the request reads no score ({@link Scores#UNUSED}): a
 * {@code constant_score} is its filter, a {@code dis_max} is the
 * disjunction of its clauses, a {@code boosting} is its positive clause
 * (the negative clause must translate too, so a refusal the Lucene
 * composition would answer is not lost, but it selects nothing), and a
 * {@code function_score} is its query, provided it has no
 * {@code min_score} (a score threshold has no predicate form) and no
 * {@code script_score} function (a script runs on the Lucene side only,
 * and its failures with it); the filters and the fields of its other
 * functions must resolve for the same reason the negative clause must.
 * On a request whose scores order or filter the hits
 * ({@link Scores#USED}) the four compounds throw, because the predicate
 * has no score and the page would come back in another order than the
 * one the query asks for; a {@code bool} whose optional {@code should}
 * clauses would score its matches apart (a {@code should} next to a
 * {@code must} or {@code filter}, or several {@code should}) throws for
 * the same reason, while one that scores every match alike translates.
 * Every other
 * query type, a value the field's type does not accept, and an
 * unmapped field throw {@link UnsupportedOperationException} naming
 * the element.
 *
 * <p>Field resolution mirrors the SQL translator this class replaces:
 * a top level column by name, a keyword sub-field declared in the
 * attach's multi fields to its base column, and a dotted path through
 * struct columns to a nested field access. A dotted path that crosses
 * a list (the {@code nested} mapping) or names nothing throws.
 */
public final class QueryToRex {

    /**
     * Most values a {@code terms} query hands to the scan filter, the
     * default of the {@code index.max_terms_count} bound the shard
     * path's {@code TermsQueryBuilder} enforces when it builds the
     * Lucene query.
     */
    static final int MAX_TERMS_VALUES = IndexSettings.MAX_TERMS_COUNT_SETTING.getDefault(Settings.EMPTY);

    /**
     * What the request the predicate serves does with document scores.
     * The predicate itself carries no score (every row it selects scores
     * 1.0 on the Lucene side), so the compound queries whose only effect
     * is on scores ({@code constant_score}, {@code dis_max},
     * {@code boosting}, {@code function_score}) reduce to the rows they
     * match only when nothing reads the score, and a {@code bool} whose
     * optional clauses would score its matches apart translates only
     * then as well.
     */
    public enum Scores {
        /**
         * No score is read: a count, an aggregation request, a page
         * ordered by stored columns, the filter of a bucket, a knn
         * prefilter or the scalar companion of a full text clause.
         */
        UNUSED,
        /**
         * Scores order or filter the hits: a page in score order, a sort
         * naming {@code _score}, {@code track_scores}, {@code min_score},
         * a {@code rescore} or a {@code collapse}.
         */
        USED
    }

    private QueryToRex() {}

    /**
     * The predicate of a request's {@code query} clause over the scan
     * the builder holds, for a request that reads no score
     * ({@link Scores#UNUSED}).
     *
     * @throws UnsupportedOperationException naming the first element
     *     outside the supported set
     */
    public static RexNode translate(QueryBuilder query, LanceSchemas.IndexModel model, RelBuilder relBuilder) {
        return translate(query, model, relBuilder, Scores.UNUSED);
    }

    /**
     * The predicate of a request's {@code query} clause over the scan
     * the builder holds.
     *
     * @param scores whether the request reads document scores, which
     *     decides whether the score shaping compounds translate
     * @throws UnsupportedOperationException naming the first element
     *     outside the supported set
     */
    public static RexNode translate(QueryBuilder query, LanceSchemas.IndexModel model, RelBuilder relBuilder, Scores scores) {
        return predicate(
            query,
            new Context(
                "",
                model.arrowSchema(),
                model.multiFields(),
                model.renamedFields(),
                model.primaryKeyField(),
                model.dateOverrideColumns(),
                scores
            ),
            relBuilder
        );
    }

    /**
     * The predicate of one {@code filter} / {@code filters} bucket. A
     * null query (a {@code filter} bucket without one) is a
     * {@code match_all} to the aggregator.
     *
     * @param aggregationName names the aggregation in refusal messages
     */
    static RexNode predicate(
        QueryBuilder query,
        String aggregationName,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        Map<String, String> renamedFields,
        RelBuilder relBuilder
    ) {
        return predicate(
            query,
            new Context(
                " in filter of aggregation [" + aggregationName + "]",
                schema,
                multiFields,
                renamedFields,
                "",
                Set.of(),
                Scores.UNUSED
            ),
            relBuilder
        );
    }

    /**
     * Where a refusal happened ({@link #where} is empty for the request's
     * query clause), what fields resolve against, and whether the
     * request reads scores.
     */
    private record Context(String where, Schema schema, Map<String, LinkedHashMap<String, String>> multiFields, Map<
        String,
        String> renamedFields, String primaryKey, Set<String> dateOverrides, Scores scores) {
    }

    private static RexNode predicate(QueryBuilder query, Context context, RelBuilder relBuilder) {
        if (query == null || query instanceof MatchAllQueryBuilder) {
            return relBuilder.literal(true);
        }
        if (query instanceof MatchNoneQueryBuilder) {
            return relBuilder.literal(false);
        }
        if (query instanceof TermQueryBuilder term) {
            Target target = resolve(term.fieldName(), context, relBuilder);
            return equalTo(target, term.value(), context, relBuilder);
        }
        if (query instanceof TermsQueryBuilder terms) {
            Target target = resolve(terms.fieldName(), context, relBuilder);
            if (terms.values() == null || terms.values().isEmpty()) {
                return relBuilder.literal(false);
            }
            if (terms.values().size() > MAX_TERMS_VALUES) {
                throw unsupported(
                    "terms on field ["
                        + terms.fieldName()
                        + "] with ["
                        + terms.values().size()
                        + "] values (max "
                        + MAX_TERMS_VALUES
                        + ")"
                        + context.where()
                );
            }
            return anyOf(target, terms.values(), context, relBuilder);
        }
        if (query instanceof IdsQueryBuilder ids) {
            if (context.primaryKey() == null || context.primaryKey().isEmpty()) {
                throw unsupported("ids query without a primary key column" + context.where());
            }
            Target target = resolve(context.primaryKey(), context, relBuilder);
            if (ids.ids().isEmpty()) {
                return relBuilder.literal(false);
            }
            // The builder keeps the ids in a hash set; sort them so the
            // predicate (and the SQL printed from it) is deterministic:
            // by the coerced value on a numeric primary key (so 2 sorts
            // before 10), lexicographically on a keyword key.
            List<String> values = new ArrayList<>();
            for (String id : ids.ids()) {
                values.add(id);
            }
            if (target.isUtf8()) {
                values.sort(null);
            } else {
                try {
                    values.sort(Comparator.comparing(BigDecimal::new));
                } catch (NumberFormatException unparseable) {
                    // A value the key cannot parse fails in equalTo with
                    // the value named; the order no longer matters.
                    values.sort(null);
                }
            }
            return anyOf(target, values, context, relBuilder);
        }
        if (query instanceof ExistsQueryBuilder exists) {
            Target target = resolve(exists.fieldName(), context, relBuilder);
            return relBuilder.isNotNull(target.ref());
        }
        if (query instanceof RangeQueryBuilder range) {
            Target target = resolve(range.fieldName(), context, relBuilder);
            return rangeOf(target, range, context, relBuilder);
        }
        if (query instanceof WildcardQueryBuilder wildcard) {
            Target target = resolve(wildcard.fieldName(), context, relBuilder);
            requireStringColumn(target, "wildcard", wildcard.fieldName(), context);
            RexNode pattern = relBuilder.literal(likePattern(wildcard.value()));
            return relBuilder.call(
                wildcard.caseInsensitive() ? SqlLibraryOperators.ILIKE : SqlStdOperatorTable.LIKE,
                target.ref(),
                pattern,
                relBuilder.literal("\\")
            );
        }
        if (query instanceof RegexpQueryBuilder regexp) {
            Target target = resolve(regexp.fieldName(), context, relBuilder);
            requireStringColumn(target, "regexp", regexp.fieldName(), context);
            // Same flag handling as RegexpQueryBuilder.doToQuery, so the
            // stock search path and this translator agree on which Lucene
            // operators the pattern may use and on case folding.
            int syntaxFlags = regexp.flags() & (RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT);
            boolean caseInsensitive = regexp.caseInsensitive();
            rejectLuceneOnlyOperators(regexp.fieldName(), regexp.value(), syntaxFlags, context);
            String anchored = (caseInsensitive ? "(?i)" : "") + "^(?:" + regexp.value() + ")$";
            return relBuilder.call(SqlLibraryOperators.RLIKE, target.ref(), relBuilder.literal(anchored));
        }
        if (query instanceof PrefixQueryBuilder prefix) {
            Target target = resolve(prefix.fieldName(), context, relBuilder);
            requireStringColumn(target, "prefix", prefix.fieldName(), context);
            if (prefix.caseInsensitive()) {
                return relBuilder.call(
                    SqlLibraryOperators.STARTS_WITH,
                    relBuilder.call(SqlStdOperatorTable.LOWER, target.ref()),
                    relBuilder.call(SqlStdOperatorTable.LOWER, relBuilder.literal(prefix.value()))
                );
            }
            return relBuilder.call(SqlLibraryOperators.STARTS_WITH, target.ref(), relBuilder.literal(prefix.value()));
        }
        if (query instanceof BoolQueryBuilder bool) {
            return boolOf(bool, context, relBuilder);
        }
        if (query instanceof ConstantScoreQueryBuilder constantScore) {
            requireScoresUnused("constant_score", context);
            return predicate(constantScore.innerQuery(), context, relBuilder);
        }
        if (query instanceof DisMaxQueryBuilder disMax) {
            requireScoresUnused("dis_max", context);
            // DisjunctionMaxQuery matches the union of its clauses; an
            // empty one matches nothing.
            RexNode any = null;
            for (QueryBuilder clause : disMax.innerQueries()) {
                RexNode one = predicate(clause, context, relBuilder);
                any = any == null ? one : relBuilder.call(SqlStdOperatorTable.OR, any, one);
            }
            return any == null ? relBuilder.literal(false) : any;
        }
        if (query instanceof BoostingQueryBuilder boosting) {
            requireScoresUnused("boosting", context);
            // The negative clause only lowers scores; it is translated so
            // a clause the translator refuses keeps the whole query on the
            // Lucene composition, which answers what the shard path would,
            // and the predicate is dropped.
            predicate(boosting.negativeQuery(), context, relBuilder);
            return predicate(boosting.positiveQuery(), context, relBuilder);
        }
        if (query instanceof FunctionScoreQueryBuilder functionScore) {
            return functionScoreOf(functionScore, context, relBuilder);
        }
        throw unsupported("query type [" + query.getName() + "]" + context.where());
    }

    /**
     * Refuses a score shaping compound on a request that reads scores:
     * the predicate scores every row 1.0, so the page would come back in
     * another order than the one the query asks for.
     */
    private static void requireScoresUnused(String queryName, Context context) {
        if (context.scores() == Scores.USED) {
            throw unsupported("query type [" + queryName + "] on a scored request" + context.where());
        }
    }

    /**
     * A {@code function_score} as the rows its query matches. Refused
     * with a {@code min_score} (the threshold reads the scores the
     * predicate does not have) and with a {@code script_score} function
     * (the script runs on the Lucene side only, where a failing script
     * answers 400). The filter of every function and the field a
     * {@code field_value_factor}, a decay function or a
     * {@code random_score} names are resolved and dropped, so a filter or
     * a field the translator cannot place keeps the query on the Lucene
     * composition and answers what the shard path answers.
     */
    private static RexNode functionScoreOf(FunctionScoreQueryBuilder functionScore, Context context, RelBuilder relBuilder) {
        // The reasons that hold under every envelope are named first,
        // so explain reports the same element for the same query.
        if (functionScore.getMinScore() != null) {
            throw unsupported("function_score with min_score" + context.where());
        }
        for (FunctionScoreQueryBuilder.FilterFunctionBuilder function : functionScore.filterFunctionBuilders()) {
            if (function.getScoreFunction() instanceof ScriptScoreFunctionBuilder) {
                throw unsupported("function_score with a script_score function" + context.where());
            }
        }
        requireScoresUnused("function_score", context);
        for (FunctionScoreQueryBuilder.FilterFunctionBuilder function : functionScore.filterFunctionBuilders()) {
            if (function.getFilter() != null) {
                predicate(function.getFilter(), context, relBuilder);
            }
            String field = functionField(function.getScoreFunction());
            if (field != null) {
                resolve(field, context, relBuilder);
            }
        }
        return predicate(functionScore.query(), context, relBuilder);
    }

    /** The field a score function reads, or null when it reads none (a {@code weight}, a {@code random_score} without a field). */
    private static String functionField(ScoreFunctionBuilder<?> function) {
        if (function instanceof FieldValueFactorFunctionBuilder factor) {
            return factor.fieldName();
        }
        if (function instanceof DecayFunctionBuilder<?> decay) {
            return decay.getFieldName();
        }
        if (function instanceof RandomScoreFunctionBuilder random) {
            return random.getField();
        }
        return null;
    }

    /** {@code target = v1 OR target = v2 OR ...} over two or more values, the shape the SQL printer collapses to {@code IN}. */
    private static RexNode anyOf(Target target, List<?> values, Context context, RelBuilder relBuilder) {
        RexNode any = null;
        for (Object value : values) {
            RexNode equal = equalTo(target, value, context, relBuilder);
            any = any == null ? equal : relBuilder.call(SqlStdOperatorTable.OR, any, equal);
        }
        return any;
    }

    private static RexNode boolOf(BoolQueryBuilder bool, Context context, RelBuilder relBuilder) {
        if (bool.minimumShouldMatch() != null) {
            throw unsupported("minimum_should_match" + context.where());
        }
        if (context.scores() == Scores.USED
            && !bool.should().isEmpty()
            && bool.should().size() + bool.must().size() + bool.filter().size() > 1) {
            // BooleanQuery scores a row by the optional clauses it matches
            // on top of the required ones, so rows the predicate selects
            // alike land in different places of a score ordered page; a
            // single should clause alone is required and scores every
            // match the same.
            throw unsupported("bool with optional should clauses on a scored request" + context.where());
        }
        RexNode all = null;
        for (QueryBuilder clause : bool.must()) {
            all = conjoin(all, predicate(clause, context, relBuilder), relBuilder);
        }
        for (QueryBuilder clause : bool.filter()) {
            all = conjoin(all, predicate(clause, context, relBuilder), relBuilder);
        }
        boolean required = bool.must().isEmpty() && bool.filter().isEmpty();
        if (required && !bool.should().isEmpty()) {
            RexNode any = null;
            for (QueryBuilder clause : bool.should()) {
                RexNode one = predicate(clause, context, relBuilder);
                any = any == null ? one : relBuilder.call(SqlStdOperatorTable.OR, any, one);
            }
            all = any;
        }
        if (all == null && !bool.mustNot().isEmpty() && !bool.adjustPureNegative()) {
            // A purely negative BooleanQuery matches nothing unless the
            // builder adds the match_all it does by default.
            throw unsupported("adjust_pure_negative false" + context.where());
        }
        for (QueryBuilder clause : bool.mustNot()) {
            RexNode excluded = predicate(clause, context, relBuilder);
            RexNode negated = relBuilder.call(SqlStdOperatorTable.NOT, relBuilder.call(SqlStdOperatorTable.IS_TRUE, excluded));
            all = conjoin(all, negated, relBuilder);
        }
        return all == null ? relBuilder.literal(true) : all;
    }

    private static RexNode conjoin(RexNode left, RexNode right, RelBuilder relBuilder) {
        return left == null ? right : relBuilder.call(SqlStdOperatorTable.AND, left, right);
    }

    // ---------------------------------------------------------------
    // Field resolution
    // ---------------------------------------------------------------

    /**
     * A field resolved to its Lance column or struct child: the
     * reference expression, the Arrow type behind it, and whether the
     * attach overrode the integer column as {@code date} (its
     * epoch-millis literals then also accept the ISO-8601 strings the
     * override serves).
     */
    private record Target(String name, RexNode ref, ArrowType type, boolean dateOnInteger) {
        boolean isDate() {
            return type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp;
        }

        boolean isBoolean() {
            return type instanceof ArrowType.Bool;
        }

        boolean isFloating() {
            return type instanceof ArrowType.FloatingPoint;
        }

        boolean isSingleFloat() {
            return type instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == FloatingPointPrecision.SINGLE;
        }

        boolean isUtf8() {
            return type instanceof ArrowType.Utf8;
        }
    }

    /**
     * Maps a query field to its reference over the scan row type: a top
     * level column by name, a keyword sub-field declared in the multi
     * fields spec to its base column, or a dotted path through struct
     * columns to a nested field access. Throws for anything else,
     * including a path that crosses a list (the {@code nested} mapping,
     * whose children only resolve through the Lucene side) and a column
     * whose type no predicate supports.
     */
    private static Target resolve(String field, Context context, RelBuilder relBuilder) {
        Schema schema = context.schema();
        String columnName = field;
        if (topLevelIndex(schema, columnName) < 0) {
            int dot = field.lastIndexOf('.');
            if (dot > 0 && context.multiFields() != null) {
                String base = field.substring(0, dot);
                String sub = field.substring(dot + 1);
                LinkedHashMap<String, String> subs = context.multiFields().get(base);
                if (subs != null && "keyword".equals(subs.get(sub))) {
                    columnName = base;
                }
            }
        }
        int index = topLevelIndex(schema, columnName);
        if (index >= 0) {
            ArrowType type = schema.getFields().get(index).getType();
            requireSupportedScalar(type, columnName, field, context);
            boolean dateOnInteger = type instanceof ArrowType.Int && context.dateOverrides().contains(columnName);
            return new Target(columnName, relBuilder.field(index), type, dateOnInteger);
        }
        if (columnName.indexOf('.') >= 0) {
            Target nested = resolveStructPath(columnName, field, context, relBuilder);
            if (nested != null) {
                return nested;
            }
        }
        // Both messages match the aggregation translator's resolveColumn
        // contract, which never carries the bucket context.
        String renamedTo = context.renamedFields() == null ? null : context.renamedFields().get(columnName);
        if (renamedTo != null) {
            throw unsupported("field [" + field + "] was renamed to [" + renamedTo + "] in the Lance table");
        }
        throw unsupported("field [" + field + "] does not map to a Lance column");
    }

    /**
     * Walks a dotted path through struct columns to a field access
     * chain, or returns null when a segment does not name a struct
     * child (the caller reports the field as unmapped, matching the
     * behaviour for multi-field sub-fields and {@code nested} children,
     * which resolve on the Lucene side only).
     */
    private static Target resolveStructPath(String columnName, String field, Context context, RelBuilder relBuilder) {
        String[] segments = columnName.split("\\.");
        int top = topLevelIndex(context.schema(), segments[0]);
        if (top < 0) {
            return null;
        }
        Field current = context.schema().getFields().get(top);
        RexNode ref = relBuilder.field(top);
        for (int i = 1; i < segments.length; i++) {
            if (!(current.getType() instanceof ArrowType.Struct)) {
                return null;
            }
            Field child = childOf(current, segments[i]);
            if (child == null) {
                return null;
            }
            ref = relBuilder.getRexBuilder().makeFieldAccess(ref, segments[i], true);
            current = child;
        }
        requireSupportedScalar(current.getType(), columnName, field, context);
        return new Target(columnName, ref, current.getType(), false);
    }

    private static Field childOf(Field struct, String name) {
        for (Field child : struct.getChildren()) {
            if (child.getName().equals(name)) {
                return child;
            }
        }
        return null;
    }

    private static int topLevelIndex(Schema schema, String column) {
        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(column)) {
                return i;
            }
        }
        return -1;
    }

    /** The scalar types a predicate compares; the same set the aggregation translator reads. */
    private static void requireSupportedScalar(ArrowType type, String columnName, String field, Context context) {
        boolean supported = type instanceof ArrowType.Utf8
            || type instanceof ArrowType.Bool
            || type instanceof ArrowType.Date
            || type instanceof ArrowType.Timestamp
            || (type instanceof ArrowType.Int intType && intType.getIsSigned())
            || (type instanceof ArrowType.FloatingPoint fp
                && (fp.getPrecision() == FloatingPointPrecision.SINGLE || fp.getPrecision() == FloatingPointPrecision.DOUBLE));
        if (!supported) {
            throw unsupported(
                "column [" + columnName + "] behind field [" + field + "]" + context.where() + " is not a supported scalar column"
            );
        }
    }

    /**
     * Refuses a pattern query on a column that is not a string: the
     * stock search path answers those with 400 from the field type, and
     * lowering them would hand DataFusion a pattern call on a numeric
     * column and surface its planning error instead.
     */
    private static void requireStringColumn(Target target, String queryName, String field, Context context) {
        if (!target.isUtf8()) {
            throw unsupported(
                queryName + " on column [" + target.name() + "] behind field [" + field + "]" + context.where() + " is not a string column"
            );
        }
    }

    // ---------------------------------------------------------------
    // Comparisons in the column's type
    // ---------------------------------------------------------------

    /** {@code target = value} in the target's type. */
    private static RexNode equalTo(Target target, Object value, Context context, RelBuilder relBuilder) {
        if (value == null) {
            throw unsupported("null term value" + context.where());
        }
        RexNode reference = target.ref();
        if (target.isUtf8()) {
            return relBuilder.call(SqlStdOperatorTable.EQUALS, reference, relBuilder.literal(text(value)));
        }
        if (target.isDate()) {
            // The date field type answers a term with the range
            // [floor, ceiling] of the value's precision.
            Long lower = parseDate(null, value, false, context, target);
            Long upper = parseDate(null, value, true, context, target);
            RexNode millis = epochMillis(target, relBuilder);
            return relBuilder.call(
                SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, millis, relBuilder.literal(lower)),
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, millis, relBuilder.literal(upper))
            );
        }
        if (target.isBoolean()) {
            Boolean flag = bool(value);
            if (flag == null) {
                throw badValue(value, target, context);
            }
            return relBuilder.call(SqlStdOperatorTable.EQUALS, reference, relBuilder.literal(flag));
        }
        if (target.isFloating()) {
            Double number = floating(target, value);
            if (number == null) {
                throw badValue(value, target, context);
            }
            return relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.cast(reference, SqlTypeName.DOUBLE), relBuilder.literal(number));
        }
        Long number = integral(target, value);
        if (number == null) {
            throw badValue(value, target, context);
        }
        return relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.cast(reference, SqlTypeName.BIGINT), relBuilder.literal(number));
    }

    /**
     * The range query's bounds as the field type resolves them: a date
     * bound parsed by the field's format with the query's own
     * {@code format} / {@code time_zone}, rounded up for an exclusive
     * lower or inclusive upper bound and then moved off the excluded
     * millisecond; a number in the column's precision. Keyword and
     * boolean ranges stay unsupported, as on the pushdown side.
     */
    private static RexNode rangeOf(Target target, RangeQueryBuilder range, Context context, RelBuilder relBuilder) {
        if (target.isUtf8() || target.isBoolean()) {
            throw unsupported("range on column [" + target.name() + "]" + context.where() + " is not numeric");
        }
        if (range.from() == null && range.to() == null) {
            throw unsupported("range without bounds" + context.where());
        }
        if (target.isDate()) {
            RexNode millis = epochMillis(target, relBuilder);
            RexNode lower = null;
            RexNode upper = null;
            if (range.from() != null) {
                long from = parseDate(range, range.from(), !range.includeLower(), context, target);
                lower = relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    millis,
                    relBuilder.literal(range.includeLower() ? from : from + 1L)
                );
            }
            if (range.to() != null) {
                long to = parseDate(range, range.to(), range.includeUpper(), context, target);
                upper = relBuilder.call(
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    millis,
                    relBuilder.literal(range.includeUpper() ? to : to - 1L)
                );
            }
            return lower == null ? upper : upper == null ? lower : relBuilder.call(SqlStdOperatorTable.AND, lower, upper);
        }
        RexNode value;
        RexNode lowerBound = null;
        RexNode upperBound = null;
        if (target.isFloating()) {
            value = relBuilder.cast(target.ref(), SqlTypeName.DOUBLE);
            if (range.from() != null) {
                Double from = floating(target, range.from());
                if (from == null) {
                    throw badValue(range.from(), target, context);
                }
                lowerBound = relBuilder.literal(from);
            }
            if (range.to() != null) {
                Double to = floating(target, range.to());
                if (to == null) {
                    throw badValue(range.to(), target, context);
                }
                upperBound = relBuilder.literal(to);
            }
        } else {
            value = relBuilder.cast(target.ref(), SqlTypeName.BIGINT);
            if (range.from() != null) {
                Long from = integral(target, range.from());
                if (from == null) {
                    throw badValue(range.from(), target, context);
                }
                lowerBound = relBuilder.literal(from);
            }
            if (range.to() != null) {
                Long to = integral(target, range.to());
                if (to == null) {
                    throw badValue(range.to(), target, context);
                }
                upperBound = relBuilder.literal(to);
            }
        }
        RexNode lower = lowerBound == null
            ? null
            : relBuilder.call(
                range.includeLower() ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.GREATER_THAN,
                value,
                lowerBound
            );
        RexNode upper = upperBound == null
            ? null
            : relBuilder.call(
                range.includeUpper() ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN,
                value,
                upperBound
            );
        return lower == null ? upper : upper == null ? lower : relBuilder.call(SqlStdOperatorTable.AND, lower, upper);
    }

    /** Epoch milliseconds of a date or timestamp target, through the timestamp cast a {@code DATE} column needs. */
    private static RexNode epochMillis(Target target, RelBuilder relBuilder) {
        RexNode reference = target.ref();
        if (target.type() instanceof ArrowType.Date) {
            reference = relBuilder.cast(reference, SqlTypeName.TIMESTAMP);
        }
        return relBuilder.call(SqlLibraryOperators.UNIX_MILLIS, reference);
    }

    /**
     * Epoch millis of a date bound through the field's date format (the
     * range query's {@code format} and {@code time_zone} when it names
     * them), throwing when the text does not parse.
     */
    private static Long parseDate(RangeQueryBuilder range, Object value, boolean roundUp, Context context, Target target) {
        try {
            String pattern = range == null ? null : range.format();
            ZoneId zone = range == null || range.timeZone() == null ? null : ZoneId.of(range.timeZone());
            DocValueFormat format = dateDocValueFormat(pattern, zone);
            return format.parseLong(text(value), roundUp, System::currentTimeMillis);
        } catch (RuntimeException unparseable) {
            throw badValue(value, target, context);
        }
    }

    // ---------------------------------------------------------------
    // Pattern rewriting for wildcard / regexp
    // ---------------------------------------------------------------

    /**
     * Translates a Lucene wildcard pattern into a SQL {@code LIKE}
     * pattern whose escape character is {@code \}. {@code *} becomes
     * {@code %}, {@code ?} becomes {@code _}, a literal {@code %},
     * {@code _} or {@code \} is prefixed with {@code \}, and a Lucene
     * escape {@code \x} yields the literal {@code x} (escaped again
     * when {@code x} is one of the three). A trailing lone backslash
     * is a literal backslash, as in Lucene's lenient parsing.
     */
    static String likePattern(String wildcard) {
        StringBuilder sb = new StringBuilder(wildcard.length() + 8);
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            switch (c) {
                case '*' -> sb.append('%');
                case '?' -> sb.append('_');
                case '\\' -> {
                    if (i + 1 < wildcard.length()) {
                        i++;
                        appendLikeLiteral(sb, wildcard.charAt(i));
                    } else {
                        appendLikeLiteral(sb, '\\');
                    }
                }
                default -> appendLikeLiteral(sb, c);
            }
        }
        return sb.toString();
    }

    private static void appendLikeLiteral(StringBuilder sb, char c) {
        if (c == '%' || c == '_' || c == '\\') {
            sb.append('\\');
        }
        sb.append(c);
    }

    /**
     * Walks the pattern the way Lucene's {@link RegExp} parser does
     * (a {@code \} escapes the next character, {@code [...]} is a
     * character class whose content is literal) and refuses the
     * operators only Lucene has, because Lance evaluates the pattern
     * with Rust's {@code regex} crate, which would read them as literal
     * characters and silently return a different answer. Each is
     * refused only when the corresponding syntax flag is on;
     * {@code "..."} is refused unconditionally because Lucene always
     * reads it as a quoted literal.
     */
    private static void rejectLuceneOnlyOperators(String field, String pattern, int syntaxFlags, Context context) {
        boolean inClass = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\') {
                i++;
                continue;
            }
            if (inClass) {
                if (c == ']') {
                    inClass = false;
                }
                continue;
            }
            String operator = switch (c) {
                case '[' -> {
                    inClass = true;
                    yield null;
                }
                case '"' -> "\"...\" (quoted literal)";
                case '~' -> (syntaxFlags & RegExp.DEPRECATED_COMPLEMENT) != 0 ? "~ (complement)" : null;
                case '&' -> (syntaxFlags & RegExp.INTERSECTION) != 0 ? "& (intersection)" : null;
                case '<' -> (syntaxFlags & (RegExp.INTERVAL | RegExp.AUTOMATON)) != 0 ? "<n-m> (numeric interval)" : null;
                case '@' -> (syntaxFlags & RegExp.ANYSTRING) != 0 ? "@ (any string)" : null;
                case '#' -> (syntaxFlags & RegExp.EMPTY) != 0 ? "# (empty language)" : null;
                default -> null;
            };
            if (operator != null) {
                throw unsupported(
                    "regexp on field ["
                        + field
                        + "]"
                        + context.where()
                        + " uses the Lucene only operator "
                        + operator
                        + " (position "
                        + i
                        + " of ["
                        + pattern
                        + "]), which Lance's Rust regex would read literally"
                );
            }
        }
    }

    // ---------------------------------------------------------------
    // Value coercion
    // ---------------------------------------------------------------

    private static UnsupportedOperationException badValue(Object value, Target target, Context context) {
        return unsupported("value [" + text(value) + "] on column [" + target.name() + "]" + context.where());
    }

    private static String text(Object value) {
        return value instanceof BytesRef bytes ? bytes.utf8ToString() : String.valueOf(value);
    }

    private static Boolean bool(Object value) {
        if (value instanceof Boolean flag) {
            return flag;
        }
        String text = text(value);
        if (text.equals("true")) {
            return true;
        }
        return text.equals("false") ? false : null;
    }

    /**
     * Parses the ISO-8601 shapes a {@code date} override on an integer
     * column accepts, so a string bound becomes the epoch millis the
     * column stores. The same default format the {@code date} field
     * type uses for query-time parsing; missing time components
     * default to midnight UTC.
     */
    private static final DateFormatter DATE_OVERRIDE_PARSER = DateFormatter.forPattern("strict_date_optional_time");

    /**
     * The value as the integer column stores it: a whole number as is;
     * on a {@code date} override column an ISO-8601 string parses to
     * its epoch millis, because the override's column holds the millis
     * as a plain integer while callers query it with date strings.
     */
    private static Long integral(Target target, Object value) {
        Long number = integral(value);
        if (number != null) {
            return number;
        }
        if (target.dateOnInteger() && (value instanceof String || value instanceof BytesRef)) {
            try {
                return DateFormatters.from(DATE_OVERRIDE_PARSER.parse(text(value))).toInstant().toEpochMilli();
            } catch (RuntimeException unparseable) {
                return null;
            }
        }
        return null;
    }

    /** A whole number, or null: a fractional value on an integer column has rounding rules the pushdown does not replicate. */
    private static Long integral(Object value) {
        if (value instanceof Boolean) {
            return null;
        }
        double number;
        if (value instanceof Number n) {
            number = n.doubleValue();
        } else {
            try {
                number = Double.parseDouble(text(value));
            } catch (NumberFormatException unparseable) {
                return null;
            }
        }
        if (!Double.isFinite(number) || number != Math.rint(number) || Math.abs(number) > 9.007199254740992E15d) {
            return null;
        }
        return (long) number;
    }

    /** The value in the column's precision: a float column compares in single precision, as the float field type does. */
    private static Double floating(Target target, Object value) {
        if (value instanceof Boolean) {
            return null;
        }
        double number;
        if (value instanceof Number n) {
            number = n.doubleValue();
        } else {
            try {
                number = Double.parseDouble(text(value));
            } catch (NumberFormatException unparseable) {
                return null;
            }
        }
        if (Double.isNaN(number)) {
            return null;
        }
        return target.isSingleFloat() ? (double) (float) number : number;
    }

    /**
     * Whether any leaf of {@code query} names a field in
     * {@code columns}, or a dotted child of one. The callers that turn
     * a translated predicate into executable Lance SQL use this as a
     * pre-flight for the {@code ip} and {@code geo_point} override
     * columns: an ip column stores raw strings while the stock search path
     * compares 16 byte encoded forms, and a geo column's children are
     * hidden by the geo_point mapping, so no predicate on them may
     * travel to Lance SQL and the caller keeps the query on the Lucene
     * side. Only the leaf shapes this translator supports are
     * inspected; anything else refuses at translation anyway.
     */
    public static boolean referencesAny(QueryBuilder query, Set<String> columns) {
        if (query == null || columns.isEmpty()) {
            return false;
        }
        for (String field : referencedFields(query)) {
            if (columns.contains(field)) {
                return true;
            }
            int dot = field.indexOf('.');
            while (dot > 0) {
                if (columns.contains(field.substring(0, dot))) {
                    return true;
                }
                dot = field.indexOf('.', dot + 1);
            }
        }
        return false;
    }

    /**
     * The fields the leaves of {@code query} name, as the request spells
     * them (a keyword sub field as {@code body.raw}, a struct child as
     * {@code location.lat}), through every {@code bool} clause, the
     * clauses of a {@code constant_score}, {@code dis_max},
     * {@code boosting} or {@code function_score} (its function filters
     * and the fields its functions read included) and the inner
     * {@code filter} of a {@code lance_knn}. Only the leaf shapes
     * this translator supports contribute; a full text clause, a
     * {@code match} or any other query names nothing here. Empty for a
     * null query.
     */
    public static Set<String> referencedFields(QueryBuilder query) {
        Set<String> fields = new LinkedHashSet<>();
        collectFields(query, fields);
        return fields;
    }

    private static void collectFields(QueryBuilder query, Set<String> fields) {
        if (query == null) {
            return;
        }
        if (query instanceof BoolQueryBuilder bool) {
            for (List<QueryBuilder> clauses : List.of(bool.must(), bool.filter(), bool.mustNot(), bool.should())) {
                for (QueryBuilder clause : clauses) {
                    collectFields(clause, fields);
                }
            }
            return;
        }
        if (query instanceof LanceKnnQueryBuilder knn) {
            collectFields(knn.filter(), fields);
            return;
        }
        if (query instanceof ConstantScoreQueryBuilder constantScore) {
            collectFields(constantScore.innerQuery(), fields);
            return;
        }
        if (query instanceof DisMaxQueryBuilder disMax) {
            for (QueryBuilder clause : disMax.innerQueries()) {
                collectFields(clause, fields);
            }
            return;
        }
        if (query instanceof BoostingQueryBuilder boosting) {
            collectFields(boosting.positiveQuery(), fields);
            collectFields(boosting.negativeQuery(), fields);
            return;
        }
        if (query instanceof FunctionScoreQueryBuilder functionScore) {
            collectFields(functionScore.query(), fields);
            for (FunctionScoreQueryBuilder.FilterFunctionBuilder function : functionScore.filterFunctionBuilders()) {
                collectFields(function.getFilter(), fields);
                String field = functionField(function.getScoreFunction());
                if (field != null) {
                    fields.add(field);
                }
            }
            return;
        }
        String field = null;
        if (query instanceof TermQueryBuilder term) {
            field = term.fieldName();
        } else if (query instanceof TermsQueryBuilder terms) {
            field = terms.fieldName();
        } else if (query instanceof RangeQueryBuilder range) {
            field = range.fieldName();
        } else if (query instanceof ExistsQueryBuilder exists) {
            field = exists.fieldName();
        } else if (query instanceof WildcardQueryBuilder wildcard) {
            field = wildcard.fieldName();
        } else if (query instanceof RegexpQueryBuilder regexp) {
            field = regexp.fieldName();
        } else if (query instanceof PrefixQueryBuilder prefix) {
            field = prefix.fieldName();
        }
        if (field != null) {
            fields.add(field);
        }
    }
}

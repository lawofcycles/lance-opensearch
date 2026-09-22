/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

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
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.BoolQueryBuilder;
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
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.search.DocValueFormat;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
 * column; {@code match_none} is the {@code false} literal. Every other
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
final class QueryToRex {

    /**
     * Most values a {@code terms} query hands to the scan filter, the
     * default of the {@code index.max_terms_count} bound the shard
     * path's {@code TermsQueryBuilder} enforces when it builds the
     * Lucene query.
     */
    static final int MAX_TERMS_VALUES = IndexSettings.MAX_TERMS_COUNT_SETTING.getDefault(Settings.EMPTY);

    private QueryToRex() {}

    /**
     * The predicate of a request's {@code query} clause over the scan
     * the builder holds.
     *
     * @throws UnsupportedOperationException naming the first element
     *     outside the supported set
     */
    public static RexNode translate(QueryBuilder query, LanceSchemas.IndexModel model, RelBuilder relBuilder) {
        return predicate(
            query,
            new Context("", model.arrowSchema(), model.multiFields(), model.renamedFields(), model.primaryKeyField()),
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
            new Context(" in filter of aggregation [" + aggregationName + "]", schema, multiFields, renamedFields, ""),
            relBuilder
        );
    }

    /** Where a refusal happened ({@link #where} is empty for the request's query clause) and what fields resolve against. */
    private record Context(String where, Schema schema, Map<String, LinkedHashMap<String, String>> multiFields, Map<
        String,
        String> renamedFields, String primaryKey) {
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
            // predicate (and the SQL printed from it) is deterministic.
            List<Object> values = new ArrayList<>(ids.ids());
            values.sort(null);
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
            // shard path and this translator agree on which Lucene
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
        throw unsupported("query type [" + query.getName() + "]" + context.where());
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

    /** A field resolved to its Lance column or struct child: the reference expression and the Arrow type behind it. */
    private record Target(String name, RexNode ref, ArrowType type) {
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
            return type instanceof ArrowType.FloatingPoint fp
                && fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;
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
            return new Target(columnName, relBuilder.field(index), type);
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
        return new Target(columnName, ref, current.getType());
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
                && (fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE
                    || fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE));
        if (!supported) {
            throw unsupported(
                "column [" + columnName + "] behind field [" + field + "]" + context.where() + " is not a supported scalar column"
            );
        }
    }

    /**
     * Refuses a pattern query on a column that is not a string: the
     * shard path answers those with 400 from the field type, and
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
        Long number = integral(value);
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
                Long from = integral(range.from());
                if (from == null) {
                    throw badValue(range.from(), target, context);
                }
                lowerBound = relBuilder.literal(from);
            }
            if (range.to() != null) {
                Long to = integral(range.to());
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
}

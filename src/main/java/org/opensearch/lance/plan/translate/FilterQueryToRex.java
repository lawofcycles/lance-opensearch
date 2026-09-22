/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.lance.plan.translate.AggregationToRel.Column;
import org.opensearch.search.DocValueFormat;

import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.opensearch.lance.plan.translate.AggregationToRel.dateDocValueFormat;
import static org.opensearch.lance.plan.translate.AggregationToRel.resolveColumn;
import static org.opensearch.lance.plan.translate.AggregationToRel.unsupported;

/**
 * Turns the query of a {@code filter} / {@code filters} bucket into a
 * {@link RexNode} predicate over the scan row type, with the semantics
 * the Lucene query the aggregator would run has: {@code term} and
 * {@code terms} compare in the field's own type (a float literal in
 * single precision, a date through the field's date format as the
 * {@code [floor, ceiling]} of the value's precision), {@code range}
 * bounds likewise, {@code exists} is {@code IS NOT NULL}, and a
 * {@code bool} ANDs its {@code must} / {@code filter} clauses, negates
 * {@code must_not} without excluding rows that have no value
 * ({@code NOT (x IS TRUE)}), and requires one {@code should} only when
 * there is no {@code must} / {@code filter}, as {@code BooleanQuery}
 * does with no {@code minimum_should_match}. Every other query type,
 * a value the field's type does not accept, and an unmapped field throw
 * {@link UnsupportedOperationException} naming the element.
 *
 * <p>This helper serves the aggregation translator only; the request
 * level {@code query} clause has its own translation later.
 */
final class FilterQueryToRex {

    private FilterQueryToRex() {}

    /**
     * The predicate of one filter query. A null query (a {@code filter}
     * bucket without one) is a {@code match_all} to the aggregator.
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
        if (query == null || query instanceof MatchAllQueryBuilder) {
            return relBuilder.literal(true);
        }
        if (query instanceof TermQueryBuilder term) {
            Column column = resolveColumn(term.fieldName(), schema, multiFields, renamedFields);
            return equalTo(column, term.value(), aggregationName, relBuilder);
        }
        if (query instanceof TermsQueryBuilder terms) {
            Column column = resolveColumn(terms.fieldName(), schema, multiFields, renamedFields);
            if (terms.values() == null || terms.values().isEmpty()) {
                return relBuilder.literal(false);
            }
            RexNode any = null;
            for (Object value : terms.values()) {
                RexNode equal = equalTo(column, value, aggregationName, relBuilder);
                any = any == null ? equal : relBuilder.call(SqlStdOperatorTable.OR, any, equal);
            }
            return any;
        }
        if (query instanceof ExistsQueryBuilder exists) {
            Column column = resolveColumn(exists.fieldName(), schema, multiFields, renamedFields);
            return relBuilder.isNotNull(relBuilder.field(column.index()));
        }
        if (query instanceof RangeQueryBuilder range) {
            Column column = resolveColumn(range.fieldName(), schema, multiFields, renamedFields);
            return rangeOf(column, range, aggregationName, relBuilder);
        }
        if (query instanceof BoolQueryBuilder bool) {
            return boolOf(bool, aggregationName, schema, multiFields, renamedFields, relBuilder);
        }
        throw unsupported("query type [" + query.getName() + "] in filter of aggregation [" + aggregationName + "]");
    }

    private static RexNode boolOf(
        BoolQueryBuilder bool,
        String aggregationName,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        Map<String, String> renamedFields,
        RelBuilder relBuilder
    ) {
        if (bool.minimumShouldMatch() != null) {
            throw unsupported("minimum_should_match in filter of aggregation [" + aggregationName + "]");
        }
        RexNode all = null;
        for (QueryBuilder clause : bool.must()) {
            all = conjoin(all, predicate(clause, aggregationName, schema, multiFields, renamedFields, relBuilder), relBuilder);
        }
        for (QueryBuilder clause : bool.filter()) {
            all = conjoin(all, predicate(clause, aggregationName, schema, multiFields, renamedFields, relBuilder), relBuilder);
        }
        boolean required = bool.must().isEmpty() && bool.filter().isEmpty();
        if (required && !bool.should().isEmpty()) {
            RexNode any = null;
            for (QueryBuilder clause : bool.should()) {
                RexNode one = predicate(clause, aggregationName, schema, multiFields, renamedFields, relBuilder);
                any = any == null ? one : relBuilder.call(SqlStdOperatorTable.OR, any, one);
            }
            all = any;
        }
        if (all == null && !bool.mustNot().isEmpty() && !bool.adjustPureNegative()) {
            // A purely negative BooleanQuery matches nothing unless the
            // builder adds the match_all it does by default.
            throw unsupported("adjust_pure_negative false in filter of aggregation [" + aggregationName + "]");
        }
        for (QueryBuilder clause : bool.mustNot()) {
            RexNode excluded = predicate(clause, aggregationName, schema, multiFields, renamedFields, relBuilder);
            RexNode negated = relBuilder.call(SqlStdOperatorTable.NOT, relBuilder.call(SqlStdOperatorTable.IS_TRUE, excluded));
            all = conjoin(all, negated, relBuilder);
        }
        return all == null ? relBuilder.literal(true) : all;
    }

    private static RexNode conjoin(RexNode left, RexNode right, RelBuilder relBuilder) {
        return left == null ? right : relBuilder.call(SqlStdOperatorTable.AND, left, right);
    }

    /** {@code column = value} in the column's type. */
    private static RexNode equalTo(Column column, Object value, String aggregationName, RelBuilder relBuilder) {
        if (value == null) {
            throw unsupported("null term value in filter of aggregation [" + aggregationName + "]");
        }
        RexNode reference = relBuilder.field(column.index());
        if (column.isUtf8()) {
            return relBuilder.call(SqlStdOperatorTable.EQUALS, reference, relBuilder.literal(text(value)));
        }
        if (column.isDate()) {
            // The date field type answers a term with the range
            // [floor, ceiling] of the value's precision.
            Long lower = parseDate(null, value, false, aggregationName, column);
            Long upper = parseDate(null, value, true, aggregationName, column);
            RexNode millis = epochMillis(column, relBuilder);
            return relBuilder.call(
                SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, millis, relBuilder.literal(lower)),
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, millis, relBuilder.literal(upper))
            );
        }
        if (column.isBoolean()) {
            Boolean flag = bool(value);
            if (flag == null) {
                throw badValue(value, column, aggregationName);
            }
            return relBuilder.call(SqlStdOperatorTable.EQUALS, reference, relBuilder.literal(flag));
        }
        if (column.isFloating()) {
            Double number = floating(column, value);
            if (number == null) {
                throw badValue(value, column, aggregationName);
            }
            return relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.cast(reference, SqlTypeName.DOUBLE), relBuilder.literal(number));
        }
        Long number = integral(value);
        if (number == null) {
            throw badValue(value, column, aggregationName);
        }
        return relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.cast(reference, SqlTypeName.BIGINT), relBuilder.literal(number));
    }

    /**
     * The range query's bounds as the field type resolves them: a date
     * bound parsed by the field's format with the query's own
     * {@code format} / {@code time_zone}, rounded up for an exclusive
     * lower or inclusive upper bound and then moved off the excluded
     * millisecond; a number in the column's precision. Keyword and
     * boolean ranges stay unsupported, as in the pushdown.
     */
    private static RexNode rangeOf(Column column, RangeQueryBuilder range, String aggregationName, RelBuilder relBuilder) {
        if (column.isUtf8() || column.isBoolean()) {
            throw unsupported("range on column [" + column.name() + "] in filter of aggregation [" + aggregationName + "] is not numeric");
        }
        if (range.from() == null && range.to() == null) {
            throw unsupported("range without bounds in filter of aggregation [" + aggregationName + "]");
        }
        if (column.isDate()) {
            RexNode millis = epochMillis(column, relBuilder);
            RexNode lower = null;
            RexNode upper = null;
            if (range.from() != null) {
                long from = parseDate(range, range.from(), !range.includeLower(), aggregationName, column);
                lower = relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    millis,
                    relBuilder.literal(range.includeLower() ? from : from + 1L)
                );
            }
            if (range.to() != null) {
                long to = parseDate(range, range.to(), range.includeUpper(), aggregationName, column);
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
        if (column.isFloating()) {
            value = relBuilder.cast(relBuilder.field(column.index()), SqlTypeName.DOUBLE);
            if (range.from() != null) {
                Double from = floating(column, range.from());
                if (from == null) {
                    throw badValue(range.from(), column, aggregationName);
                }
                lowerBound = relBuilder.literal(from);
            }
            if (range.to() != null) {
                Double to = floating(column, range.to());
                if (to == null) {
                    throw badValue(range.to(), column, aggregationName);
                }
                upperBound = relBuilder.literal(to);
            }
        } else {
            value = relBuilder.cast(relBuilder.field(column.index()), SqlTypeName.BIGINT);
            if (range.from() != null) {
                Long from = integral(range.from());
                if (from == null) {
                    throw badValue(range.from(), column, aggregationName);
                }
                lowerBound = relBuilder.literal(from);
            }
            if (range.to() != null) {
                Long to = integral(range.to());
                if (to == null) {
                    throw badValue(range.to(), column, aggregationName);
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

    /** Epoch milliseconds of a date or timestamp column, through the timestamp cast a {@code DATE} column needs. */
    private static RexNode epochMillis(Column column, RelBuilder relBuilder) {
        RexNode reference = relBuilder.field(column.index());
        if (column.type() instanceof ArrowType.Date) {
            reference = relBuilder.cast(reference, SqlTypeName.TIMESTAMP);
        }
        return relBuilder.call(SqlLibraryOperators.UNIX_MILLIS, reference);
    }

    /**
     * Epoch millis of a date bound through the field's date format (the
     * range query's {@code format} and {@code time_zone} when it names
     * them), throwing when the text does not parse.
     */
    private static Long parseDate(RangeQueryBuilder range, Object value, boolean roundUp, String aggregationName, Column column) {
        try {
            String pattern = range == null ? null : range.format();
            ZoneId zone = range == null || range.timeZone() == null ? null : ZoneId.of(range.timeZone());
            DocValueFormat format = dateDocValueFormat(pattern, zone);
            return format.parseLong(text(value), roundUp, System::currentTimeMillis);
        } catch (RuntimeException unparseable) {
            throw badValue(value, column, aggregationName);
        }
    }

    private static UnsupportedOperationException badValue(Object value, Column column, String aggregationName) {
        return unsupported(
            "value [" + text(value) + "] on column [" + column.name() + "] in filter of aggregation [" + aggregationName + "]"
        );
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
    private static Double floating(Column column, Object value) {
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
        return column.isSingleFloat() ? (double) (float) number : number;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query.substrait;

import java.util.List;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Cast;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Expression;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Float64Literal;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.IfThen;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Int64Literal;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.ScalarFunction;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.ScalarType;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.StringLiteral;

/**
 * Expression shapes the aggregation pushdown needs on top of the raw
 * {@link SubstraitAggregatePlan} vocabulary. Arithmetic is spelled
 * with binary operators and casts only: the DataFusion build inside
 * Lance registers no math functions, so {@code floor} is not available
 * and floor semantics are derived from truncating division and a
 * comparison. Its datetime functions are registered, which is what
 * {@link #dateTrunc} relies on. Predicates are combined with the
 * boolean operators and {@code CASE} ({@link IfThen}), which the
 * consumer maps without any function lookup.
 */
public final class SubstraitExpressions {

    private SubstraitExpressions() {}

    /**
     * DataFusion's {@code date_trunc(unit, timestamp)}: the first
     * instant of the calendar unit ({@code second}, {@code minute},
     * {@code hour}, {@code day}, {@code week}, {@code month},
     * {@code quarter}, {@code year}) that contains the timestamp, as a
     * timestamp of the same Arrow unit. Weeks start on Monday and, for a
     * column without a time zone, the calendar is UTC, which is the
     * rounding {@code date_histogram} applies for a {@code calendar_interval}
     * without {@code time_zone}. The input has to be an Arrow
     * {@code Timestamp}: Lance builds the physical expression without
     * DataFusion's coercion pass, and the function itself rejects a
     * {@code Date32} array.
     */
    public static Expression dateTrunc(String unit, Expression timestamp) {
        return ScalarFunction.of("date_trunc", new StringLiteral(unit), timestamp);
    }

    /**
     * Epoch milliseconds of a date or timestamp column as an {@code i64},
     * converted the same way the fragment leaf reader converts the column
     * for doc values: {@code Date32} days are multiplied by 86 400 000,
     * seconds by 1000, micro and nanoseconds are divided with truncation
     * toward zero (Java {@code /} on {@code long}).
     */
    public static Expression epochMillis(Expression column, ArrowType type) {
        Expression asInt64 = new Cast(column, ScalarType.I64);
        if (type instanceof ArrowType.Date date) {
            if (date.getUnit() == DateUnit.DAY) {
                return ScalarFunction.of("multiply", asInt64, new Int64Literal(86_400_000L));
            }
            return asInt64;
        }
        if (type instanceof ArrowType.Timestamp timestamp) {
            return switch (timestamp.getUnit()) {
                case SECOND -> ScalarFunction.of("multiply", asInt64, new Int64Literal(1000L));
                case MILLISECOND -> asInt64;
                case MICROSECOND -> ScalarFunction.of("divide", asInt64, new Int64Literal(1000L));
                case NANOSECOND -> ScalarFunction.of("divide", asInt64, new Int64Literal(1_000_000L));
            };
        }
        throw new IllegalArgumentException("not a date or timestamp type: " + type);
    }

    /**
     * {@code Math.floorDiv(value - offset, interval)} on {@code i64}
     * values: the bucket ordinal of a fixed interval date histogram.
     * DataFusion's integer division truncates toward zero, so the
     * quotient is lowered by one when the remainder is negative:
     * {@code (x / n) - cast((x % n) < 0 as i64)}.
     */
    public static Expression floorDivInt64(Expression value, long offset, long interval) {
        Expression shifted = offset == 0L ? value : ScalarFunction.of("subtract", value, new Int64Literal(offset));
        Expression quotient = ScalarFunction.of("divide", shifted, new Int64Literal(interval));
        Expression remainder = ScalarFunction.of("modulus", shifted, new Int64Literal(interval));
        Expression negative = ScalarFunction.of("lt", remainder, new Int64Literal(0L));
        return ScalarFunction.of("subtract", quotient, new Cast(negative, ScalarType.I64));
    }

    /**
     * {@code Math.floor((value - offset) / interval)} on {@code fp64}
     * values as an {@code i64}: the bucket ordinal of a numeric
     * histogram, computed with the same double subtraction and division
     * the histogram aggregator performs. The quotient is truncated by a
     * cast to {@code i64} and lowered by one when the truncation moved
     * it up, that is when {@code cast(t as fp64) > q}.
     */
    public static Expression floorFp64(Expression value, double offset, double interval) {
        Expression asDouble = new Cast(value, ScalarType.FP64);
        Expression shifted = offset == 0d ? asDouble : ScalarFunction.of("subtract", asDouble, new Float64Literal(offset));
        Expression quotient = ScalarFunction.of("divide", shifted, new Float64Literal(interval));
        Expression truncated = new Cast(quotient, ScalarType.I64);
        Expression movedUp = ScalarFunction.of("gt", new Cast(truncated, ScalarType.FP64), quotient);
        return ScalarFunction.of("subtract", truncated, new Cast(movedUp, ScalarType.I64));
    }

    /** {@code value * value} as an {@code fp64}: the summand of a sum of squares. */
    public static Expression square(Expression value) {
        Expression asDouble = new Cast(value, ScalarType.FP64);
        return ScalarFunction.of("multiply", asDouble, asDouble);
    }

    /** {@code left AND right}. Null when either side is null and the other is not false, as in SQL. */
    public static Expression and(Expression left, Expression right) {
        return ScalarFunction.of("and", left, right);
    }

    /** {@code left OR right}. Null when either side is null and the other is not true, as in SQL. */
    public static Expression or(Expression left, Expression right) {
        return ScalarFunction.of("or", left, right);
    }

    /**
     * {@code NOT (condition IS TRUE)}: the negation a Lucene
     * {@code must_not} clause applies, under which a document without a
     * value for the field is not excluded. SQL's plain {@code NOT} of a
     * null condition is null, which a {@code CASE} would treat as not
     * matched, so the condition is first collapsed to a two valued one.
     */
    public static Expression notTrue(Expression condition) {
        return ScalarFunction.of("not", ScalarFunction.of("is_true", condition));
    }

    /** {@code column IS NULL}. */
    public static Expression isNull(Expression column) {
        return ScalarFunction.of("is_null", column);
    }

    /** {@code column IS NOT NULL}. */
    public static Expression isNotNull(Expression column) {
        return ScalarFunction.of("is_not_null", column);
    }

    /**
     * The bit mask of the conditions that hold: {@code CASE WHEN c0 THEN
     * 1 ELSE 0 END + CASE WHEN c1 THEN 2 ELSE 0 END + ...} as an
     * {@code i64}. A grouping on it yields one row per combination of
     * matching conditions, which lets the caller count a row toward
     * every range or filter it belongs to when they overlap, unlike a
     * single {@code CASE} that would pick the first match only. Never
     * null: a null condition contributes 0. At most 62 conditions fit
     * the sign bit free part of the mask.
     */
    public static Expression matchMask(List<Expression> conditions) {
        if (conditions.isEmpty() || conditions.size() > MAX_MASK_CONDITIONS) {
            throw new IllegalArgumentException("a match mask takes 1 to " + MAX_MASK_CONDITIONS + " conditions, not " + conditions.size());
        }
        Expression mask = null;
        for (int i = 0; i < conditions.size(); i++) {
            Expression bit = new IfThen(List.of(new IfThen.Branch(conditions.get(i), new Int64Literal(1L << i))), new Int64Literal(0L));
            mask = mask == null ? bit : ScalarFunction.of("add", mask, bit);
        }
        return mask;
    }

    /** Largest number of conditions {@link #matchMask} encodes. */
    public static final int MAX_MASK_CONDITIONS = 62;
}

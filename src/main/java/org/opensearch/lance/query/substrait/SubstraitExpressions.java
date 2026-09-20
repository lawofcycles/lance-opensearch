/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query.substrait;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Cast;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Expression;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Float64Literal;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Int64Literal;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.ScalarFunction;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.ScalarType;

/**
 * Expression shapes the aggregation pushdown needs on top of the raw
 * {@link SubstraitAggregatePlan} vocabulary. Everything here is spelled
 * with binary operators and casts only: the DataFusion build inside
 * Lance registers no math functions, so {@code floor} is not available
 * and floor semantics are derived from truncating division and a
 * comparison.
 */
public final class SubstraitExpressions {

    private SubstraitExpressions() {}

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
}

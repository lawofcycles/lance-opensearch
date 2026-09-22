/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Adds one metric aggregation ({@code sum}, {@code avg}, {@code min},
 * {@code max}, {@code value_count}) to a {@link RelBuilder} whose top of
 * stack is the scan of the aggregated index. The aggregation's
 * OpenSearch name becomes the output column alias, so the plan reads
 * {@code s=[SUM($1)]} for {@code "aggs": {"s": {"sum": ...}}}.
 *
 * <p>The aggregation field resolves to a Lance column with the rules the
 * aggregation pushdown applies: the field names a top level column, or a
 * keyword sub-field declared in {@code index.lance.multi_fields}
 * resolves to its base column. The column must be numeric in the sense
 * the pushdown gives the word: a signed integer, a single or double
 * precision float, or a date / timestamp column (which the plugin reads
 * as epoch milliseconds). Everything else throws
 * {@link UnsupportedOperationException} naming the offending element,
 * which the explain endpoint reports as a 400.
 */
final class AggregationToRel {

    private AggregationToRel() {}

    /**
     * Places the aggregate over the builder's current top node via
     * {@code aggregate(groupKey(), call)}, producing a
     * {@code LogicalAggregate} with no group keys.
     *
     * @param builder one of the five supported metric builders; the
     *     caller has already rejected other aggregation types
     * @param schema Arrow schema of the scanned table
     * @param multiFields base column to declared sub-fields, nullable
     * @param relBuilder builder whose top of stack is the table scan
     */
    static void metric(
        ValuesSourceAggregationBuilder<?> builder,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        RelBuilder relBuilder
    ) {
        if (builder.script() != null) {
            throw new UnsupportedOperationException("script on aggregation [" + builder.getName() + "]");
        }
        if (builder.missing() != null) {
            throw new UnsupportedOperationException("missing on aggregation [" + builder.getName() + "]");
        }
        String field = builder.field();
        if (field == null) {
            throw new UnsupportedOperationException("aggregation [" + builder.getName() + "] without a field");
        }
        String column = resolveColumn(field, schema, multiFields);
        ArrowType type = columnType(schema, column);
        if (!isNumeric(type)) {
            throw new UnsupportedOperationException("column [" + column + "] behind aggregation field [" + field + "] is not numeric");
        }
        relBuilder.aggregate(relBuilder.groupKey(), call(builder, relBuilder, column));
    }

    private static RelBuilder.AggCall call(ValuesSourceAggregationBuilder<?> builder, RelBuilder relBuilder, String column) {
        String alias = builder.getName();
        if (builder instanceof SumAggregationBuilder) {
            return relBuilder.sum(false, alias, relBuilder.field(column));
        }
        if (builder instanceof AvgAggregationBuilder) {
            return relBuilder.avg(false, alias, relBuilder.field(column));
        }
        if (builder instanceof MinAggregationBuilder) {
            return relBuilder.min(alias, relBuilder.field(column));
        }
        if (builder instanceof MaxAggregationBuilder) {
            return relBuilder.max(alias, relBuilder.field(column));
        }
        if (builder instanceof ValueCountAggregationBuilder) {
            return relBuilder.count(false, alias, relBuilder.field(column));
        }
        // The caller dispatches on the same five classes; reaching this
        // is a programming error, not a request shape.
        throw new IllegalStateException("no aggregate call for aggregation type [" + builder.getType() + "]");
    }

    /**
     * The Lance column behind an aggregation field: the field itself
     * when it names a top level column, else the base column of a
     * keyword sub-field the attach declared.
     *
     * @throws UnsupportedOperationException naming the field when
     *     neither rule applies
     */
    private static String resolveColumn(String field, Schema schema, Map<String, LinkedHashMap<String, String>> multiFields) {
        if (indexOf(schema, field) >= 0) {
            return field;
        }
        int dot = field.lastIndexOf('.');
        if (dot > 0 && multiFields != null) {
            String base = field.substring(0, dot);
            String sub = field.substring(dot + 1);
            LinkedHashMap<String, String> subs = multiFields.get(base);
            if (subs != null && "keyword".equals(subs.get(sub)) && indexOf(schema, base) >= 0) {
                return base;
            }
        }
        throw new UnsupportedOperationException("field [" + field + "] does not map to a Lance column");
    }

    private static ArrowType columnType(Schema schema, String column) {
        return schema.getFields().get(indexOf(schema, column)).getType();
    }

    private static boolean isNumeric(ArrowType type) {
        if (type instanceof ArrowType.Int intType) {
            return intType.getIsSigned();
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE || fp.getPrecision() == FloatingPointPrecision.DOUBLE;
        }
        return type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp;
    }

    private static int indexOf(Schema schema, String column) {
        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(column)) {
                return i;
            }
        }
        return -1;
    }
}

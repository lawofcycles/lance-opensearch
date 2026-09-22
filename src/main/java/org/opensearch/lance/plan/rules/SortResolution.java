/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import org.lance.ipc.ColumnOrdering;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

/**
 * Resolves a request's sort between its three spellings: the
 * OpenSearch {@link SortBuilder} list of the request, the
 * {@link RelFieldCollation}s of the plan's
 * {@link org.opensearch.lance.plan.rel.LanceTopK} node, and the Lance
 * {@link ColumnOrdering}s the executor hands to
 * {@code ScanOptions.setColumnOrderings}.
 *
 * <p>{@link #collationsOf} maps the request's sort clauses onto a row
 * type: a plain field sort resolves to its column (a keyword sub-field
 * declared in the attach's multi fields to its Utf8 base column, whose
 * ordinals also serve the sub-field on the Lucene side), a
 * {@code _score} sort to the {@code _score} column a
 * {@link LanceFtsMatch} adds (descending) or the {@code _distance}
 * column a {@link LanceKnnSearch} adds (ascending, smaller distance is
 * the larger score), {@code missing} {@code _first} / {@code _last}
 * to the null direction. Everything without a collation spelling
 * throws {@link UnsupportedOperationException} naming the element
 * (a geo distance or script sort, a nested sort, a sort {@code mode},
 * a {@code numeric_type} cast, a literal {@code missing} value), so
 * the explain endpoint reports it and the executor keeps the shape on
 * the Lucene collector.
 *
 * <p>{@link #toOrderings} turns collations back into Lance orderings
 * for the pushdown rule, checking each column's Arrow type against
 * what Lance / DataFusion can order by, and {@link #cursorPredicate}
 * spells a {@code search_after} cursor as the strict bound predicate
 * the scan ANDs into its filter. Both return empty instead of throwing:
 * the rule passes and the plan keeps the
 * {@link org.opensearch.lance.plan.rel.LanceTopK} for the Lucene side.
 */
public final class SortResolution {

    private SortResolution() {}

    /**
     * The collations of the request's sort clauses over {@code rowType}
     * (the row type of the plan node the top-k sits on).
     *
     * @throws UnsupportedOperationException naming the first sort
     *     element without a collation spelling
     */
    public static List<RelFieldCollation> collationsOf(List<SortBuilder<?>> sorts, RelDataType rowType, LanceSchemas.IndexModel model) {
        List<RelFieldCollation> collations = new ArrayList<>(sorts.size());
        for (SortBuilder<?> sort : sorts) {
            collations.add(collationOf(sort, rowType, model));
        }
        return collations;
    }

    private static RelFieldCollation collationOf(SortBuilder<?> sort, RelDataType rowType, LanceSchemas.IndexModel model) {
        if (sort instanceof ScoreSortBuilder score) {
            if (score.order() == SortOrder.ASC) {
                throw unsupported("sort by [_score] ascending");
            }
            int scoreIndex = rowType.getFieldNames().indexOf(LanceFtsMatch.SCORE_FIELD);
            if (scoreIndex >= 0) {
                return new RelFieldCollation(scoreIndex, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST);
            }
            int distanceIndex = rowType.getFieldNames().indexOf(LanceKnnSearch.DISTANCE_FIELD);
            if (distanceIndex >= 0) {
                return new RelFieldCollation(distanceIndex, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST);
            }
            throw unsupported("sort by [_score] without a full text or knn query");
        }
        if (!(sort instanceof FieldSortBuilder field)) {
            throw unsupported("sort type [" + sort.getWriteableName() + "]");
        }
        String name = field.getFieldName();
        if (field.getNestedSort() != null) {
            throw unsupported("sort on field [" + name + "] with a nested sort");
        }
        if (field.sortMode() != null) {
            throw unsupported("sort on field [" + name + "] with mode [" + field.sortMode() + "]");
        }
        if (field.getNumericType() != null) {
            throw unsupported("sort on field [" + name + "] with numeric_type [" + field.getNumericType() + "]");
        }
        RelFieldCollation.NullDirection nullDirection;
        Object missing = field.missing();
        if (missing == null || "_last".equals(missing)) {
            nullDirection = RelFieldCollation.NullDirection.LAST;
        } else if ("_first".equals(missing)) {
            nullDirection = RelFieldCollation.NullDirection.FIRST;
        } else {
            throw unsupported("sort on field [" + name + "] with missing [" + missing + "]");
        }
        if (name.startsWith("_")) {
            // _doc, _id and the other metadata sorts have no Lance
            // column behind them.
            throw unsupported("sort on metadata field [" + name + "]");
        }
        int index = fieldIndex(name, rowType, model);
        RelFieldCollation.Direction direction = field.order() == SortOrder.DESC
            ? RelFieldCollation.Direction.DESCENDING
            : RelFieldCollation.Direction.ASCENDING;
        return new RelFieldCollation(index, direction, nullDirection);
    }

    /**
     * The row type index of a sort field: the column itself, or the
     * Utf8 base column of a keyword sub-field declared in the multi
     * fields spec. The messages match the query translator's field
     * resolution.
     */
    private static int fieldIndex(String name, RelDataType rowType, LanceSchemas.IndexModel model) {
        List<String> fieldNames = rowType.getFieldNames();
        int index = fieldNames.indexOf(name);
        if (index >= 0) {
            return index;
        }
        int dot = name.lastIndexOf('.');
        if (dot > 0 && model.multiFields() != null) {
            String base = name.substring(0, dot);
            String sub = name.substring(dot + 1);
            LinkedHashMap<String, String> subs = model.multiFields().get(base);
            if (subs != null && "keyword".equals(subs.get(sub))) {
                int baseIndex = fieldNames.indexOf(base);
                if (baseIndex >= 0
                    && topLevelField(model.arrowSchema(), base) instanceof Field baseField
                    && baseField.getType() instanceof ArrowType.Utf8) {
                    return baseIndex;
                }
            }
        }
        String renamedTo = model.renamedFields() == null ? null : model.renamedFields().get(name);
        if (renamedTo != null) {
            throw unsupported("sort field [" + name + "] was renamed to [" + renamedTo + "] in the Lance table");
        }
        throw unsupported("sort field [" + name + "] does not map to a Lance column");
    }

    /**
     * Whether the collation is the score order a pushed FTS or knn
     * scan produces by itself: {@code _score} descending or
     * {@code _distance} ascending over {@code rowType}.
     */
    public static boolean isScoreCollation(RelDataType rowType, RelFieldCollation collation) {
        String name = rowType.getFieldNames().get(collation.getFieldIndex());
        if (LanceFtsMatch.SCORE_FIELD.equals(name)) {
            return collation.getDirection() == RelFieldCollation.Direction.DESCENDING;
        }
        if (LanceKnnSearch.DISTANCE_FIELD.equals(name)) {
            return collation.getDirection() == RelFieldCollation.Direction.ASCENDING;
        }
        return false;
    }

    /**
     * The Lance orderings of the collations over {@code rowType},
     * or empty when a column's Arrow type is not one Lance /
     * DataFusion can order by (the rule then passes and the Lucene
     * collector serves the sort through doc values). Multi-valued
     * (list) columns and struct children are not orderable here; the
     * ip and geo_point override columns are excluded by the callers,
     * which see the mapping.
     */
    public static Optional<List<ColumnOrdering>> toOrderings(RelDataType rowType, List<RelFieldCollation> collations, Schema arrowSchema) {
        List<ColumnOrdering> orderings = new ArrayList<>(collations.size());
        for (RelFieldCollation collation : collations) {
            String name = rowType.getFieldNames().get(collation.getFieldIndex());
            Field field = topLevelField(arrowSchema, name);
            if (field == null || !orderable(field.getType())) {
                return Optional.empty();
            }
            ColumnOrdering.Builder builder = new ColumnOrdering.Builder();
            builder.setColumnName(name);
            builder.setAscending(collation.getDirection() != RelFieldCollation.Direction.DESCENDING);
            builder.setNullFirst(collation.nullDirection == RelFieldCollation.NullDirection.FIRST);
            orderings.add(builder.build());
        }
        return Optional.of(orderings);
    }

    /**
     * The strict bound a {@code search_after} continuation adds to the
     * scan filter: rows after the cursor in the collation's order.
     * The executor's Lucene cursor pins the tie-break doc past the
     * reader, so a tied row never passes there; the strict comparison
     * reproduces that. With nulls last the rows after any non-null
     * cursor include the null rows, so the bound ORs {@code IS NULL};
     * with nulls first the null rows came before the cursor and the
     * bare comparison stands. Returns empty for a cursor value the
     * column's type cannot compare this way (a non-number on a numeric
     * column, a boolean column, a NaN), leaving the shape on the
     * Lucene collector.
     */
    public static Optional<RexNode> cursorPredicate(
        RexBuilder rexBuilder,
        RelDataType rowType,
        RelFieldCollation collation,
        Schema arrowSchema,
        Object cursor
    ) {
        String name = rowType.getFieldNames().get(collation.getFieldIndex());
        Field field = topLevelField(arrowSchema, name);
        if (field == null) {
            return Optional.empty();
        }
        boolean ascending = collation.getDirection() != RelFieldCollation.Direction.DESCENDING;
        RexNode ref = rexBuilder.makeInputRef(rowType.getFieldList().get(collation.getFieldIndex()).getType(), collation.getFieldIndex());
        ArrowType type = field.getType();
        RexNode value;
        RexNode bound;
        if (type instanceof ArrowType.Utf8) {
            if (!(cursor instanceof String text)) {
                return Optional.empty();
            }
            value = ref;
            bound = rexBuilder.makeLiteral(text);
        } else if (type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp) {
            if (!(cursor instanceof Number number)) {
                return Optional.empty();
            }
            RexNode reference = type instanceof ArrowType.Date
                ? rexBuilder.makeCast(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP), ref)
                : ref;
            value = rexBuilder.makeCall(SqlLibraryOperators.UNIX_MILLIS, reference);
            bound = rexBuilder.makeExactLiteral(
                BigDecimal.valueOf(number.longValue()),
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)
            );
        } else if (type instanceof ArrowType.Int intType && intType.getIsSigned()) {
            if (!(cursor instanceof Number number)) {
                return Optional.empty();
            }
            value = rexBuilder.makeCast(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT), ref);
            bound = rexBuilder.makeExactLiteral(
                BigDecimal.valueOf(number.longValue()),
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)
            );
        } else if (type instanceof ArrowType.FloatingPoint fp
            && (fp.getPrecision() == FloatingPointPrecision.SINGLE || fp.getPrecision() == FloatingPointPrecision.DOUBLE)) {
                if (!(cursor instanceof Number number)) {
                    return Optional.empty();
                }
                // The column compares in its own precision, as the range
                // translator does for float columns.
                double bare = number.doubleValue();
                double narrowed = fp.getPrecision() == FloatingPointPrecision.SINGLE ? (double) (float) bare : bare;
                if (!Double.isFinite(narrowed)) {
                    return Optional.empty();
                }
                value = rexBuilder.makeCast(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), ref);
                bound = rexBuilder.makeApproxLiteral(
                    BigDecimal.valueOf(narrowed),
                    rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE)
                );
            } else {
                // Booleans have no strict order literal the Lucene
                // comparator agrees with, and every other type is not
                // orderable to begin with.
                return Optional.empty();
            }
        RexNode comparison = rexBuilder.makeCall(
            ascending ? SqlStdOperatorTable.GREATER_THAN : SqlStdOperatorTable.LESS_THAN,
            value,
            bound
        );
        if (collation.nullDirection != RelFieldCollation.NullDirection.FIRST) {
            return Optional.of(
                rexBuilder.makeCall(SqlStdOperatorTable.OR, comparison, rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ref))
            );
        }
        return Optional.of(comparison);
    }

    /** The Arrow types Lance / DataFusion order by: the scalar set the predicate translator compares. */
    private static boolean orderable(ArrowType type) {
        if (type instanceof ArrowType.Int intType) {
            return intType.getIsSigned();
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE || fp.getPrecision() == FloatingPointPrecision.DOUBLE;
        }
        return type instanceof ArrowType.Bool
            || type instanceof ArrowType.Date
            || type instanceof ArrowType.Timestamp
            || type instanceof ArrowType.Utf8;
    }

    private static Field topLevelField(Schema schema, String name) {
        for (Field candidate : schema.getFields()) {
            if (candidate.getName().equals(name)) {
                return candidate;
            }
        }
        return null;
    }

    private static UnsupportedOperationException unsupported(String element) {
        return new UnsupportedOperationException(element);
    }
}

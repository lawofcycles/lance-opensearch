/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Prints a {@link RexNode} predicate as the DataFusion SQL expression
 * Lance's {@code filter(sql)} accepts. This is the single source of the
 * SQL spelling; each construct is pinned by a unit test:
 *
 * <ul>
 *   <li>identifiers bare (Lance's filter parser reads a double quoted
 *       token as a string literal, so quoting is not available), a
 *       struct child as {@code parent.child}</li>
 *   <li>string literals single quoted with embedded quotes doubled
 *       (DataFusion's dialect has no backslash escapes in literals)</li>
 *   <li>{@code =} / {@code <>} / {@code <} / {@code <=} / {@code >} /
 *       {@code >=}; a comparison of {@code UNIX_MILLIS(col)} against an
 *       integer literal prints as the column against
 *       {@code to_timestamp_millis(millis)}, the timestamp form the SQL
 *       translator emits for date bounds today</li>
 *   <li>{@code AND} / {@code OR} / {@code NOT} with parentheses always;
 *       nested conjunctions and disjunctions of the same kind flatten
 *       into one parenthesised group</li>
 *   <li>an {@code OR} of equalities on one column collapses to
 *       {@code IN (...)}</li>
 *   <li>{@code IS NULL} / {@code IS NOT NULL}; {@code (x) IS TRUE} under
 *       {@code NOT}, which keeps rows with no value in a
 *       {@code must_not}</li>
 *   <li>{@code LIKE} / {@code ILIKE} with {@code ESCAPE '\'} for
 *       wildcard patterns, {@code regexp_like(col, '^(?:...)$')} for the
 *       anchored regexp, {@code starts_with(col, '...')} and
 *       {@code lower(...)} for prefix</li>
 *   <li>boolean literals {@code true} / {@code false}, numeric literals
 *       in plain notation</li>
 * </ul>
 *
 * <p>Casts that only widen the planning type (an integer column
 * compared as {@code BIGINT}, a float column as {@code DOUBLE}, the
 * date column's timestamp cast under {@code UNIX_MILLIS}) print as the
 * bare column: DataFusion coerces comparison operands itself.
 *
 * <p>{@link #print} returns empty when the tree contains any call
 * outside this list, so callers leave the {@code Filter} in place for
 * the Lucene side instead of handing Lance SQL it cannot run.
 */
public final class RexToLanceSql extends RexVisitorImpl<String> {

    private final List<String> fieldNames;

    private RexToLanceSql(RelDataType inputRowType) {
        super(false);
        this.fieldNames = inputRowType.getFieldNames();
    }

    /**
     * The SQL of {@code predicate} over a scan of {@code inputRowType},
     * or empty when some construct in the tree has no SQL spelling.
     */
    public static Optional<String> print(RexNode predicate, RelDataType inputRowType) {
        try {
            return Optional.of(new RexToLanceSql(inputRowType).sql(predicate));
        } catch (Unprintable unprintable) {
            return Optional.empty();
        }
    }

    /** Thrown internally for any construct outside the printable list; never escapes {@link #print}. */
    private static final class Unprintable extends RuntimeException {
        Unprintable() {
            super(null, null, false, false);
        }
    }

    private String sql(RexNode node) {
        String printed = node.accept(this);
        if (printed == null) {
            throw new Unprintable();
        }
        return printed;
    }

    @Override
    public String visitInputRef(RexInputRef inputRef) {
        if (inputRef.getIndex() >= fieldNames.size()) {
            throw new Unprintable();
        }
        return identifier(fieldNames.get(inputRef.getIndex()));
    }

    @Override
    public String visitFieldAccess(RexFieldAccess fieldAccess) {
        RexNode reference = fieldAccess.getReferenceExpr();
        if (!(reference instanceof RexInputRef) && !(reference instanceof RexFieldAccess)) {
            throw new Unprintable();
        }
        return sql(reference) + "." + identifier(fieldAccess.getField().getName());
    }

    @Override
    public String visitLiteral(RexLiteral literal) {
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (typeName == SqlTypeName.BOOLEAN) {
            Boolean value = literal.getValueAs(Boolean.class);
            if (value == null) {
                throw new Unprintable();
            }
            return value.toString();
        }
        if (typeName.getFamily() == SqlTypeFamily.CHARACTER) {
            String value = literal.getValueAs(String.class);
            if (value == null) {
                throw new Unprintable();
            }
            return stringLiteral(value);
        }
        if (typeName.getFamily() == SqlTypeFamily.NUMERIC) {
            BigDecimal value = literal.getValueAs(BigDecimal.class);
            if (value == null) {
                throw new Unprintable();
            }
            return value.toPlainString();
        }
        throw new Unprintable();
    }

    @Override
    public String visitCall(RexCall call) {
        switch (call.getKind()) {
            case AND:
                return group(flatten(call, SqlKind.AND), " AND ");
            case OR:
                return disjunction(call);
            case NOT:
                return "NOT (" + isTrueOrPlain(call.getOperands().get(0)) + ")";
            case IS_TRUE:
                return "(" + sql(call.getOperands().get(0)) + ") IS TRUE";
            case IS_NULL:
                return sql(call.getOperands().get(0)) + " IS NULL";
            case IS_NOT_NULL:
                return sql(call.getOperands().get(0)) + " IS NOT NULL";
            case EQUALS:
                return comparison(call, " = ");
            case NOT_EQUALS:
                return comparison(call, " <> ");
            case LESS_THAN:
                return comparison(call, " < ");
            case LESS_THAN_OR_EQUAL:
                return comparison(call, " <= ");
            case GREATER_THAN:
                return comparison(call, " > ");
            case GREATER_THAN_OR_EQUAL:
                return comparison(call, " >= ");
            case LIKE:
                return like(call);
            case CAST:
                // The translator only casts to widen the planning type;
                // DataFusion coerces comparison operands itself, so the
                // cast prints as its operand.
                return sql(call.getOperands().get(0));
            case OTHER_FUNCTION:
            case OTHER:
                return function(call);
            case STARTS_WITH:
                return "starts_with(" + sql(call.getOperands().get(0)) + ", " + sql(call.getOperands().get(1)) + ")";
            case RLIKE:
                return "regexp_like(" + sql(call.getOperands().get(0)) + ", " + sql(call.getOperands().get(1)) + ")";
            default:
                throw new Unprintable();
        }
    }

    /**
     * {@code NOT}'s operand: the {@code (x) IS TRUE} the translator
     * wraps a negated clause in prints without a second layer of
     * parentheses because {@code NOT (...)} already carries one.
     */
    private String isTrueOrPlain(RexNode operand) {
        if (operand.getKind() == SqlKind.IS_TRUE) {
            return "(" + sql(((RexCall) operand).getOperands().get(0)) + ") IS TRUE";
        }
        return sql(operand);
    }

    /** An {@code OR}: flattened, and collapsed to {@code IN (...)} when every branch equates the same column to a literal. */
    private String disjunction(RexCall call) {
        List<RexNode> branches = flatten(call, SqlKind.OR);
        String inList = asInList(branches);
        return inList != null ? inList : group(branches, " OR ");
    }

    private String asInList(List<RexNode> branches) {
        if (branches.size() < 2) {
            return null;
        }
        String column = null;
        List<String> values = new ArrayList<>(branches.size());
        for (RexNode branch : branches) {
            if (branch.getKind() != SqlKind.EQUALS) {
                return null;
            }
            RexCall equals = (RexCall) branch;
            RexNode left = stripCasts(equals.getOperands().get(0));
            RexNode right = stripCasts(equals.getOperands().get(1));
            if (!(right instanceof RexLiteral) || left instanceof RexLiteral) {
                return null;
            }
            if (left.getKind() == SqlKind.OTHER_FUNCTION || left.getKind() == SqlKind.OTHER) {
                return null;
            }
            String reference = sql(left);
            if (column == null) {
                column = reference;
            } else if (!column.equals(reference)) {
                return null;
            }
            values.add(sql(right));
        }
        return column + " IN (" + String.join(", ", values) + ")";
    }

    private String group(List<RexNode> operands, String separator) {
        List<String> printed = new ArrayList<>(operands.size());
        for (RexNode operand : operands) {
            printed.add(sql(operand));
        }
        return "(" + String.join(separator, printed) + ")";
    }

    private static List<RexNode> flatten(RexCall call, SqlKind kind) {
        List<RexNode> flat = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            if (operand.getKind() == kind) {
                flat.addAll(flatten((RexCall) operand, kind));
            } else {
                flat.add(operand);
            }
        }
        return flat;
    }

    /**
     * A comparison, with the one rewrite the printer applies: the
     * epoch-millis form {@code UNIX_MILLIS(col) op millis} the
     * translator builds for date bounds prints as
     * {@code col op to_timestamp_millis(millis)}, the literal shape
     * DataFusion compares against a timestamp column.
     */
    private String comparison(RexCall call, String operator) {
        RexNode left = stripCasts(call.getOperands().get(0));
        RexNode right = stripCasts(call.getOperands().get(1));
        if (left instanceof RexCall function && isUnixMillis(function)) {
            if (!(right instanceof RexLiteral literal) || literal.getType().getSqlTypeName().getFamily() != SqlTypeFamily.NUMERIC) {
                throw new Unprintable();
            }
            RexNode column = stripCasts(function.getOperands().get(0));
            return sql(column) + operator + "to_timestamp_millis(" + sql(right) + ")";
        }
        return sql(left) + operator + sql(right);
    }

    private static boolean isUnixMillis(RexCall call) {
        return call.getOperands().size() == 1 && call.getOperator().getName().equals("UNIX_MILLIS");
    }

    private static RexNode stripCasts(RexNode node) {
        while (node.getKind() == SqlKind.CAST) {
            node = ((RexCall) node).getOperands().get(0);
        }
        return node;
    }

    /** {@code LIKE} / {@code ILIKE} with the backslash escape clause when the call carries the escape operand. */
    private String like(RexCall call) {
        String operator = call.getOperator().getName().equals("ILIKE") ? " ILIKE " : " LIKE ";
        String printed = sql(call.getOperands().get(0)) + operator + sql(call.getOperands().get(1));
        if (call.getOperands().size() == 3) {
            printed += " ESCAPE " + sql(call.getOperands().get(2));
        }
        return printed;
    }

    /** The named functions with a lower case DataFusion spelling; anything else refuses. */
    private String function(RexCall call) {
        String name = call.getOperator().getName();
        if (name.equals("LOWER") && call.getOperands().size() == 1) {
            return "lower(" + sql(call.getOperands().get(0)) + ")";
        }
        throw new Unprintable();
    }

    /**
     * Bare identifier, as the translator this printer replaces emits.
     * Lance's filter parser reads a double quoted token as a string
     * literal, not as a quoted identifier (a quoted column name
     * compares as a constant and a quoted struct path is refused), so
     * quoting is not available; a column whose name collides with a SQL
     * keyword or contains a dot cannot be addressed, exactly as on the
     * old path.
     */
    private static String identifier(String name) {
        return name;
    }

    /**
     * Single quoted SQL string literal with the embedded single quotes
     * doubled, the escape DataFusion's parser accepts. Backslashes are
     * left alone: Lance parses filters with a dialect that has no
     * backslash escapes inside string literals.
     */
    private static String stringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}

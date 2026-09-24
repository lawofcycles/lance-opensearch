/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.extendedexpression.ExtendedExpression;
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extendedexpression.ImmutableExpressionReference;
import io.substrait.extendedexpression.ImmutableExtendedExpression;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Produces the Substrait bytes Lance's
 * {@code ScanOptions.Builder#substraitFilter} accepts from a Calcite
 * predicate over a Lance table scan: an {@code ExtendedExpression}
 * message whose base schema is the scan's row type (the Arrow field
 * order of the dataset, which the field references index by position)
 * and whose single referred expression is the boolean predicate. Lance
 * decodes the message with DataFusion's Substrait consumer, checks the
 * base schema against the dataset schema field by field, and plans the
 * resulting expression exactly as it plans the SQL filter of
 * {@code ScanOptions.Builder#filter}: the same coercion and
 * simplification passes run and the same scalar indexes apply, so the
 * two encodings of one predicate evaluate identically. The producer's
 * job is the encoding, not the evaluation.
 *
 * <p>The Calcite side of the conversion is isthmus's
 * {@link RexExpressionConverter} for field references, literals, casts
 * and {@code CASE}, with a converter of this class in front of it for
 * every call the predicate translator emits: the boolean connectives
 * and comparisons are spelled directly (same kind conjunctions and
 * disjunctions flattened into one n-ary call, a disjunction of
 * equalities on one column collapsed to an {@code IN} list, the shape
 * the SQL printer produces as well), a comparison of the translator's
 * {@code UNIX_MILLIS(col)} against an epoch millis literal becomes the
 * column against a millisecond precision timestamp literal (the same
 * literal shape the SQL printer spells as
 * {@code to_timestamp_millis}), and {@code LIKE} / {@code ILIKE} with
 * their escape operand, the anchored {@code regexp_like}, {@code
 * starts_with}, {@code lower} and {@code IS TRUE} are emitted under the
 * names DataFusion's consumer resolves. A struct child reference
 * ({@code parent.child}) makes the producer return empty: Lance's
 * consumer refuses nested field references in a filter, while the SQL
 * printer addresses the child as a dotted path.
 *
 * <p>{@link #toLanceFilter} returns empty for any construct outside
 * this vocabulary, so the caller keeps the SQL spelling (or leaves the
 * {@code Filter} in place) instead of handing Lance bytes it cannot
 * run.
 */
public final class LanceSubstraitFilterProducer {

    private LanceSubstraitFilterProducer() {}

    /** Output name of the single expression the message carries. */
    static final String OUTPUT_NAME = "filter";

    /**
     * Scalar function names the producer may emit: the names DataFusion's
     * consumer maps onto its operators, built in expressions or the UDFs
     * Lance's session registers. Anything else in a converted expression
     * makes the producer return empty.
     */
    private static final Set<String> ACCEPTED_SCALARS = Set.of(
        "and",
        "or",
        "not",
        "equal",
        "not_equal",
        "lt",
        "lte",
        "gt",
        "gte",
        "is_null",
        "is_not_null",
        "is_true",
        "like",
        "ilike",
        "regexp_like",
        "starts_with",
        "lower",
        "add",
        "subtract",
        "multiply",
        "divide",
        "modulus"
    );

    /**
     * Encodes {@code condition}, a boolean predicate over a scan of
     * {@code rowType}, as the {@code ExtendedExpression} Lance's
     * {@code substraitFilter} takes. Empty when the predicate uses a
     * construct outside the vocabulary above, references a struct child,
     * or is not boolean.
     *
     * @param condition the predicate, over the fields of {@code rowType}
     * @param rowType the scan's row type, in the dataset's field order
     * @param typeFactory the planner's type factory, for isthmus's
     *     function signature matching
     */
    public static Optional<ByteBuffer> toLanceFilter(RexNode condition, RelDataType rowType, RelDataTypeFactory typeFactory) {
        if (condition.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
            return Optional.empty();
        }
        try {
            NamedStruct baseSchema = TypeConverter.DEFAULT.toNamedStruct(rowType);
            Expression predicate = accepted(condition.accept(converter(typeFactory)));
            if (!(predicate.getType() instanceof Type.Bool)) {
                return Optional.empty();
            }
            ExtendedExpression message = ImmutableExtendedExpression.builder()
                .baseSchema(baseSchema)
                .addReferredExpressions(ImmutableExpressionReference.builder().expression(predicate).addOutputNames(OUTPUT_NAME).build())
                .build();
            return Optional.of(serialize(message));
        } catch (UnsupportedOperationException | IllegalArgumentException | IllegalStateException outsideTheConsumer) {
            return Optional.empty();
        }
    }

    /**
     * The message as protobuf bytes in a direct buffer: the JNI side
     * reads the buffer through {@code GetDirectBufferAddress}, which
     * returns null for a heap buffer.
     */
    private static ByteBuffer serialize(ExtendedExpression message) {
        byte[] bytes = new ExtendedExpressionProtoConverter().toProto(message).toByteArray();
        ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
        direct.put(bytes);
        direct.flip();
        return direct;
    }

    // ---------------------------------------------------------------
    // Expression conversion
    // ---------------------------------------------------------------

    /**
     * The isthmus expression converter with the filter converter in
     * front of the defaults, so every call the translator emits is
     * spelled by this class and isthmus only converts field references,
     * literals, casts and {@code CASE}.
     */
    private static RexExpressionConverter converter(RelDataTypeFactory typeFactory) {
        List<CallConverter> converters = new ArrayList<>();
        converters.add(new FilterCallConverter());
        converters.addAll(CallConverters.defaults(TypeConverter.DEFAULT));
        converters.add(new ScalarFunctionConverter(LanceSubstraitExtensions.COLLECTION.scalarFunctions(), typeFactory));
        converters.add(CallConverters.CREATE_SEARCH_CONV.apply(new RexBuilder(typeFactory)));
        WindowFunctionConverter windows = new WindowFunctionConverter(LanceSubstraitExtensions.COLLECTION.windowFunctions(), typeFactory);
        return new RexExpressionConverter(null, converters, windows, TypeConverter.DEFAULT);
    }

    /**
     * Spells every call of the predicate translator's vocabulary
     * directly, so no call depends on isthmus's signature matching
     * (which refuses a {@code LIKE} with an escape operand, an
     * {@code ILIKE}, a {@code RLIKE} and mixed width comparison
     * operands) and every emitted name is one DataFusion's consumer
     * resolves.
     */
    private static final class FilterCallConverter implements CallConverter {

        @Override
        public Optional<Expression> convert(RexCall call, Function<RexNode, Expression> converter) {
            switch (call.getKind()) {
                case AND:
                    return Optional.of(scalar("and", TypeCreator.NULLABLE.BOOLEAN, flattened(call, SqlKind.AND, converter)));
                case OR:
                    return Optional.of(disjunction(call, converter));
                case NOT:
                    return Optional.of(scalar("not", TypeCreator.NULLABLE.BOOLEAN, converter.apply(call.getOperands().get(0))));
                case IS_TRUE:
                    return Optional.of(scalar("is_true", TypeCreator.NULLABLE.BOOLEAN, converter.apply(call.getOperands().get(0))));
                case IS_NULL:
                    return Optional.of(scalar("is_null", TypeCreator.NULLABLE.BOOLEAN, converter.apply(call.getOperands().get(0))));
                case IS_NOT_NULL:
                    return Optional.of(scalar("is_not_null", TypeCreator.NULLABLE.BOOLEAN, converter.apply(call.getOperands().get(0))));
                case EQUALS:
                    return Optional.of(comparison(call, "equal", converter));
                case NOT_EQUALS:
                    return Optional.of(comparison(call, "not_equal", converter));
                case LESS_THAN:
                    return Optional.of(comparison(call, "lt", converter));
                case LESS_THAN_OR_EQUAL:
                    return Optional.of(comparison(call, "lte", converter));
                case GREATER_THAN:
                    return Optional.of(comparison(call, "gt", converter));
                case GREATER_THAN_OR_EQUAL:
                    return Optional.of(comparison(call, "gte", converter));
                case PLUS:
                    return Optional.of(arithmetic(call, "add", converter));
                case MINUS:
                    return Optional.of(arithmetic(call, "subtract", converter));
                case TIMES:
                    return Optional.of(arithmetic(call, "multiply", converter));
                case DIVIDE:
                    return Optional.of(arithmetic(call, "divide", converter));
                case MOD:
                    return Optional.of(arithmetic(call, "modulus", converter));
                case LIKE:
                    return Optional.of(like(call, converter));
                case RLIKE:
                    return Optional.of(
                        scalar(
                            "regexp_like",
                            TypeCreator.NULLABLE.BOOLEAN,
                            converter.apply(call.getOperands().get(0)),
                            stringLiteral(converter.apply(call.getOperands().get(1)))
                        )
                    );
                case STARTS_WITH:
                    return Optional.of(
                        scalar(
                            "starts_with",
                            TypeCreator.NULLABLE.BOOLEAN,
                            converter.apply(call.getOperands().get(0)),
                            stringLiteral(converter.apply(call.getOperands().get(1)))
                        )
                    );
                case OTHER_FUNCTION:
                case OTHER:
                    if (call.getOperator().getName().toUpperCase(Locale.ROOT).equals("LOWER") && call.getOperands().size() == 1) {
                        return Optional.of(
                            scalar("lower", TypeCreator.NULLABLE.STRING, stringLiteral(converter.apply(call.getOperands().get(0))))
                        );
                    }
                    return Optional.empty();
                default:
                    return Optional.empty();
            }
        }

        /** The operands of a same kind chain of {@code kind} calls, converted, in tree order. */
        private static List<Expression> flattened(RexCall call, SqlKind kind, Function<RexNode, Expression> converter) {
            List<Expression> converted = new ArrayList<>();
            for (RexNode operand : flatten(call, kind)) {
                converted.add(converter.apply(operand));
            }
            return converted;
        }

        /**
         * An {@code OR}: flattened, and collapsed to an {@code IN} list
         * when every branch equates the same column to a literal, the
         * shape DataFusion evaluates as one list membership and Lance's
         * scalar index planner answers with one index lookup.
         */
        private static Expression disjunction(RexCall call, Function<RexNode, Expression> converter) {
            List<RexNode> branches = flatten(call, SqlKind.OR);
            Expression inList = asInList(branches, converter);
            if (inList != null) {
                return inList;
            }
            List<Expression> converted = new ArrayList<>(branches.size());
            for (RexNode branch : branches) {
                converted.add(converter.apply(branch));
            }
            return scalar("or", TypeCreator.NULLABLE.BOOLEAN, converted);
        }

        private static Expression asInList(List<RexNode> branches, Function<RexNode, Expression> converter) {
            if (branches.size() < 2) {
                return null;
            }
            RexNode column = null;
            List<Expression> options = new ArrayList<>(branches.size());
            for (RexNode branch : branches) {
                if (branch.getKind() != SqlKind.EQUALS) {
                    return null;
                }
                RexCall equals = (RexCall) branch;
                RexNode left = stripCasts(equals.getOperands().get(0));
                RexNode right = stripCasts(equals.getOperands().get(1));
                if (!(right instanceof RexLiteral) || left instanceof RexLiteral || left instanceof RexCall) {
                    return null;
                }
                if (column == null) {
                    column = left;
                } else if (!column.equals(left)) {
                    return null;
                }
                options.add(converter.apply(right));
            }
            return Expression.SingleOrList.builder().condition(converter.apply(column)).addAllOptions(options).build();
        }

        /**
         * A comparison, with the one rewrite the filter needs: the epoch
         * millis form {@code UNIX_MILLIS(col) op millis} the translator
         * builds for date bounds becomes the column against a
         * millisecond precision timestamp literal, and the widening casts
         * around the column are dropped (DataFusion coerces the operands
         * itself, and the raw column is what a scalar index answers for).
         */
        private static Expression comparison(RexCall call, String function, Function<RexNode, Expression> converter) {
            RexNode left = stripCasts(call.getOperands().get(0));
            RexNode right = stripCasts(call.getOperands().get(1));
            if (left instanceof RexCall unixMillis && isUnixMillis(unixMillis)) {
                if (!(right instanceof RexLiteral literal) || literal.getType().getSqlTypeName().getFamily() != SqlTypeFamily.NUMERIC) {
                    throw new IllegalArgumentException("an epoch millis comparison needs a numeric literal, got " + right);
                }
                RexNode column = stripCasts(unixMillis.getOperands().get(0));
                long millis = literal.getValueAs(Long.class);
                return scalar(
                    function,
                    TypeCreator.NULLABLE.BOOLEAN,
                    converter.apply(column),
                    ExpressionCreator.precisionTimestamp(false, millis, 3)
                );
            }
            return scalar(function, TypeCreator.NULLABLE.BOOLEAN, converter.apply(left), converter.apply(right));
        }

        private static Expression arithmetic(RexCall call, String function, Function<RexNode, Expression> converter) {
            Type type = TypeConverter.DEFAULT.toSubstrait(call.getType());
            return scalar(function, type, converter.apply(call.getOperands().get(0)), converter.apply(call.getOperands().get(1)));
        }

        /** {@code like} / {@code ilike} with the escape operand when the call carries one. */
        private static Expression like(RexCall call, Function<RexNode, Expression> converter) {
            String function = call.getOperator().getName().toUpperCase(Locale.ROOT).equals("ILIKE") ? "ilike" : "like";
            List<Expression> arguments = new ArrayList<>(3);
            arguments.add(converter.apply(call.getOperands().get(0)));
            arguments.add(stringLiteral(converter.apply(call.getOperands().get(1))));
            if (call.getOperands().size() == 3) {
                arguments.add(stringLiteral(converter.apply(call.getOperands().get(2))));
            }
            return scalar(function, TypeCreator.NULLABLE.BOOLEAN, arguments);
        }

        private static boolean isUnixMillis(RexCall call) {
            return call.getOperands().size() == 1 && call.getOperator().getName().toUpperCase(Locale.ROOT).equals("UNIX_MILLIS");
        }

        private static RexNode stripCasts(RexNode node) {
            while (node.getKind() == SqlKind.CAST) {
                node = ((RexCall) node).getOperands().get(0);
            }
            return node;
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
    }

    /** A char or varchar literal as a plain string literal; any other expression unchanged. */
    private static Expression stringLiteral(Expression literal) {
        if (literal instanceof Expression.FixedCharLiteral fixedChar) {
            return ExpressionCreator.string(false, fixedChar.value());
        }
        if (literal instanceof Expression.VarCharLiteral varChar) {
            return ExpressionCreator.string(false, varChar.value());
        }
        return literal;
    }

    private static Expression scalar(String function, Type outputType, Expression... arguments) {
        return ExpressionCreator.scalarFunction(LanceSubstraitExtensions.scalar(function), outputType, arguments);
    }

    private static Expression scalar(String function, Type outputType, List<Expression> arguments) {
        return ExpressionCreator.scalarFunction(LanceSubstraitExtensions.scalar(function), outputType, arguments);
    }

    // ---------------------------------------------------------------
    // Acceptance
    // ---------------------------------------------------------------

    /**
     * Refuses any expression node Lance's filter consumer is not known
     * to evaluate. The walk is a conservative allow list: field
     * references to a top level column (a nested reference is refused
     * by the consumer), the literal kinds DataFusion decodes into plain
     * scalars, casts to the types the translator widens to, {@code
     * CASE}, {@code IN} lists and the accepted scalar functions.
     */
    private static Expression accepted(Expression expression) {
        if (!isAccepted(expression)) {
            throw new IllegalArgumentException("expression outside the Lance filter consumer's vocabulary: " + expression);
        }
        return expression;
    }

    private static boolean isAccepted(Expression expression) {
        if (expression instanceof FieldReference reference) {
            return reference.isSimpleRootReference()
                && reference.segments().size() == 1
                && reference.segments().get(0) instanceof FieldReference.StructField
                && reference.outerReferenceStepsOut().isEmpty();
        }
        if (expression instanceof Expression.BoolLiteral
            || expression instanceof Expression.I8Literal
            || expression instanceof Expression.I16Literal
            || expression instanceof Expression.I32Literal
            || expression instanceof Expression.I64Literal
            || expression instanceof Expression.FP32Literal
            || expression instanceof Expression.FP64Literal
            || expression instanceof Expression.DecimalLiteral
            || expression instanceof Expression.StrLiteral
            || expression instanceof Expression.FixedCharLiteral
            || expression instanceof Expression.VarCharLiteral
            || expression instanceof Expression.DateLiteral
            || expression instanceof Expression.PrecisionTimestampLiteral) {
            return true;
        }
        if (expression instanceof Expression.NullLiteral nullLiteral) {
            Type type = nullLiteral.type();
            return type instanceof Type.Bool
                || type instanceof Type.I8
                || type instanceof Type.I16
                || type instanceof Type.I32
                || type instanceof Type.I64
                || type instanceof Type.FP32
                || type instanceof Type.FP64
                || type instanceof Type.Str;
        }
        if (expression instanceof Expression.Cast cast) {
            Type target = cast.type();
            boolean spellable = target instanceof Type.I8
                || target instanceof Type.I16
                || target instanceof Type.I32
                || target instanceof Type.I64
                || target instanceof Type.FP32
                || target instanceof Type.FP64
                || target instanceof Type.Bool
                || target instanceof Type.Str
                || target instanceof Type.Date
                || target instanceof Type.PrecisionTimestamp;
            return spellable && isAccepted(cast.input());
        }
        if (expression instanceof Expression.IfThen ifThen) {
            for (Expression.IfClause clause : ifThen.ifClauses()) {
                if (!isAccepted(clause.condition()) || !isAccepted(clause.then())) {
                    return false;
                }
            }
            return isAccepted(ifThen.elseClause());
        }
        if (expression instanceof Expression.SingleOrList inList) {
            if (!isAccepted(inList.condition())) {
                return false;
            }
            for (Expression option : inList.options()) {
                if (!isAccepted(option)) {
                    return false;
                }
            }
            return true;
        }
        if (expression instanceof Expression.ScalarFunctionInvocation invocation) {
            if (!ACCEPTED_SCALARS.contains(invocation.declaration().name())) {
                return false;
            }
            for (FunctionArg argument : invocation.arguments()) {
                if (!(argument instanceof Expression nested) || !isAccepted(nested)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}

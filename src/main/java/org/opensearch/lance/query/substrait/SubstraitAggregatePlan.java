/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query.substrait;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hand written Substrait producer for the one plan shape Lance's
 * {@code ScanOptions.Builder#substraitAggregate} accepts: a
 * {@code Plan} whose single relation is an {@code AggregateRel} with
 * grouping expressions and aggregate measures. Lance parses it with
 * datafusion-substrait against the dataset's Arrow schema and runs the
 * group by inside the scan, so the plugin receives one row per group
 * instead of one row per document. {@link Builder#topK} can put a
 * {@code SortRel} and a {@code FetchRel} above the aggregate, which the
 * Lance 12 consumer rejects; see the method for why it exists.
 *
 * <p>The encoding follows substrait 0.63.0, the version the Lance 12
 * native library links. Field numbers are quoted from the proto files
 * next to each writer. datafusion-substrait's consumer resolves each
 * function through {@code extensions[].extension_function.name}
 * (the part before the last {@code :} is the DataFusion function name)
 * and ignores {@code extension_urns}, {@code output_type} and the
 * compound signature suffix, so those are written for validity only.
 *
 * <p>Field references are positions in the dataset schema Lance hands
 * the consumer ({@code Dataset.getSchema()}, every top level column in
 * order, before any projection). The caller resolves column names to
 * those indexes.
 *
 * <p>Lance builds the physical expressions without DataFusion's type
 * coercion pass, so every arithmetic operand has to carry the exact
 * type the operator needs: the builder never mixes an integer column
 * with a floating point literal without an explicit {@link Cast}.
 */
public final class SubstraitAggregatePlan {

    private SubstraitAggregatePlan() {}

    /** Scalar types this producer can spell as a Substrait {@code Type}. */
    public enum ScalarType {
        BOOL,
        I64,
        FP64,
        STRING
    }

    /** An expression inside a grouping or a measure argument. */
    public sealed interface Expression permits FieldReference, BoolLiteral, Int64Literal, Float64Literal, StringLiteral, ScalarFunction,
        Cast, IfThen {}

    /**
     * {@code Expression.selection}: a direct struct field reference into
     * the input schema by position.
     *
     * @param fieldIndex position of the column in the dataset schema
     */
    public record FieldReference(int fieldIndex) implements Expression {
        public FieldReference {
            if (fieldIndex < 0) {
                throw new IllegalArgumentException("field index must not be negative: " + fieldIndex);
            }
        }
    }

    /** {@code Expression.literal.boolean}. */
    public record BoolLiteral(boolean value) implements Expression {
    }

    /** {@code Expression.literal.i64}. */
    public record Int64Literal(long value) implements Expression {
    }

    /** {@code Expression.literal.fp64}. */
    public record Float64Literal(double value) implements Expression {
    }

    /**
     * {@code Expression.literal.string}: a UTF-8 string constant, the
     * form a function such as {@code date_trunc} takes its unit
     * argument in.
     */
    public record StringLiteral(String value) implements Expression {
        public StringLiteral {
            if (value == null) {
                throw new IllegalArgumentException("string literal must not be null");
            }
        }
    }

    /**
     * {@code Expression.scalar_function}. {@code name} is the DataFusion
     * side name: the binary operators {@code add}, {@code subtract},
     * {@code multiply}, {@code divide}, {@code modulus}, {@code lt},
     * {@code gt}, {@code and}, {@code or} and friends map onto DataFusion
     * operators in the consumer's {@code name_to_op}; {@code not},
     * {@code is_null}, {@code is_not_null} and {@code is_true} onto the
     * matching unary expressions; anything else is looked up as a scalar
     * UDF.
     *
     * @param name      DataFusion side function name
     * @param arguments the arguments, in order
     */
    public record ScalarFunction(String name, List<Expression> arguments) implements Expression {
        public ScalarFunction {
            arguments = List.copyOf(arguments);
        }

        public static ScalarFunction of(String name, Expression... arguments) {
            return new ScalarFunction(name, List.of(arguments));
        }
    }

    /** {@code Expression.cast} with {@code FAILURE_BEHAVIOR_THROW_EXCEPTION}. */
    public record Cast(Expression input, ScalarType type) implements Expression {
    }

    /**
     * {@code Expression.if_then}: the searched {@code CASE WHEN c1 THEN
     * v1 WHEN c2 THEN v2 ... ELSE e END}. datafusion-substrait's consumer
     * turns it into DataFusion's {@code Case} expression, which evaluates
     * the branches in order and takes the first whose condition is true;
     * a null condition counts as not true. Without an {@code otherwise}
     * the result is null when no branch matches, which is how a grouping
     * on it leaves rows without a bucket.
     *
     * @param branches  the {@code WHEN ... THEN} pairs, at least one
     * @param otherwise the {@code ELSE} value, or null for no {@code ELSE}
     */
    public record IfThen(List<Branch> branches, Expression otherwise) implements Expression {
        public IfThen {
            branches = List.copyOf(branches);
            if (branches.isEmpty()) {
                throw new IllegalArgumentException("an if-then needs at least one branch");
            }
        }

        /** One {@code WHEN condition THEN value} pair. */
        public record Branch(Expression condition, Expression value) {
        }
    }

    /**
     * Builder for one plan: zero or more grouping expressions followed
     * by one or more measures. Output names go into
     * {@code RelRoot.names} in that order so the result batch carries
     * the caller's column names.
     */
    public static final class Builder {

        private static final String URN_ARITHMETIC = "extension:io.substrait:functions_arithmetic";
        private static final String URN_AGGREGATE_GENERIC = "extension:io.substrait:functions_aggregate_generic";
        private static final String URN_COMPARISON = "extension:io.substrait:functions_comparison";
        private static final String URN_BOOLEAN = "extension:io.substrait:functions_boolean";
        private static final String URN_DATETIME = "extension:io.substrait:functions_datetime";

        private final List<Expression> groupings = new ArrayList<>();
        private final List<String> groupingNames = new ArrayList<>();
        private final List<Measure> measures = new ArrayList<>();
        private final LinkedHashMap<String, Integer> functionAnchors = new LinkedHashMap<>();
        private Expression topKSort;
        private boolean topKAscending;
        private long topKCount;

        private record Measure(String function, List<Expression> arguments, ScalarType outputType, String outputName) {
        }

        /** Adds a grouping expression whose output column is {@code outputName}. */
        public Builder groupBy(Expression expression, String outputName) {
            groupings.add(expression);
            groupingNames.add(outputName);
            return this;
        }

        /**
         * Adds a measure. {@code function} is the DataFusion aggregate
         * name ({@code count}, {@code sum}, {@code min}, {@code max});
         * an empty argument list on {@code count} is {@code count(*)}
         * (the consumer substitutes a constant argument).
         */
        public Builder measure(String function, List<Expression> arguments, ScalarType outputType, String outputName) {
            measures.add(new Measure(function, List.copyOf(arguments), outputType, outputName));
            return this;
        }

        /**
         * Cuts the aggregate's rows to the {@code count} best by
         * {@code sortExpression}: a {@code SortRel} over the aggregate
         * and a {@code FetchRel} of {@code count} rows above it.
         * {@code sortExpression} reads the aggregate's output schema, the
         * groupings first and then the measures, so a
         * {@link FieldReference} of the grouping count is the first
         * measure. Nulls sort last in either direction.
         *
         * <p>No caller sends this to Lance today: the Lance 12 Substrait
         * consumer takes the one relation of the plan as an
         * {@code AggregateRel} and rejects anything else, the
         * {@code FetchRel} included, with "Expected Substrait
         * AggregateRel". The producer is ready for a consumer that takes
         * the sorted cut, at which point the executor can stop cutting
         * top-k on the Java side.
         */
        public Builder topK(Expression sortExpression, boolean ascending, long count) {
            if (sortExpression == null) {
                throw new IllegalArgumentException("top-k needs a sort expression");
            }
            if (count <= 0L) {
                throw new IllegalArgumentException("top-k count must be positive: " + count);
            }
            this.topKSort = sortExpression;
            this.topKAscending = ascending;
            this.topKCount = count;
            return this;
        }

        /** Encodes the plan. The buffer is what {@code ScanOptions.Builder#substraitAggregate} takes. */
        public ByteBuffer build() {
            if (measures.isEmpty()) {
                throw new IllegalStateException("a Substrait aggregate plan needs at least one measure");
            }
            functionAnchors.clear();
            ProtoWriter aggregateRel = new ProtoWriter();
            // AggregateRel (algebra.proto): groupings = 3, measures = 4.
            // AggregateRel.input (2) is omitted: Lance replaces the input
            // with its own filtered fragment scan and never reads it.
            if (!groupings.isEmpty()) {
                // AggregateRel.Grouping: grouping_expressions = 1. The
                // consumer reads this deprecated inline form whenever the
                // relation level grouping_expressions (5) list is empty.
                ProtoWriter grouping = new ProtoWriter();
                for (Expression expression : groupings) {
                    grouping.message(1, expression(expression));
                }
                aggregateRel.message(3, grouping);
            }
            for (Measure measure : measures) {
                // AggregateRel.Measure: measure = 1 (AggregateFunction).
                aggregateRel.message(4, new ProtoWriter().message(1, aggregateFunction(measure)));
            }

            // Rel: fetch = 3, aggregate = 4, sort = 5. RelRoot: input = 1,
            // names = 2.
            ProtoWriter rel = new ProtoWriter().message(4, aggregateRel);
            if (topKSort != null) {
                // SortRel: input = 2, sorts = 3. SortField: expr = 1,
                // direction = 2 (SORT_DIRECTION_ASC_NULLS_LAST = 2,
                // SORT_DIRECTION_DESC_NULLS_LAST = 4).
                ProtoWriter sortField = new ProtoWriter().message(1, expression(topKSort)).varint(2, topKAscending ? 2 : 4);
                ProtoWriter sortRel = new ProtoWriter().message(2, rel).message(3, sortField);
                // FetchRel: input = 2, count = 4 (the int64 form; the
                // expression form is count_expr = 6).
                ProtoWriter fetchRel = new ProtoWriter().message(2, new ProtoWriter().message(5, sortRel)).varint(4, topKCount);
                rel = new ProtoWriter().message(3, fetchRel);
            }
            ProtoWriter relRoot = new ProtoWriter().message(1, rel);
            for (String name : groupingNames) {
                relRoot.string(2, name);
            }
            for (Measure measure : measures) {
                relRoot.string(2, measure.outputName());
            }

            // Plan (plan.proto): extensions = 2, relations = 3, version = 6,
            // extension_urns = 8. PlanRel: root = 2.
            ProtoWriter plan = new ProtoWriter();
            plan.message(6, new ProtoWriter().varint(1, 0).varint(2, 63).varint(3, 0).string(5, "lance-opensearch"));
            Map<String, Integer> urnAnchors = new LinkedHashMap<>();
            for (String function : functionAnchors.keySet()) {
                String urn = urnFor(function);
                if (!urnAnchors.containsKey(urn)) {
                    int anchor = urnAnchors.size() + 1;
                    urnAnchors.put(urn, anchor);
                    // SimpleExtensionURN (extensions.proto): extension_urn_anchor = 1, urn = 2.
                    plan.message(8, new ProtoWriter().varint(1, anchor).string(2, urn));
                }
            }
            for (Map.Entry<String, Integer> entry : functionAnchors.entrySet()) {
                // SimpleExtensionDeclaration: extension_function = 3.
                // ExtensionFunction: function_anchor = 2, name = 3,
                // extension_urn_reference = 4. The name carries the
                // compound signature form the consumer expects and strips.
                ProtoWriter function = new ProtoWriter().varint(2, entry.getValue())
                    .string(3, entry.getKey() + ":any")
                    .varint(4, urnAnchors.get(urnFor(entry.getKey())));
                plan.message(2, new ProtoWriter().message(3, function));
            }
            plan.message(3, new ProtoWriter().message(2, relRoot));
            // The JNI side reads the buffer through GetDirectBufferAddress,
            // which returns null for a heap buffer, so the bytes have to
            // live in a direct buffer.
            byte[] bytes = plan.toBytes();
            ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
            direct.put(bytes);
            direct.flip();
            return direct;
        }

        private ProtoWriter aggregateFunction(Measure measure) {
            // AggregateFunction (algebra.proto): function_reference = 1,
            // phase = 4, output_type = 5, invocation = 6, arguments = 7.
            ProtoWriter function = new ProtoWriter().varint(1, anchorOf(measure.function()));
            function.varint(4, 3); // AGGREGATION_PHASE_INITIAL_TO_RESULT
            function.message(5, type(measure.outputType()));
            function.varint(6, 1); // AGGREGATION_INVOCATION_ALL
            for (Expression argument : measure.arguments()) {
                function.message(7, functionArgument(argument));
            }
            return function;
        }

        private ProtoWriter expression(Expression expression) {
            // Expression (algebra.proto): literal = 1, selection = 2,
            // scalar_function = 3, cast = 11.
            ProtoWriter writer = new ProtoWriter();
            if (expression instanceof FieldReference reference) {
                // FieldReference: direct_reference = 1, root_reference = 4.
                // ReferenceSegment: struct_field = 2. StructField: field = 1.
                ProtoWriter structField = new ProtoWriter().varint(1, reference.fieldIndex());
                ProtoWriter segment = new ProtoWriter().message(2, structField);
                ProtoWriter fieldReference = new ProtoWriter().message(1, segment).message(4, new ProtoWriter());
                writer.message(2, fieldReference);
            } else if (expression instanceof BoolLiteral literal) {
                // Literal: boolean = 1, nullable = 50.
                writer.message(1, new ProtoWriter().bool(1, literal.value()).bool(50, false));
            } else if (expression instanceof Int64Literal literal) {
                // Literal: i64 = 7, nullable = 50.
                writer.message(1, new ProtoWriter().varint(7, literal.value()).bool(50, false));
            } else if (expression instanceof Float64Literal literal) {
                // Literal: fp64 = 11, nullable = 50.
                writer.message(1, new ProtoWriter().fixed64Double(11, literal.value()).bool(50, false));
            } else if (expression instanceof StringLiteral literal) {
                // Literal: string = 12, nullable = 50.
                writer.message(1, new ProtoWriter().string(12, literal.value()).bool(50, false));
            } else if (expression instanceof ScalarFunction call) {
                // ScalarFunction: function_reference = 1, arguments = 4.
                ProtoWriter function = new ProtoWriter().varint(1, anchorOf(call.name()));
                for (Expression argument : call.arguments()) {
                    function.message(4, functionArgument(argument));
                }
                writer.message(3, function);
            } else if (expression instanceof Cast cast) {
                // Cast: type = 1, input = 2, failure_behavior = 3
                // (FAILURE_BEHAVIOR_THROW_EXCEPTION = 2).
                ProtoWriter castWriter = new ProtoWriter().message(1, type(cast.type())).message(2, expression(cast.input())).varint(3, 2);
                writer.message(11, castWriter);
            } else if (expression instanceof IfThen ifThen) {
                // IfThen: ifs = 1 (IfClause: if = 1, then = 2), else = 2.
                ProtoWriter ifThenWriter = new ProtoWriter();
                for (IfThen.Branch branch : ifThen.branches()) {
                    ProtoWriter clause = new ProtoWriter().message(1, expression(branch.condition()))
                        .message(2, expression(branch.value()));
                    ifThenWriter.message(1, clause);
                }
                if (ifThen.otherwise() != null) {
                    ifThenWriter.message(2, expression(ifThen.otherwise()));
                }
                writer.message(6, ifThenWriter);
            } else {
                throw new IllegalArgumentException("unsupported expression " + expression);
            }
            return writer;
        }

        private ProtoWriter functionArgument(Expression argument) {
            // FunctionArgument (algebra.proto): value = 3.
            return new ProtoWriter().message(3, expression(argument));
        }

        private static ProtoWriter type(ScalarType type) {
            // Type (type.proto): bool = 1, i64 = 7, fp64 = 11, string = 12.
            // Each kind message: nullability = 2 (NULLABILITY_NULLABLE = 1).
            ProtoWriter nullable = new ProtoWriter().varint(2, 1);
            int field = switch (type) {
                case BOOL -> 1;
                case I64 -> 7;
                case FP64 -> 11;
                case STRING -> 12;
            };
            return new ProtoWriter().message(field, nullable);
        }

        private int anchorOf(String function) {
            return functionAnchors.computeIfAbsent(function, ignored -> functionAnchors.size() + 1);
        }

        private static String urnFor(String function) {
            return switch (function) {
                case "count" -> URN_AGGREGATE_GENERIC;
                case "lt", "gt", "lte", "gte", "equal", "not_equal", "is_null", "is_not_null", "is_true" -> URN_COMPARISON;
                case "and", "or", "not" -> URN_BOOLEAN;
                case "date_trunc" -> URN_DATETIME;
                default -> URN_ARITHMETIC;
            };
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.fun.SqlBasicAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

/**
 * The custom SQL operators the Lance translator needs and Calcite's
 * standard operator table does not carry.
 *
 * <p>{@link #LANCE_DATE_TRUNC} is the calendar truncation of a
 * {@code date_histogram} with a {@code calendar_interval}:
 * {@code LANCE_DATE_TRUNC('month', ts)} is the first instant of the
 * unit that contains the timestamp, as a timestamp of the same type.
 * {@code SqlStdOperatorTable} has no {@code DATE_TRUNC}, so the
 * operator is defined here to keep the group key expression well typed;
 * the Substrait producer maps it to {@code date_trunc}.
 *
 * <p>The aggregate function factories serve the metric kinds whose
 * OpenSearch result is a set of named doubles ({@code stats},
 * {@code extended_stats}, {@code percentiles},
 * {@code percentile_ranks}). Their planning time type is a {@code ROW}
 * of {@code DOUBLE} fields named by statistic or percent, which depends
 * on the request, so each call gets its own function instance created
 * with that explicit return type; the executor decides the real result
 * shape when the plan runs.
 */
public final class LanceOperatorTable {

    private LanceOperatorTable() {}

    /** {@code LANCE_DATE_TRUNC(unit, timestamp)}: the timestamp truncated to the calendar unit. */
    public static final SqlFunction LANCE_DATE_TRUNC = SqlBasicFunction.create(
        "LANCE_DATE_TRUNC",
        ReturnTypes.ARG1_NULLABLE,
        OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.TIMESTAMP)
    );

    /** The {@code stats} aggregate, typed as the given row of doubles. */
    public static SqlAggFunction stats(RelDataType rowType) {
        return rowTyped("LANCE_STATS", rowType);
    }

    /** The {@code extended_stats} aggregate, typed as the given row of doubles. */
    public static SqlAggFunction extendedStats(RelDataType rowType) {
        return rowTyped("LANCE_EXTENDED_STATS", rowType);
    }

    /** The {@code percentiles} aggregate, typed as a row with one double per requested percent. */
    public static SqlAggFunction percentiles(RelDataType rowType) {
        return rowTyped("LANCE_PERCENTILES", rowType);
    }

    /** The {@code percentile_ranks} aggregate, typed as a row with one double per requested value. */
    public static SqlAggFunction percentileRanks(RelDataType rowType) {
        return rowTyped("LANCE_PERCENTILE_RANKS", rowType);
    }

    private static SqlAggFunction rowTyped(String name, RelDataType rowType) {
        return SqlBasicAggFunction.create(name, SqlKind.OTHER_FUNCTION, ReturnTypes.explicit(rowType), OperandTypes.ANY);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.lance.index.IndexType;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.metadata.ColumnStatistics;
import org.opensearch.lance.plan.metadata.ColumnStatistics.IndexSummary;
import org.opensearch.lance.plan.metadata.TableStatistics;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * A planner model shaped like the perf tables the cost model was fitted
 * on: the perf schema, a bitmap index on {@code category} reporting 200
 * distinct values, BTree indexes on the numeric columns (the one on
 * {@code rating} bounding its values to the range 1 to 5, the one on
 * {@code id} to the row count, as their {@code min} and {@code max}
 * report), and a row count and fragment count the test picks. Public so
 * the planner tests outside this package can plan against the same
 * statistics.
 */
public final class PerfTableFixture {

    private PerfTableFixture() {}

    /** Distinct values of {@code rating} in the perf tables (1 to 5), the range the BTree statistics bound it to. */
    static final long RATING_VALUES = 5L;

    static final Schema SCHEMA = new Schema(
        List.of(
            field("id", new ArrowType.Int(64, true), false),
            field("body", new ArrowType.Utf8(), true),
            field("category", new ArrowType.Utf8(), true),
            field("rating", new ArrowType.Int(32, true), true),
            field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), true),
            field("price", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), true),
            field("user_id", new ArrowType.Int(64, true), true)
        )
    );

    private static Field field(String name, ArrowType type, boolean nullable) {
        return new Field(name, new FieldType(nullable, type, null), null);
    }

    private static IndexSummary index(String name, IndexType type, int fragments, OptionalLong distinct) {
        return index(name, type, fragments, distinct, OptionalLong.empty());
    }

    private static IndexSummary index(String name, IndexType type, int fragments, OptionalLong distinct, OptionalLong integerRange) {
        return new IndexSummary(
            name,
            Optional.of(type),
            fragments,
            fragments,
            OptionalLong.of(1L << 20),
            OptionalLong.empty(),
            OptionalLong.of(0L),
            distinct,
            OptionalLong.empty(),
            integerRange,
            true
        );
    }

    static TableStatistics statistics(long rows, int fragments) {
        List<TableStatistics.FragmentStats> fragmentStats = new ArrayList<>(fragments);
        for (int i = 0; i < fragments; i++) {
            fragmentStats.add(new TableStatistics.FragmentStats(i, rows / fragments, 1));
        }
        return new TableStatistics(
            rows,
            0L,
            fragmentStats,
            Map.of(
                "category",
                new ColumnStatistics("category", List.of(index("category_idx", IndexType.BITMAP, fragments, OptionalLong.of(200L)))),
                "rating",
                new ColumnStatistics(
                    "rating",
                    List.of(index("rating_idx", IndexType.BTREE, fragments, OptionalLong.empty(), OptionalLong.of(RATING_VALUES)))
                ),
                "ts",
                new ColumnStatistics("ts", List.of(index("ts_idx", IndexType.BTREE, fragments, OptionalLong.empty()))),
                "price",
                new ColumnStatistics("price", List.of(index("price_idx", IndexType.BTREE, fragments, OptionalLong.empty()))),
                "id",
                new ColumnStatistics(
                    "id",
                    List.of(index("id_idx", IndexType.BTREE, fragments, OptionalLong.empty(), OptionalLong.of(rows)))
                )
            ),
            1L,
            Instant.EPOCH
        );
    }

    public static LanceSchemas.IndexModel model(String indexName, long rows, int fragments) {
        return LanceSchemas.model(indexName, SCHEMA, Map.of(), Map.of(), "", Set.of(), statistics(rows, fragments));
    }

    /** perf1b: one billion rows in 250 fragments. */
    public static LanceSchemas.IndexModel perf1b() {
        return model("perf1b", 1_000_000_000L, 250);
    }

    /** perf20m: twenty million rows in 80 fragments. */
    public static LanceSchemas.IndexModel perf20m() {
        return model("perf20m", 20_000_000L, 80);
    }

    static RelNode translate(LanceSchemas.IndexModel model, LancePlannerFactory factory, String body) throws IOException {
        return SearchRequestToRel.translate(PlanTestFixtures.parse(body), model, factory);
    }
}

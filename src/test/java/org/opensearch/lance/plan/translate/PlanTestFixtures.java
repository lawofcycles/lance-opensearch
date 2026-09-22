/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The shared translation fixture of the plan tests: an index model over
 * a schema with one column per type family the translator handles, and
 * the parse / translate helpers the fixture and refusal tests share.
 */
final class PlanTestFixtures {

    private PlanTestFixtures() {}

    /**
     * Columns, in order: {@code id} int32 non null, {@code rating}
     * int32, {@code price} float64, {@code weight} float32,
     * {@code category} utf8, {@code body} utf8 with a declared
     * {@code raw} keyword sub-field, {@code flag} bool, {@code ts}
     * microsecond timestamp without a zone, {@code day} date32.
     */
    static final Schema SCHEMA = new Schema(
        List.of(
            field("id", new ArrowType.Int(32, true), false),
            field("rating", new ArrowType.Int(32, true), true),
            field("price", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), true),
            field("weight", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), true),
            field("category", new ArrowType.Utf8(), true),
            field("body", new ArrowType.Utf8(), true),
            field("flag", new ArrowType.Bool(), true),
            field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), true),
            field("day", new ArrowType.Date(DateUnit.DAY), true)
        )
    );

    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );

    private static Field field(String name, ArrowType arrowType, boolean nullable) {
        return new Field(name, new FieldType(nullable, arrowType, null), null);
    }

    static LanceSchemas.IndexModel model() {
        LinkedHashMap<String, String> bodySubs = new LinkedHashMap<>();
        bodySubs.put("raw", "keyword");
        return LanceSchemas.model("idx", SCHEMA, Map.of("body", bodySubs), () -> 512L);
    }

    static LancePlannerFactory factory() {
        return new LancePlannerFactory(1L << 30, 1L << 30);
    }

    /** Parses a search body with the stock aggregation registry. */
    static SearchSourceBuilder parse(String json) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(REGISTRY, null, json)) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }

    static RelNode translate(SearchSourceBuilder source) {
        return SearchRequestToRel.translate(source, model(), factory());
    }

    static String plan(SearchSourceBuilder source) {
        return RelOptUtil.toString(translate(source));
    }
}

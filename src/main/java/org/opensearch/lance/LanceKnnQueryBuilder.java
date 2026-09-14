/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryShardContext;

/**
 * DSL query {@code lance_knn}: {"lance_knn": {"field": "vec", "vector": [...], "k": 5}}.
 * The shard level rewrite target for vector search over an attached table.
 */
public class LanceKnnQueryBuilder extends AbstractQueryBuilder<LanceKnnQueryBuilder> {

    public static final String NAME = "lance_knn";

    private final String field;
    private final float[] vector;
    private final int k;

    public LanceKnnQueryBuilder(String field, float[] vector, int k) {
        this.field = field;
        this.vector = vector;
        this.k = k;
    }

    public LanceKnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.vector = in.readFloatArray();
        this.k = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeFloatArray(vector);
        out.writeVInt(k);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("field", field);
        builder.field("vector", vector);
        builder.field("k", k);
        builder.endObject();
    }

    @SuppressWarnings("unchecked")
    public static LanceKnnQueryBuilder fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> map = parser.map();
        String field = (String) map.get("field");
        List<Number> values = (List<Number>) map.get("vector");
        float[] vector = new float[values.size()];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = values.get(i).floatValue();
        }
        int k = ((Number) map.getOrDefault("k", 10)).intValue();
        return new LanceKnnQueryBuilder(field, vector, k);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        return new LanceKnnQuery(field, vector, k);
    }

    @Override
    protected boolean doEquals(LanceKnnQueryBuilder other) {
        return field.equals(other.field) && java.util.Arrays.equals(vector, other.vector) && k == other.k;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, java.util.Arrays.hashCode(vector), k);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}

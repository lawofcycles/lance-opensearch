/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SourceValueFetcher;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

/**
 * Field type {@code lance_vector}. Marks a column that carries a Lance
 * {@code FixedSizeList<Float32>} vector so that {@link LanceKnnQueryBuilder}
 * can validate the field name and dimension against the mapping before ever
 * reaching Lance.
 *
 * <p>The type is read-only: the mapper does not accept indexed / stored data
 * from user documents (Lance owns the data on the table side), so
 * {@code termQuery} and {@code existsQuery} always short-circuit to
 * {@link MatchNoDocsQuery}. Vector similarity queries go through
 * {@link LanceKnnQueryBuilder}, not through the standard query builders.
 *
 * <p>{@code dimension} is required at derive time so the query builder can
 * confirm that the caller's vector matches the column layout. {@code
 * element_type} is a hint (currently only {@code Float32} is queryable
 * through the Java SDK); other element types are surfaced in the attach
 * notes rather than the mapping.
 */
public class LanceVectorFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "lance_vector";

    private final Integer dimension;
    private final String elementType;

    public static class Builder extends ParametrizedFieldMapper.Builder {
        private final Parameter<Integer> dimension = new Parameter<>(
            "dimension",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : ((Number) o).intValue(),
            m -> ((LanceVectorFieldMapper) m).dimension
        );
        private final Parameter<String> elementType = Parameter.stringParam(
            "element_type",
            false,
            m -> ((LanceVectorFieldMapper) m).elementType,
            "Float32"
        );
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(dimension, elementType, meta);
        }

        @Override
        public LanceVectorFieldMapper build(BuilderContext context) {
            Integer dim = dimension.getValue();
            if (dim == null || dim <= 0) {
                throw new IllegalArgumentException("[lance_vector] mapping requires a positive [dimension] on field [" + name + "]");
            }
            return new LanceVectorFieldMapper(
                name,
                new LanceVectorFieldType(buildFullName(context), dim, elementType.getValue(), meta.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class LanceVectorFieldType extends MappedFieldType {
        private final int dimension;
        private final String elementType;

        LanceVectorFieldType(String name, int dimension, String elementType, Map<String, String> meta) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.dimension = dimension;
            this.elementType = elementType;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public int dimension() {
            return dimension;
        }

        public String elementType() {
            return elementType;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            // Vector similarity does not go through term / match query builders.
            // Callers must use `lance_knn` to search this field.
            return new MatchNoDocsQuery("term query on lance_vector; use lance_knn");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            // Lance vectors are dense per row (no missing-vector representation
            // surfaces through the current derive path), so `exists` degrades
            // to match-nothing to avoid a false positive on the mapping alone.
            return new MatchNoDocsQuery("exists on lance_vector is not supported");
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup lookup, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }
    }

    private LanceVectorFieldMapper(String simpleName, MappedFieldType fieldType, MultiFields multiFields, CopyTo copyTo, Builder builder) {
        super(simpleName, fieldType, multiFields, copyTo);
        this.dimension = builder.dimension.getValue();
        this.elementType = builder.elementType.getValue();
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        throw new UnsupportedOperationException("lance_vector is read only");
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}

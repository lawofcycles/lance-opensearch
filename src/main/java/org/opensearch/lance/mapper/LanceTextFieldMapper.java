/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.mapper;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SourceValueFetcher;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.search.lookup.SearchLookup;

/**
 * Field type "lance_text". Term and match queries on it are rewritten to
 * Lance FTS execution. The optional "tokens_column" parameter selects the
 * RFC's analyzer mode: queries target the derived column holding
 * OpenSearch-analyzed tokens instead of the raw column.
 */
public class LanceTextFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "lance_text";

    private final String tokensColumn;

    public static class Builder extends ParametrizedFieldMapper.Builder {
        private final Parameter<String> tokensColumn = Parameter.stringParam(
            "tokens_column",
            false,
            m -> ((LanceTextFieldMapper) m).tokensColumn,
            null
        ).acceptsNull();
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(tokensColumn, meta);
        }

        @Override
        public LanceTextFieldMapper build(BuilderContext context) {
            return new LanceTextFieldMapper(
                name,
                new LanceTextFieldType(buildFullName(context), tokensColumn.getValue(), meta.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class LanceTextFieldType extends MappedFieldType {
        private final String tokensColumn;

        LanceTextFieldType(String name, String tokensColumn, Map<String, String> meta) {
            super(name, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.tokensColumn = tokensColumn;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            rejectIfDropped();
            String column = tokensColumn != null ? tokensColumn : name();
            String text = value instanceof org.apache.lucene.util.BytesRef b ? b.utf8ToString() : value.toString();
            return new LanceFtsQuery(column, text);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            rejectIfDropped();
            return new MatchAllDocsQuery();
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup lookup, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }

        /**
         * Reject queries against this field type when
         * {@link LanceNamespaceService} has marked it {@code lance_dropped}
         * after the Lance schema dropped or reset the underlying column.
         * The mapping is kept for backwards compatibility with existing
         * caches, but returning matches from a column that no longer
         * exists would silently give wrong answers.
         */
        private void rejectIfDropped() {
            if ("true".equals(meta().get("lance_dropped"))) {
                throw new IllegalArgumentException(
                    "[lance_text] field ["
                        + name()
                        + "] no longer exists in the underlying Lance table; recreate the OpenSearch index to drop it"
                );
            }
        }
    }

    private LanceTextFieldMapper(String simpleName, MappedFieldType fieldType, MultiFields multiFields, CopyTo copyTo, Builder builder) {
        super(simpleName, fieldType, multiFields, copyTo);
        this.tokensColumn = builder.tokensColumn.getValue();
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        throw new UnsupportedOperationException("lance_text is read only");
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

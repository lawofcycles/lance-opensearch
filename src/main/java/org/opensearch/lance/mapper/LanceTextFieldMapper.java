/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.mapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.lance.ipc.FullTextQuery;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SourceValueFetcher;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.attach.LanceTextAnalyzerBackfill;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceScanFilterQuery;
import org.opensearch.lance.query.LanceStringPatternSql;
import org.opensearch.search.lookup.SearchLookup;

/**
 * Field type "lance_text". Term and match queries on it are rewritten to
 * Lance FTS execution; wildcard, regexp and prefix queries to a Lance
 * scan filter over the raw column. The optional "tokens_column"
 * parameter selects the RFC's analyzer mode: term and match queries
 * target the derived column holding OpenSearch-analyzed tokens instead
 * of the raw column.
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

        /** The derived tokens column term and match queries target, or {@code null} for the raw column. */
        public String tokensColumn() {
            return tokensColumn;
        }

        /** The Lance column term and match queries run against: {@link #tokensColumn()} when set, else the field's own column. */
        public String lanceColumn() {
            return tokensColumn != null ? tokensColumn : name();
        }

        /**
         * The OpenSearch analyzer name of the RFC's analyzer mode
         * ({@code meta.lance_analyzer}, written by the derivation for a
         * {@code type: text_analyzer} override), or {@code null} when
         * the field runs on Lance's native tokenizer.
         */
        public String analyzerName() {
            return meta().get("lance_analyzer");
        }

        /**
         * The query text as it should reach Lance: in the analyzer mode
         * the text goes through the field's OpenSearch analyzer and the
         * tokens are joined by single spaces, matching what the
         * backfill stored in the tokens column (whose inverted index
         * splits on whitespace); otherwise the text passes through
         * untouched and Lance's own tokenizer handles it.
         *
         * @throws IllegalArgumentException when the analyzer named in
         *     the mapping meta does not resolve on this index (the
         *     analyzer definition disappeared after attach)
         */
        public String searchText(QueryShardContext context, String text) {
            String analyzerName = analyzerName();
            if (analyzerName == null) {
                return text;
            }
            NamedAnalyzer analyzer = context.getIndexAnalyzers().get(analyzerName);
            if (analyzer == null) {
                throw new IllegalArgumentException(
                    "field [" + name() + "] declares analyzer [" + analyzerName + "] which does not exist on this index"
                );
            }
            try {
                return LanceTextAnalyzerBackfill.joinTokens(analyzer, name(), text);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to analyze query text for field [" + name() + "]", e);
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            rejectIfDropped();
            String text = value instanceof org.apache.lucene.util.BytesRef b ? b.utf8ToString() : value.toString();
            // The FullTextQuery targets the Lance column (the derived
            // tokens column in the analyzer mode); the FLS visibility
            // set carries the mapped field name, because that is the
            // name a security plugin's wrapper reader hides.
            return new LanceFtsQuery(FullTextQuery.match(searchText(context, text), lanceColumn()), java.util.Set.of(name()));
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            rejectIfDropped();
            return MatchAllDocsQuery.INSTANCE;
        }

        /**
         * Wildcard, regexp and prefix run as a Lance scan filter over
         * the raw stored string of the column named by this field, not
         * over the analyzed tokens of {@code tokens_column}: Lance's
         * inverted index has no wildcard or regexp query type and keeps
         * its term dictionary to itself, and a pattern match on the raw
         * value is what the same filter means to a Lance user. The
         * query is unbounded ({@link LanceScanFilterQuery#SCAN_LIMIT_UNBOUNDED});
         * the fragment executor's top-k clip applies when the
         * coordinator translated the whole request to SQL, which is the
         * common case for a bare wildcard / regexp / prefix, and this
         * query is what runs when the pattern sits inside a shape the
         * translator does not cover, where every match is needed.
         */
        @Override
        public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            rejectIfDropped();
            return new LanceScanFilterQuery(LanceStringPatternSql.wildcard(name(), value, caseInsensitive));
        }

        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {
            rejectIfDropped();
            return new LanceScanFilterQuery(LanceStringPatternSql.regexp(name(), value, syntaxFlags, matchFlags));
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            rejectIfDropped();
            return new LanceScanFilterQuery(LanceStringPatternSql.prefix(name(), value, caseInsensitive));
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

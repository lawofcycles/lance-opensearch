/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.CheckedFunction;
import org.opensearch.index.mapper.SeqNoFieldMapper;

/**
 * A reader wrapper shaped like the security plugin's document and field
 * level security reader, for the tests. {@code LancePlugin.onIndexModule}
 * installs it through {@code IndexModule.setReaderWrapper} on every Lance
 * backed index whose name starts with the prefix of
 * {@code plugins.lance.test.hiding_wrapper_index_prefix}, so the
 * integration test cluster (which has no security plugin) and the single
 * node tests see the paths a wrapper changes: the fragment path's count
 * and page through the searcher, the stats withhold, the GET through the
 * wrapper's live docs, the result and fetch caches skipped, the full text
 * clause on a hidden column answering nothing.
 *
 * <p>A {@link Rule} says what the wrapper does. On every leaf it hides
 * the rows whose filter column is below the minimum (a row without a
 * value is hidden as well), through {@code getLiveDocs()} with
 * {@code hasDeletions() == true} and {@code numDocs()} left at the
 * wrapped leaf's value, which is what the security plugin's
 * {@code DlsGetEvaluator} does and what makes a {@code MatchAllDocsQuery}
 * count shortcut report hidden rows. It drops the hidden column from
 * {@code getFieldInfos()} and answers no doc values, terms, points or
 * norms for it, the way the security plugin's field level security drops
 * a field; the stored fields (the {@code _source}) are left as they are.
 * The filter column has to carry numeric doc values whose long value is
 * the row's value (an integer, boolean or date column); when the leaf has
 * no column of that name, the first column with numeric doc values stands
 * in, and a leaf without one hides no row.
 *
 * <p>The directory reader keeps the wrapped reader's cache key, as the
 * security plugin's does, so {@code IndexShard#wrapSearcher} accepts it;
 * a leaf shares the core key and has no reader key of its own, because
 * its live docs differ from the wrapped leaf's.
 */
public final class HidingReaderWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {

    /**
     * What the wrapper hides, parsed from the setting's value
     * {@code <index prefix>:<hidden column>:<filter column>:<minimum>}.
     *
     * @param indexPrefix  the indexes the wrapper is installed on are those whose name starts with this
     * @param hiddenColumn the column dropped from the leaves' field infos
     * @param filterColumn the numeric column a row is judged on
     * @param minimum      the least value of the filter column a visible row has
     */
    public record Rule(String indexPrefix, String hiddenColumn, String filterColumn, long minimum) {

        /**
         * The rule of a setting value, or {@code null} for the empty value
         * (no wrapper). Anything but four non empty colon separated parts
         * with an integer last part is refused.
         */
        public static Rule parse(String value) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            String[] parts = value.split(":", -1);
            if (parts.length != 4) {
                throw new IllegalArgumentException(
                    "[plugins.lance.test.hiding_wrapper_index_prefix] takes <index prefix>:<hidden column>:<filter column>:<minimum>, got ["
                        + value
                        + "]"
                );
            }
            for (int i = 0; i < 3; i++) {
                if (parts[i].isEmpty()) {
                    throw new IllegalArgumentException(
                        "[plugins.lance.test.hiding_wrapper_index_prefix] has an empty part in [" + value + "]"
                    );
                }
            }
            long minimum;
            try {
                minimum = Long.parseLong(parts[3]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "[plugins.lance.test.hiding_wrapper_index_prefix] minimum must be an integer in [" + value + "]",
                    e
                );
            }
            return new Rule(parts[0], parts[1], parts[2], minimum);
        }

        /** Whether the wrapper is installed on the index named {@code indexName}. */
        public boolean appliesTo(String indexName) {
            return indexName.startsWith(indexPrefix);
        }
    }

    private final Rule rule;

    public HidingReaderWrapper(Rule rule) {
        this.rule = rule;
    }

    public Rule rule() {
        return rule;
    }

    @Override
    public DirectoryReader apply(DirectoryReader reader) throws IOException {
        return new HidingDirectoryReader(reader, rule);
    }

    static final class HidingDirectoryReader extends FilterDirectoryReader {

        private final Rule rule;

        HidingDirectoryReader(DirectoryReader in, Rule rule) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new HidingLeafReader(reader, rule);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
            this.rule = rule;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new HidingDirectoryReader(in, rule);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    static final class HidingLeafReader extends FilterLeafReader {

        private final String hiddenColumn;
        private final FieldInfos fieldInfos;
        private final Bits liveDocs;
        private final boolean hasDeletions;

        HidingLeafReader(LeafReader in, Rule rule) throws IOException {
            super(in);
            this.hiddenColumn = rule.hiddenColumn();
            List<FieldInfo> kept = new ArrayList<>();
            for (FieldInfo info : in.getFieldInfos()) {
                if (!info.name.equals(hiddenColumn)) {
                    kept.add(info);
                }
            }
            this.fieldInfos = new FieldInfos(kept.toArray(new FieldInfo[0]));
            String filterColumn = filterColumnOf(in, rule.filterColumn());
            if (filterColumn == null) {
                this.liveDocs = in.getLiveDocs();
                this.hasDeletions = in.hasDeletions();
                return;
            }
            Bits inner = in.getLiveDocs();
            FixedBitSet visible = new FixedBitSet(in.maxDoc());
            NumericDocValues values = in.getNumericDocValues(filterColumn);
            if (values != null) {
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    if (values.longValue() >= rule.minimum() && (inner == null || inner.get(doc))) {
                        visible.set(doc);
                    }
                }
            }
            this.liveDocs = visible;
            this.hasDeletions = true;
        }

        /**
         * The column the rows are judged on: {@code preferred} when the
         * leaf has numeric doc values under that name, else the first
         * field with numeric doc values (the plugin's own
         * {@code _primary_term} of a nested table excluded), else null.
         */
        private static String filterColumnOf(LeafReader in, String preferred) {
            FieldInfo preferredInfo = in.getFieldInfos().fieldInfo(preferred);
            if (preferredInfo != null && preferredInfo.getDocValuesType() == DocValuesType.NUMERIC) {
                return preferred;
            }
            for (FieldInfo info : in.getFieldInfos()) {
                if (info.getDocValuesType() == DocValuesType.NUMERIC && !info.name.equals(SeqNoFieldMapper.PRIMARY_TERM_NAME)) {
                    return info.name;
                }
            }
            return null;
        }

        @Override
        public FieldInfos getFieldInfos() {
            return fieldInfos;
        }

        @Override
        public Bits getLiveDocs() {
            return liveDocs;
        }

        @Override
        public boolean hasDeletions() {
            return hasDeletions;
        }

        @Override
        public int numDocs() {
            // Deliberately not the cardinality of the live docs: the
            // security plugin's DlsGetEvaluator keeps in.numDocs() here.
            return in.numDocs();
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            return field.equals(hiddenColumn) ? null : in.getNumericDocValues(field);
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            return field.equals(hiddenColumn) ? null : in.getBinaryDocValues(field);
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            return field.equals(hiddenColumn) ? null : in.getSortedDocValues(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            return field.equals(hiddenColumn) ? null : in.getSortedNumericDocValues(field);
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            return field.equals(hiddenColumn) ? null : in.getSortedSetDocValues(field);
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            return field.equals(hiddenColumn) ? null : in.getNormValues(field);
        }

        @Override
        public Terms terms(String field) throws IOException {
            return field.equals(hiddenColumn) ? null : in.terms(field);
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            return field.equals(hiddenColumn) ? null : in.getPointValues(field);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }
}

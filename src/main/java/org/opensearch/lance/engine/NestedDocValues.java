/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.lance.engine.LanceFragmentSchema.ColumnKind;

/**
 * Doc values and postings a {@link LanceFragmentLeafReader} serves over
 * its {@link NestedDocLayout}: the {@code _primary_term} parent marker
 * ({@link #parentDocValues}), the {@code _nested_path} postings over
 * the child docs of each nested column ({@link #nestedPathTerms}), and
 * the numeric, boolean and keyword values of nested child fields on
 * their column's child docs ({@link #childNumericDocValues},
 * {@link #childSortedDocValues}). Together they emulate what OpenSearch
 * stores for a {@code nested} field, so {@code Queries.newNonNestedFilter}
 * and {@code NestedPathFieldMapper.filter} work unchanged.
 *
 * <p>Owns the nested child columns of the leaf, decoded per leaf by
 * {@link #ensureNestedColumnLoaded} in one scan of the parent
 * {@code List<Struct>} column and indexed by element ordinal. They
 * bypass the shard column cache, the off-heap store and the hint
 * machinery (nested predicates never push to Lance, so no hint
 * describes child docs). Does not own top-level columns, the layout
 * itself or the stored fields. Built only for leaves whose schema has
 * nested columns.
 */
final class NestedDocValues {

    private final Dataset dataset;
    private final int fragmentId;
    private final NestedDocLayout nestedLayout;
    /** Live docs of the leaf in doc id space (dead parents masked), or {@code null}. */
    private final Bits liveDocs;
    /** Rows plus nested elements, the leaf's {@code maxDoc}. */
    private final int maxDoc;
    private final Map<String, ColumnKind> columnKind;
    /**
     * Top-level {@code List<Struct>} column names with at least one
     * surfaced child. Each element of such a column is a hidden nested
     * child doc placed before the row's parent doc; see
     * {@link NestedDocLayout}. The row take projects the whole column so
     * {@link LanceStoredFields#materialiseStoredFields} renders the array of objects.
     */
    private final Set<String> nestedColumns;
    /** Dotted child path → nested column, for children served on child docs. */
    private final Map<String, String> nestedChildToParent;
    /** Per nested column monitor so {@link #ensureNestedColumnLoaded} serialises the scan of that column. */
    private final Map<String, Object> columnLocks = new ConcurrentHashMap<>();

    // Nested child columns, keyed by dotted child path and indexed by the
    // element ordinal of the child's nested column (see NestedDocLayout).
    // Loaded per leaf by ensureNestedColumnLoaded in one scan of the
    // parent List<Struct> column; they bypass the shard column cache, the
    // off-heap store and the hint machinery (nested predicates never push
    // to Lance, so no hint describes child docs).
    private final Map<String, long[]> nestedNumericColumns = new ConcurrentHashMap<>();
    private final Map<String, FixedBitSet> nestedNumericPresence = new ConcurrentHashMap<>();
    private final Map<String, BytesRef[]> nestedKeywordTerms = new ConcurrentHashMap<>();
    private final Map<String, int[]> nestedKeywordOrds = new ConcurrentHashMap<>();
    /** Nested columns whose children have been decoded into the maps above. */
    private final Set<String> nestedColumnsLoaded = ConcurrentHashMap.newKeySet();

    NestedDocValues(Dataset dataset, int fragmentId, NestedDocLayout nestedLayout, Bits liveDocs, int maxDoc, LanceFragmentSchema schema) {
        this.dataset = dataset;
        this.fragmentId = fragmentId;
        this.nestedLayout = nestedLayout;
        this.liveDocs = liveDocs;
        this.maxDoc = maxDoc;
        this.columnKind = schema.columnKind();
        this.nestedColumns = schema.nestedColumns();
        this.nestedChildToParent = schema.nestedChildToParent();
    }

    private Object columnLock(String name) {
        return columnLocks.computeIfAbsent(name, k -> new Object());
    }

    /**
     * Decode every surfaced child of the nested column
     * {@code nestedColumn} in one Lance scan of the fragment, projecting
     * the whole column. Values land in the nested child maps indexed by
     * element ordinal. The leaf's {@code filterSql} is deliberately not applied:
     * element ordinals cover every live row, and a filtered scan would
     * leave holes the doc id layout does not know about.
     */
    private void ensureNestedColumnLoaded(String nestedColumn) throws IOException {
        if (nestedColumnsLoaded.contains(nestedColumn)) {
            return;
        }
        synchronized (columnLock(nestedColumn)) {
            if (nestedColumnsLoaded.contains(nestedColumn)) {
                return;
            }
            int col = nestedLayout.columnIndexOf(nestedColumn);
            int totalElements = nestedLayout.totalElements(col);
            List<String> childPaths = new ArrayList<>();
            for (Map.Entry<String, String> entry : nestedChildToParent.entrySet()) {
                if (entry.getValue().equals(nestedColumn)) {
                    childPaths.add(entry.getKey());
                }
            }
            Map<String, long[]> numericValues = new HashMap<>();
            Map<String, FixedBitSet> numericPresent = new HashMap<>();
            Map<String, int[]> keywordIds = new HashMap<>();
            Map<String, KeywordDictionaryBuilder> keywordBuilders = new HashMap<>();
            for (String path : childPaths) {
                ColumnKind kind = columnKind.get(path);
                if (kind == ColumnKind.TEXT_KEYWORD) {
                    int[] ids = new int[totalElements];
                    Arrays.fill(ids, -1);
                    keywordIds.put(path, ids);
                    keywordBuilders.put(path, new KeywordDictionaryBuilder());
                } else {
                    numericValues.put(path, new long[totalElements]);
                    numericPresent.put(path, new FixedBitSet(totalElements));
                }
            }
            ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(fragmentId))
                .columns(Collections.singletonList(nestedColumn))
                .withRowAddress(true)
                .build();
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    ListVector list = (ListVector) root.getVector(nestedColumn);
                    StructVector elements = (StructVector) list.getDataVector();
                    for (int i = 0; i < root.getRowCount(); i++) {
                        if (list.isNull(i)) {
                            continue;
                        }
                        int row = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                        int base = nestedLayout.elementBase(col, row);
                        int start = list.getElementStartIndex(i);
                        int end = list.getElementEndIndex(i);
                        for (int e = start; e < end; e++) {
                            int ordinal = base + (e - start);
                            for (String path : childPaths) {
                                FieldVector child = nestedChildVector(elements, nestedColumn, path, e);
                                if (child == null || child.isNull(e)) {
                                    continue;
                                }
                                ColumnKind kind = columnKind.get(path);
                                if (kind == ColumnKind.TEXT_KEYWORD) {
                                    keywordIds.get(path)[ordinal] = keywordBuilders.get(path).intern((VarCharVector) child, e);
                                } else if (kind == ColumnKind.BOOLEAN) {
                                    numericValues.get(path)[ordinal] = ((BitVector) child).get(e);
                                    numericPresent.get(path).set(ordinal);
                                } else {
                                    numericValues.get(path)[ordinal] = LanceColumnLoader.readAsLong(child, e);
                                    numericPresent.get(path).set(ordinal);
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            for (String path : childPaths) {
                ColumnKind kind = columnKind.get(path);
                if (kind == ColumnKind.TEXT_KEYWORD) {
                    KeywordDictionaryBuilder.Dictionary dictionary = keywordBuilders.get(path).finish();
                    int[] ids = keywordIds.get(path);
                    dictionary.remap(ids);
                    nestedKeywordTerms.put(path, dictionary.terms());
                    nestedKeywordOrds.put(path, ids);
                } else {
                    nestedNumericColumns.put(path, numericValues.get(path));
                    nestedNumericPresence.put(path, numericPresent.get(path));
                }
            }
            nestedColumnsLoaded.add(nestedColumn);
        }
    }

    /**
     * The leaf vector behind the child path {@code path} inside the
     * element struct of {@code nestedColumn}, or {@code null} when an
     * intermediate struct is Arrow null at element {@code index} (its
     * descendants are absent for that element).
     */
    private static FieldVector nestedChildVector(StructVector elements, String nestedColumn, String path, int index) {
        String relative = path.substring(nestedColumn.length() + 1);
        FieldVector current = elements;
        for (String segment : relative.split("\\.")) {
            if (current.isNull(index)) {
                return null;
            }
            current = ((StructVector) current).getChild(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    /** {@code _primary_term} doc values: present on parent docs only. */
    NumericDocValues parentDocValues() {
        return new ParentDocValues();
    }

    /** Numeric or boolean doc values of the nested child field {@code path} of {@code parentColumn}. */
    NumericDocValues childNumericDocValues(String path, String parentColumn) {
        return new NestedChildNumericDocValues(path, parentColumn);
    }

    /** Keyword doc values of the nested child field {@code path} of {@code parentColumn}. */
    SortedDocValues childSortedDocValues(String path, String parentColumn) {
        return new NestedChildSortedDocValues(path, parentColumn);
    }

    /** Postings view of the synthetic {@code _nested_path} field. */
    Terms nestedPathTerms() {
        return new NestedPathTerms();
    }

    /**
     * Numeric doc values present on parent docs only, backing
     * {@code Queries.newNonNestedFilter()}'s {@code FieldExistsQuery} on
     * {@code _primary_term}. The value itself (1) is never read by that
     * query; only presence matters. Deliberately blind to liveDocs, as
     * postings and doc values are: the consumer intersects with accepted
     * docs itself.
     */
    private final class ParentDocValues extends NumericDocValues {
        private int doc = -1;

        @Override
        public long longValue() {
            return 1L;
        }

        @Override
        public boolean advanceExact(int target) {
            doc = target;
            return nestedLayout.isParent(target);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            if (target >= maxDoc) {
                doc = NO_MORE_DOCS;
                return doc;
            }
            // The parent of the block target falls in is the first
            // parent at or after target (the parent closes its block).
            doc = nestedLayout.parentDocOf(nestedLayout.rowOfDoc(target));
            return doc;
        }

        @Override
        public long cost() {
            return nestedLayout.rows();
        }
    }

    /**
     * Numeric or boolean doc values of a nested child field, present on
     * the child docs of its nested column only. Parent docs and other
     * columns' child docs report no value, matching how OpenSearch
     * stores a nested field's values on its hidden child documents.
     */
    private final class NestedChildNumericDocValues extends NumericDocValues {
        private final String path;
        private final String parentColumn;
        private final int col;
        private boolean resolved;
        private long[] values;
        private FixedBitSet present;
        private int doc = -1;
        private int ordinal = -1;

        NestedChildNumericDocValues(String path, String parentColumn) {
            this.path = path;
            this.parentColumn = parentColumn;
            this.col = nestedLayout.columnIndexOf(parentColumn);
        }

        private void resolve() throws IOException {
            if (resolved) {
                return;
            }
            resolved = true;
            ensureNestedColumnLoaded(parentColumn);
            values = nestedNumericColumns.get(path);
            present = nestedNumericPresence.get(path);
        }

        @Override
        public long longValue() {
            return values[ordinal];
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            doc = target;
            if (liveDocs != null && !liveDocs.get(target)) {
                return false;
            }
            if (nestedLayout.childColumnOf(target) != col) {
                return false;
            }
            resolve();
            ordinal = nestedLayout.childElementOrdinalOf(col, target);
            return present.get(ordinal);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            resolve();
            for (int candidate = target; candidate < maxDoc; candidate++) {
                if (liveDocs != null && !liveDocs.get(candidate)) {
                    continue;
                }
                if (nestedLayout.childColumnOf(candidate) != col) {
                    continue;
                }
                int candidateOrdinal = nestedLayout.childElementOrdinalOf(col, candidate);
                if (present.get(candidateOrdinal)) {
                    doc = candidate;
                    ordinal = candidateOrdinal;
                    return doc;
                }
            }
            doc = NO_MORE_DOCS;
            return doc;
        }

        @Override
        public long cost() {
            return nestedLayout.totalElements(col);
        }
    }

    /** Keyword doc values of a nested child field, on its column's child docs only. */
    private final class NestedChildSortedDocValues extends SortedDocValues {
        private final String path;
        private final String parentColumn;
        private final int col;
        private boolean resolved;
        private BytesRef[] terms;
        private int[] ords;
        private int doc = -1;
        private int currentOrd = -1;

        NestedChildSortedDocValues(String path, String parentColumn) {
            this.path = path;
            this.parentColumn = parentColumn;
            this.col = nestedLayout.columnIndexOf(parentColumn);
        }

        private void resolve() {
            if (resolved) {
                return;
            }
            resolved = true;
            try {
                ensureNestedColumnLoaded(parentColumn);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            terms = nestedKeywordTerms.get(path);
            ords = nestedKeywordOrds.get(path);
        }

        @Override
        public int ordValue() {
            return currentOrd;
        }

        @Override
        public BytesRef lookupOrd(int ord) {
            resolve();
            return terms[ord];
        }

        @Override
        public int getValueCount() {
            resolve();
            return terms.length;
        }

        @Override
        public boolean advanceExact(int target) {
            doc = target;
            if (liveDocs != null && !liveDocs.get(target)) {
                currentOrd = -1;
                return false;
            }
            if (nestedLayout.childColumnOf(target) != col) {
                currentOrd = -1;
                return false;
            }
            resolve();
            currentOrd = ords[nestedLayout.childElementOrdinalOf(col, target)];
            return currentOrd >= 0;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            resolve();
            for (int candidate = target; candidate < maxDoc; candidate++) {
                if (liveDocs != null && !liveDocs.get(candidate)) {
                    continue;
                }
                if (nestedLayout.childColumnOf(candidate) != col) {
                    continue;
                }
                int ord = ords[nestedLayout.childElementOrdinalOf(col, candidate)];
                if (ord >= 0) {
                    doc = candidate;
                    currentOrd = ord;
                    return doc;
                }
            }
            doc = NO_MORE_DOCS;
            currentOrd = -1;
            return doc;
        }

        @Override
        public long cost() {
            return nestedLayout.totalElements(col);
        }
    }

    /**
     * Postings view of the synthetic {@code _nested_path} field: one term
     * per nested column (the column's path), whose postings are the
     * column's child docs. Backs the {@code TermQuery} that
     * {@code NestedPathFieldMapper.filter} builds as the child filter of
     * a nested query.
     */
    private final class NestedPathTerms extends Terms {

        /** Nested column names sorted in unsigned byte order, with their layout indices. */
        private BytesRef[] sortedTerms;
        private int[] sortedColumns;

        NestedPathTerms() {
            TreeMap<BytesRef, Integer> sorted = new TreeMap<>();
            for (String column : nestedColumns) {
                sorted.put(new BytesRef(column), nestedLayout.columnIndexOf(column));
            }
            sortedTerms = new BytesRef[sorted.size()];
            sortedColumns = new int[sorted.size()];
            int i = 0;
            for (Map.Entry<BytesRef, Integer> entry : sorted.entrySet()) {
                sortedTerms[i] = entry.getKey();
                sortedColumns[i] = entry.getValue();
                i++;
            }
        }

        @Override
        public TermsEnum iterator() {
            return new NestedPathTermsEnum(sortedTerms, sortedColumns);
        }

        @Override
        public long size() {
            return sortedTerms.length;
        }

        @Override
        public long getSumTotalTermFreq() {
            return nestedLayout.nestedDocCount();
        }

        @Override
        public long getSumDocFreq() {
            return nestedLayout.nestedDocCount();
        }

        @Override
        public int getDocCount() {
            return nestedLayout.nestedDocCount();
        }

        @Override
        public boolean hasFreqs() {
            return false;
        }

        @Override
        public boolean hasOffsets() {
            return false;
        }

        @Override
        public boolean hasPositions() {
            return false;
        }

        @Override
        public boolean hasPayloads() {
            return false;
        }

        @Override
        public BytesRef getMin() {
            return sortedTerms.length == 0 ? null : sortedTerms[0];
        }

        @Override
        public BytesRef getMax() {
            return sortedTerms.length == 0 ? null : sortedTerms[sortedTerms.length - 1];
        }
    }

    private final class NestedPathTermsEnum extends BaseTermsEnum {
        private final BytesRef[] terms;
        private final int[] columns;
        private int cursor = -1;

        NestedPathTermsEnum(BytesRef[] terms, int[] columns) {
            this.terms = terms;
            this.columns = columns;
        }

        @Override
        public BytesRef next() {
            cursor++;
            return cursor < terms.length ? terms[cursor] : null;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) {
            int index = Arrays.binarySearch(terms, text);
            if (index >= 0) {
                cursor = index;
                return SeekStatus.FOUND;
            }
            cursor = -index - 1;
            return cursor < terms.length ? SeekStatus.NOT_FOUND : SeekStatus.END;
        }

        @Override
        public void seekExact(long ord) {
            cursor = (int) ord;
        }

        @Override
        public BytesRef term() {
            return terms[cursor];
        }

        @Override
        public long ord() {
            return cursor;
        }

        @Override
        public int docFreq() {
            return nestedLayout.totalElements(columns[cursor]);
        }

        @Override
        public long totalTermFreq() {
            return docFreq();
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) {
            return new NestedPathPostingsEnum(columns[cursor]);
        }

        @Override
        public ImpactsEnum impacts(int flags) {
            return new SlowImpactsEnum(postings(null, PostingsEnum.FREQS));
        }
    }

    /** Iterates the child docs of one nested column in doc id order. */
    private final class NestedPathPostingsEnum extends PostingsEnum {
        private final int col;
        private int row = -1;
        private int inRow;
        private int doc = -1;

        NestedPathPostingsEnum(int col) {
            this.col = col;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            if (row >= 0 && inRow + 1 < nestedLayout.childCount(col, row)) {
                inRow++;
                doc = nestedLayout.childDocStart(col, row) + inRow;
                return doc;
            }
            for (int r = row + 1; r < nestedLayout.rows(); r++) {
                if (nestedLayout.childCount(col, r) > 0) {
                    row = r;
                    inRow = 0;
                    doc = nestedLayout.childDocStart(col, r);
                    return doc;
                }
            }
            doc = NO_MORE_DOCS;
            return doc;
        }

        @Override
        public int advance(int target) {
            if (target >= maxDoc) {
                doc = NO_MORE_DOCS;
                return doc;
            }
            for (int r = Math.max(0, nestedLayout.rowOfDoc(target)); r < nestedLayout.rows(); r++) {
                int n = nestedLayout.childCount(col, r);
                if (n == 0) {
                    continue;
                }
                int start = nestedLayout.childDocStart(col, r);
                int candidate = Math.max(start, target);
                if (candidate < start + n) {
                    row = r;
                    inRow = candidate - start;
                    doc = candidate;
                    return doc;
                }
                // target sits past this row's slice of the column; later
                // rows start after target, so their first child qualifies.
            }
            doc = NO_MORE_DOCS;
            return doc;
        }

        @Override
        public long cost() {
            return nestedLayout.totalElements(col);
        }

        @Override
        public int freq() {
            return 1;
        }

        @Override
        public int nextPosition() {
            return -1;
        }

        @Override
        public int startOffset() {
            return -1;
        }

        @Override
        public int endOffset() {
            return -1;
        }

        @Override
        public BytesRef getPayload() {
            return null;
        }
    }
}

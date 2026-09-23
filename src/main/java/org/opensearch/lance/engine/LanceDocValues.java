/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;

/**
 * Doc values a {@link LanceFragmentLeafReader} serves over the columns
 * its {@link LanceColumnLoader} holds: numeric and boolean
 * ({@link #numeric}), keyword ({@link #sorted}), multi-valued keyword
 * ({@link #sortedSet}) and geo_point ({@link #geoPoint}). The hinted
 * families defer the choice between the rows taken for the scorer's
 * hint, the off-heap store and the full heap column to their first
 * {@code advanceExact}, because Lucene obtains sort comparators and
 * aggregators, and with them these instances, before the Weight has
 * reported its hint for the leaf.
 *
 * <p>Owns the per instance iteration state and the source selection
 * rules (when a sparse dictionary may be used, when an instance falls
 * back to the full column). Does not own the columns, the hint or the
 * sparse structures, which live in the loader, nor the doc id layout,
 * which the leaf answers through {@code rowIfParent}. The nested child
 * families live in {@link NestedDocValues}. One instance per leaf,
 * built by the leaf's constructor.
 */
final class LanceDocValues {

    private final LanceFragmentLeafReader leaf;
    private final LanceColumnLoader loader;
    /** Live docs of the leaf in doc id space, or {@code null}. */
    private final Bits liveDocs;
    /** The leaf's {@code maxDoc}: rows plus nested elements. */
    private final int maxDoc;
    private final int fragmentId;

    LanceDocValues(LanceFragmentLeafReader leaf, LanceColumnLoader loader, Bits liveDocs, int maxDoc, int fragmentId) {
        this.leaf = leaf;
        this.loader = loader;
        this.liveDocs = liveDocs;
        this.maxDoc = maxDoc;
        this.fragmentId = fragmentId;
    }

    /** Numeric or boolean doc values of the top-level column {@code name}, choosing hinted rows or the full column on first use. */
    NumericDocValues numeric(String name, boolean isBoolean) {
        return new HintedNumericDocValues(name, isBoolean);
    }

    /** Encoded {@code lat|lon} doc values of the geo_point column {@code name}. */
    NumericDocValues geoPoint(String name) {
        return new GeoPointNumericDocValues(name);
    }

    /** Keyword doc values of the Utf8 column {@code name} (a base column, or the raw view of an ip column's sub-field). */
    SortedDocValues sorted(String name) {
        return new HintedSortedDocValues(name);
    }

    /** Multi-valued keyword doc values of the {@code List<Utf8>} column {@code name}. */
    SortedSetDocValues sortedSet(String name) {
        return new HintedSortedSetDocValues(name);
    }

    /**
     * Numeric doc values that pick their data source on first use
     * rather than when the instance is created. Lucene's
     * {@code IndexSearcher.searchLeaf} obtains the leaf collector, and
     * with it the sort comparators' doc values, before it asks the
     * Weight for a scorer, so a Lance scorer's hint for the leaf
     * arrives after this instance exists but before the first
     * {@link #advanceExact}. Deferring the choice to that call lets
     * the hint be used.
     *
     * <p>On first use the data source is chosen in this order:
     * <ol>
     *   <li>rows already taken for the current hint on this leaf (a
     *       previous instance of the column took them, and none has
     *       since left them for the full column);</li>
     *   <li>the full column, when it is already present on this leaf
     *       (loaded by an earlier accessor, or published by the shard
     *       cache on behalf of another leaf);</li>
     *   <li>with a hint below {@link LanceColumnLoader#SPARSE_RATIO}: the off-heap
     *       {@link ColumnStore} when it holds this fragment's column
     *       from an earlier request, read for this fragment alone and
     *       without loading anything else; otherwise a take of the
     *       hinted rows;</li>
     *   <li>otherwise the full column, loaded through the shard cache
     *       (into the store when it has room, into heap when not).</li>
     * </ol>
     * A sparse instance that is asked about a doc outside
     * the hint loads the full column at that moment and answers from
     * it for the rest of its life, so a Lucene clause that collects
     * docs the Lance scorer did not produce still sees correct values.
     * The switch is recorded on the sparse structure so later
     * instances of the same column start on the full column.
     *
     * <p>{@link #advance} and {@link #nextDoc} walk every doc of the
     * leaf, which a sparse structure cannot answer; they switch to the
     * full column as well.
     */
    private final class HintedNumericDocValues extends NumericDocValues {
        private final String name;
        private final boolean isBoolean;
        private boolean resolved;
        private long[] column;
        private FixedBitSet presence;
        private CachedColumn offHeap;
        private LanceColumnLoader.SparseNumeric sparse;
        private int sparseIndex = -1;
        private int doc = -1;
        /** Row behind {@link #doc}; what the full column and presence are indexed by. */
        private int row = -1;

        HintedNumericDocValues(String name, boolean isBoolean) {
            this.name = name;
            this.isBoolean = isBoolean;
        }

        private void resolve() throws IOException {
            if (resolved) {
                return;
            }
            resolved = true;
            int[] hint = loader.hintedOffsets;
            // Rows already taken for this hint serve as well as the full
            // column would, so they win even when a shard cache load on
            // behalf of another leaf has published the full column here.
            LanceColumnLoader.SparseNumeric taken = loader.sparseNumeric.get(name);
            if (taken != null && taken.offsets == hint && !taken.fellBack) {
                sparse = taken;
                return;
            }
            if (hasFullColumn()) {
                useFullColumn();
                return;
            }
            if (loader.isSparseHint(hint)) {
                // A slice the store already holds for this fragment is
                // read for one pin; the take is only cheaper than a load
                // the store has not done yet.
                if (loader.serveHeldFromStore(name, isBoolean)) {
                    useFullColumn();
                    return;
                }
                LanceColumnLoader.SparseNumeric candidate = loader.sparseNumericFor(name, isBoolean, hint);
                if (!candidate.fellBack) {
                    sparse = candidate;
                    return;
                }
            }
            // No usable hint: the whole column is needed. The shard cache
            // serves it from the off-heap column store when it holds or
            // can load the column, and loads it into heap for every leaf
            // of the reader in one scan otherwise.
            loadFullColumn(true);
        }

        private boolean hasFullColumn() {
            return loader.offHeapColumns.containsKey(name) || (isBoolean ? loader.booleanColumns : loader.numericColumns).containsKey(name);
        }

        private void loadFullColumn(boolean useShardCache) throws IOException {
            if (isBoolean) {
                loader.ensureBooleanLoaded(name, useShardCache);
            } else {
                loader.ensureNumericLoaded(name, useShardCache);
            }
            useFullColumn();
        }

        private void useFullColumn() {
            offHeap = loader.offHeapColumns.get(name);
            if (offHeap == null) {
                column = isBoolean ? loader.booleanColumns.get(name) : loader.numericColumns.get(name);
                presence = isBoolean ? loader.booleanPresence.get(name) : loader.numericPresence.get(name);
            }
            if (sparse != null) {
                sparse.fellBack = true;
                sparse = null;
            }
        }

        @Override
        public long longValue() {
            if (sparse != null) {
                return sparse.values[sparseIndex];
            }
            return offHeap != null ? offHeap.get(row) : column[row];
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            doc = target;
            if (liveDocs != null && !liveDocs.get(target)) {
                return false;
            }
            row = leaf.rowIfParent(target);
            if (row < 0) {
                // A nested child doc carries no top-level values.
                return false;
            }
            resolve();
            if (sparse != null) {
                int index = Arrays.binarySearch(sparse.offsets, target);
                if (index >= 0) {
                    sparseIndex = index;
                    return sparse.presence.get(index);
                }
                // A doc the Lance scorer did not produce: another
                // clause of the query drives collection on this leaf,
                // so the hint does not cover what will be asked. Read
                // the whole column from here on, scanning this fragment
                // only; the other leaves may still be served sparsely.
                loadFullColumn(false);
            }
            // presence bit is clear for Arrow-null docs; exists / term /
            // range / agg / sort all check advanceExact and stop reading
            // the value here.
            return offHeap != null ? offHeap.isSet(row) : presence.get(row);
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
            if (sparse != null) {
                loadFullColumn(false);
            }
            // Skip liveDocs holes and Arrow-null slots. DocValuesFieldExistsQuery
            // iterates through the doc values with advance/nextDoc alone and does
            // not call advanceExact, so the null bitmap must also be honoured here
            // - otherwise exists / _field_names checks count every row regardless
            // of presence.
            for (int candidate = target; candidate < maxDoc; candidate++) {
                if (liveDocs != null && !liveDocs.get(candidate)) {
                    continue;
                }
                int candidateRow = leaf.rowIfParent(candidate);
                if (candidateRow < 0) {
                    continue;
                }
                if (offHeap != null ? offHeap.isSet(candidateRow) : presence.get(candidateRow)) {
                    doc = candidate;
                    row = candidateRow;
                    return doc;
                }
            }
            doc = NO_MORE_DOCS;
            return doc;
        }

        @Override
        public long cost() {
            // An estimate only; do not resolve here, because a caller
            // that asks for the cost while building the query tree
            // would fix the data source before the hint has arrived.
            if (!resolved) {
                return maxDoc;
            }
            return sparse != null ? sparse.offsets.length : maxDoc;
        }
    }

    /**
     * Doc values of a geo_point column: the encoded {@code lat|lon}
     * long per present row. Loads the column on first use (Lucene asks
     * for doc values while building scorers, before any doc is
     * consumed) and answers presence from the load's bitmap so
     * {@code exists} and the two-phase geo iterators skip Arrow-null
     * locations. No hint path: geo predicates never push to Lance, so
     * the scan that produced the hits ran unfiltered and every doc of
     * the leaf may be asked.
     */
    private final class GeoPointNumericDocValues extends NumericDocValues {
        private final String name;
        private long[] column;
        private FixedBitSet presence;
        private int doc = -1;
        /** Row behind {@link #doc}; what the column and presence are indexed by. */
        private int row = -1;

        GeoPointNumericDocValues(String name) {
            this.name = name;
        }

        private void resolve() throws IOException {
            if (column != null) {
                return;
            }
            loader.ensureGeoPointLoaded(name);
            column = loader.geoColumns.get(name);
            presence = loader.geoPresence.get(name);
        }

        @Override
        public long longValue() {
            return column[row];
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            doc = target;
            if (liveDocs != null && !liveDocs.get(target)) {
                return false;
            }
            row = leaf.rowIfParent(target);
            if (row < 0) {
                return false;
            }
            resolve();
            return presence.get(row);
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
                int candidateRow = leaf.rowIfParent(candidate);
                if (candidateRow < 0) {
                    continue;
                }
                if (presence.get(candidateRow)) {
                    doc = candidate;
                    row = candidateRow;
                    return doc;
                }
            }
            doc = NO_MORE_DOCS;
            return doc;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }

    /**
     * Keyword doc values over a Utf8 column that choose between the
     * sparse dictionary of the hinted rows and the full dictionary on
     * first use, for the same reason {@link HintedNumericDocValues}
     * defers its choice.
     *
     * <p>Unlike numeric values, an ordinal space cannot change after a
     * consumer has seen it: a sort comparator keeps the ordinal of its
     * current bottom slot, and a global ordinal map records every
     * segment ordinal it saw when it was built. The source is chosen on
     * first use in this order, and the decision for the column is
     * recorded in {@link LanceColumnLoader#keywordServedSparse} so every later instance
     * under the same hint uses the same dictionary:
     * <ol>
     *   <li>the decision already recorded for the column under the
     *       current hint;</li>
     *   <li>the full dictionary, when it is already present on this
     *       leaf;</li>
     *   <li>with an exclusive hint (every doc the search collects on
     *       this leaf is a hinted doc) below {@link LanceColumnLoader#SPARSE_RATIO}: the
     *       off-heap {@link ColumnStore} when it holds this fragment's
     *       dictionary from an earlier request, read for this fragment
     *       alone; otherwise the sparse dictionary built from the hinted
     *       rows;</li>
     *   <li>otherwise the full dictionary, loaded through the shard
     *       cache.</li>
     * </ol>
     * Should a doc outside the hint still be requested from a sparse
     * instance, the full column is loaded and the doc's term is looked
     * up in the sparse dictionary; a term that is not there has no
     * ordinal in the space the consumer is using, and the instance
     * fails rather than report the doc as missing or reorder the
     * values.
     *
     * <p>The full dictionary is either the heap {@code BytesRef[]} plus
     * {@code int[]} built for this request or a {@link CachedKeywordColumn}
     * of the off-heap store. Over the store, {@link #ordValue} is one
     * read of the ordinal buffer, {@link #lookupOrd} copies the term
     * into a scratch owned by this instance (the returned
     * {@link BytesRef} is valid until the next call, as with Lucene's
     * own codecs), and {@link #lookupTerm} compares the off-heap bytes
     * in place.
     */
    private final class HintedSortedDocValues extends SortedDocValues {
        private final String name;
        private boolean resolved;
        private int[] ords;
        private BytesRef[] terms;
        private CachedKeywordColumn offHeap;
        private BytesRefBuilder scratch;
        private LanceColumnLoader.SparseKeyword sparse;
        private int currentOrd = -1;
        private int doc = -1;

        HintedSortedDocValues(String name) {
            this.name = name;
        }

        private void resolve() {
            if (resolved) {
                return;
            }
            resolved = true;
            try {
                int[] hint = loader.hintedOffsets;
                boolean exclusive = loader.hintExclusive;
                if (loader.serveKeywordSparse(name, hint, exclusive)) {
                    sparse = loader.sparseKeywordFor(name, hint);
                    terms = sparse.terms;
                } else {
                    loader.serveHeldKeywordInsteadOfTake(name, hint, exclusive, false);
                    loader.ensureTextLoaded(name);
                    useFullColumn();
                }
            } catch (IOException e) {
                // SortedDocValues.getValueCount / lookupOrd do not declare
                // IOException, so the scan failure surfaces unchecked.
                throw new UncheckedIOException(e);
            }
        }

        /** Point this instance at whichever full dictionary {@link LanceColumnLoader#ensureTextLoaded} published for the leaf. */
        private void useFullColumn() {
            offHeap = loader.offHeapKeywordColumns.get(name);
            if (offHeap != null) {
                scratch = new BytesRefBuilder();
            } else {
                ords = loader.keywordOrds.get(name);
                terms = loader.keywordTerms.get(name);
            }
        }

        /**
         * Ordinal, in the sparse dictionary, of a doc the hint does not
         * cover. Loads the full column (store or heap, this fragment
         * only) to learn the doc's term.
         */
        private int ordOutsideHint(int target) throws IOException {
            loader.ensureTextLoaded(name, false);
            int targetRow = leaf.rowIfParent(target);
            if (targetRow < 0) {
                return -1;
            }
            BytesRef term;
            CachedKeywordColumn full = loader.offHeapKeywordColumns.get(name);
            if (full != null) {
                int fullOrd = full.ord(targetRow);
                if (fullOrd < 0) {
                    return -1;
                }
                if (scratch == null) {
                    scratch = new BytesRefBuilder();
                }
                term = full.term(fullOrd, scratch);
            } else {
                int fullOrd = loader.keywordOrds.get(name)[targetRow];
                if (fullOrd < 0) {
                    return -1;
                }
                term = loader.keywordTerms.get(name)[fullOrd];
            }
            int sparseOrd = Arrays.binarySearch(sparse.terms, term);
            if (sparseOrd < 0) {
                throw new IllegalStateException(
                    "doc "
                        + target
                        + " of column "
                        + name
                        + " on fragment "
                        + fragmentId
                        + " was collected although the Lance scorer that hinted the leaf as exclusive did not match it, "
                        + "and its value is not in the sparse dictionary"
                );
            }
            return sparseOrd;
        }

        @Override
        public int ordValue() {
            return currentOrd;
        }

        @Override
        public BytesRef lookupOrd(int ord) {
            resolve();
            return offHeap != null ? offHeap.term(ord, scratch) : terms[ord];
        }

        @Override
        public int lookupTerm(BytesRef key) throws IOException {
            resolve();
            return offHeap != null ? offHeap.lookupTerm(key) : super.lookupTerm(key);
        }

        @Override
        public int getValueCount() {
            resolve();
            return offHeap != null ? offHeap.valueCount() : terms.length;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            doc = target;
            if (liveDocs != null && !liveDocs.get(target)) {
                currentOrd = -1;
                return false;
            }
            resolve();
            if (sparse != null) {
                int index = Arrays.binarySearch(sparse.offsets, target);
                currentOrd = index >= 0 ? sparse.ords[index] : ordOutsideHint(target);
            } else {
                int targetRow = leaf.rowIfParent(target);
                if (targetRow < 0) {
                    currentOrd = -1;
                    return false;
                }
                currentOrd = offHeap != null ? offHeap.ord(targetRow) : ords[targetRow];
            }
            return currentOrd >= 0;
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
            if (sparse != null) {
                // Only hinted docs can be collected under an exclusive
                // hint, so iteration walks the hinted rows.
                int index = Arrays.binarySearch(sparse.offsets, target);
                if (index < 0) {
                    index = -index - 1;
                }
                for (; index < sparse.offsets.length; index++) {
                    int candidate = sparse.offsets[index];
                    if ((liveDocs == null || liveDocs.get(candidate)) && sparse.ords[index] >= 0) {
                        doc = candidate;
                        currentOrd = sparse.ords[index];
                        return doc;
                    }
                }
            } else {
                for (int i = target; i < maxDoc; i++) {
                    if (liveDocs != null && !liveDocs.get(i)) {
                        continue;
                    }
                    int candidateRow = leaf.rowIfParent(i);
                    if (candidateRow < 0) {
                        continue;
                    }
                    int ord = offHeap != null ? offHeap.ord(candidateRow) : ords[candidateRow];
                    if (ord >= 0) {
                        doc = i;
                        currentOrd = ord;
                        return i;
                    }
                }
            }
            doc = NO_MORE_DOCS;
            currentOrd = -1;
            return doc;
        }

        @Override
        public long cost() {
            if (!resolved) {
                return maxDoc;
            }
            return sparse != null ? sparse.offsets.length : maxDoc;
        }
    }

    /**
     * Multi-valued keyword doc values over a List&lt;Utf8&gt; column.
     * Same source selection order and ordinal-space rules as
     * {@link HintedSortedDocValues}. Over the off-heap store the current
     * doc's ordinals are the flat range
     * {@code [rowStart, rowStart + rowCount)} of the
     * {@link CachedKeywordArrayColumn}; over heap they are the doc's
     * {@code int[]}.
     */
    private final class HintedSortedSetDocValues extends SortedSetDocValues {
        private final String name;
        private boolean resolved;
        private int[][] rowOrds;
        private BytesRef[] terms;
        private CachedKeywordArrayColumn offHeap;
        private BytesRefBuilder scratch;
        private LanceColumnLoader.SparseKeywordArray sparse;
        private int[] currentRow;
        private int rowStart;
        private int rowCount;
        private int cursor;
        private int doc = -1;

        HintedSortedSetDocValues(String name) {
            this.name = name;
        }

        private void resolve() {
            if (resolved) {
                return;
            }
            resolved = true;
            try {
                int[] hint = loader.hintedOffsets;
                boolean exclusive = loader.hintExclusive;
                if (loader.serveKeywordSparse(name, hint, exclusive)) {
                    sparse = loader.sparseKeywordArrayFor(name, hint);
                    terms = sparse.terms;
                } else {
                    loader.serveHeldKeywordInsteadOfTake(name, hint, exclusive, true);
                    loader.ensureKeywordArrayLoaded(name);
                    useFullColumn();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void useFullColumn() {
            offHeap = loader.offHeapKeywordArrayColumns.get(name);
            if (offHeap != null) {
                scratch = new BytesRefBuilder();
            } else {
                rowOrds = loader.keywordArrayOrds.get(name);
                terms = loader.keywordArrayTerms.get(name);
            }
        }

        /**
         * Ordinals, in the sparse dictionary, of a doc the hint does not
         * cover. Loads the full column (store or heap, this fragment
         * only) to learn the doc's terms.
         */
        private int[] rowOutsideHint(int target) throws IOException {
            loader.ensureKeywordArrayLoaded(name, false);
            int targetRow = leaf.rowIfParent(target);
            if (targetRow < 0) {
                return null;
            }
            CachedKeywordArrayColumn full = loader.offHeapKeywordArrayColumns.get(name);
            int[] row;
            if (full != null) {
                int start = full.rowStart(targetRow);
                int count = full.rowEnd(targetRow) - start;
                if (count == 0) {
                    return null;
                }
                if (scratch == null) {
                    scratch = new BytesRefBuilder();
                }
                row = new int[count];
                for (int i = 0; i < count; i++) {
                    row[i] = sparseOrdOf(target, full.term(full.ordinal(start + i), scratch));
                }
            } else {
                int[] fullRow = loader.keywordArrayOrds.get(name)[targetRow];
                if (fullRow == null) {
                    return null;
                }
                BytesRef[] fullTerms = loader.keywordArrayTerms.get(name);
                row = new int[fullRow.length];
                for (int i = 0; i < fullRow.length; i++) {
                    row[i] = sparseOrdOf(target, fullTerms[fullRow[i]]);
                }
            }
            // Full-column ordinals are ascending and so are their sparse
            // counterparts (both dictionaries sort the same way).
            return row;
        }

        private int sparseOrdOf(int target, BytesRef term) {
            int sparseOrd = Arrays.binarySearch(sparse.terms, term);
            if (sparseOrd < 0) {
                throw new IllegalStateException(
                    "doc "
                        + target
                        + " of column "
                        + name
                        + " on fragment "
                        + fragmentId
                        + " was collected although the Lance scorer that hinted the leaf as exclusive did not match it, "
                        + "and one of its values is not in the sparse dictionary"
                );
            }
            return sparseOrd;
        }

        @Override
        public long nextOrd() {
            // Caller iterates docValueCount() times; no sentinel needed.
            if (currentRow != null) {
                return currentRow[cursor++];
            }
            return offHeap.ordinal(rowStart + cursor++);
        }

        @Override
        public int docValueCount() {
            return currentRow != null ? currentRow.length : rowCount;
        }

        @Override
        public BytesRef lookupOrd(long ord) {
            resolve();
            return offHeap != null ? offHeap.term((int) ord, scratch) : terms[(int) ord];
        }

        @Override
        public long lookupTerm(BytesRef key) throws IOException {
            resolve();
            return offHeap != null ? offHeap.lookupTerm(key) : super.lookupTerm(key);
        }

        @Override
        public long getValueCount() {
            resolve();
            return offHeap != null ? offHeap.valueCount() : terms.length;
        }

        /** Point the current doc at the off-heap range of the row behind {@code target}; returns whether it has values. */
        private boolean setStoreRow(int target) {
            currentRow = null;
            int targetRow = leaf.rowIfParent(target);
            if (targetRow < 0) {
                rowCount = 0;
                return false;
            }
            rowStart = offHeap.rowStart(targetRow);
            rowCount = offHeap.rowEnd(targetRow) - rowStart;
            return rowCount > 0;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            doc = target;
            cursor = 0;
            if (liveDocs != null && !liveDocs.get(target)) {
                currentRow = null;
                rowCount = 0;
                return false;
            }
            resolve();
            if (sparse != null) {
                int index = Arrays.binarySearch(sparse.offsets, target);
                currentRow = index >= 0 ? sparse.rowOrds[index] : rowOutsideHint(target);
            } else if (offHeap != null) {
                return setStoreRow(target);
            } else {
                int targetRow = leaf.rowIfParent(target);
                currentRow = targetRow < 0 ? null : rowOrds[targetRow];
            }
            rowCount = currentRow == null ? 0 : currentRow.length;
            return rowCount > 0;
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
            cursor = 0;
            if (sparse != null) {
                int index = Arrays.binarySearch(sparse.offsets, target);
                if (index < 0) {
                    index = -index - 1;
                }
                for (; index < sparse.offsets.length; index++) {
                    int candidate = sparse.offsets[index];
                    int[] row = sparse.rowOrds[index];
                    if ((liveDocs == null || liveDocs.get(candidate)) && row != null && row.length > 0) {
                        doc = candidate;
                        currentRow = row;
                        rowCount = row.length;
                        return doc;
                    }
                }
            } else if (offHeap != null) {
                for (int i = target; i < maxDoc; i++) {
                    if ((liveDocs == null || liveDocs.get(i)) && setStoreRow(i)) {
                        doc = i;
                        return i;
                    }
                }
            } else {
                for (int i = target; i < maxDoc; i++) {
                    if (liveDocs != null && !liveDocs.get(i)) {
                        continue;
                    }
                    int candidateRow = leaf.rowIfParent(i);
                    if (candidateRow < 0) {
                        continue;
                    }
                    if (rowOrds[candidateRow] != null && rowOrds[candidateRow].length > 0) {
                        doc = i;
                        currentRow = rowOrds[candidateRow];
                        rowCount = currentRow.length;
                        return i;
                    }
                }
            }
            doc = NO_MORE_DOCS;
            currentRow = null;
            rowCount = 0;
            return doc;
        }

        @Override
        public long cost() {
            if (!resolved) {
                return maxDoc;
            }
            return sparse != null ? sparse.offsets.length : maxDoc;
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.util.Arrays;

import org.apache.arrow.vector.VarCharVector;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * Builds the per-fragment keyword dictionary (distinct terms sorted in
 * unsigned byte order plus a per-doc ordinal mapping) that backs
 * {@link LanceFragmentLeafReader#getSortedDocValues} and
 * {@link LanceFragmentLeafReader#getSortedSetDocValues}, straight from
 * Arrow {@link VarCharVector} bytes. The heap path takes the terms as a
 * {@code BytesRef[]} through {@link #finish}; the off-heap
 * {@link ColumnStore} sizes its vectors from {@link #size} and
 * {@link #termBytes}, then has the sorted terms written into a
 * {@link VarCharVector} through {@link #sort} and {@link #writeTerms}.
 *
 * <p>Values are interned into a {@link BytesRefHash} as they stream out
 * of the Lance scan. Each cell is copied once into a reusable scratch
 * buffer and hashed; only the first occurrence of a distinct term is
 * copied into the hash's byte pool. Nothing per row survives except one
 * {@code int} in the caller's ordinal array, so a 20M-row keyword
 * column with a few thousand distinct values costs about 80 MB of
 * ordinals plus the distinct terms; a {@code String[maxDoc]}
 * intermediate would cost 20M {@link String} objects, roughly 1.5 GB
 * plus the garbage of allocating them.
 *
 * <p>Ordering follows {@link BytesRefHash#sort()}, which compares
 * UTF-8 bytes unsigned. That is the order Lucene's
 * {@link org.apache.lucene.index.SortedDocValues} contract requires and
 * the order Lance / DataFusion apply to Utf8 columns, so a sort pushed
 * down to Lance and a sort run through these ordinals agree; the
 * previous {@code TreeSet<String>} used UTF-16 code unit order, which
 * differs for text outside the Basic Multilingual Plane.
 *
 * <p>Not thread-safe; one builder per (column, fragment) load.
 */
final class KeywordDictionaryBuilder {

    /**
     * Optional per-column transformation applied to every cell before it
     * enters the dictionary. An {@code ip}-overridden Utf8 column
     * installs {@link IpTermEncoder} so the dictionary holds the 16 byte
     * {@code InetAddressPoint} form OpenSearch's {@code IpFieldType}
     * compares doc values against; plain keyword columns install none
     * and intern the raw UTF-8 bytes.
     */
    @FunctionalInterface
    interface TermEncoder {
        /**
         * Encode the {@code length}-byte UTF-8 value at the start of
         * {@code utf8}. Returns the encoded term, or {@code null} when
         * the value cannot be encoded; the row is then treated as
         * missing and counted in {@link #invalidCount()}.
         */
        BytesRef encode(byte[] utf8, int length);
    }

    private final BytesRefHash hash = new BytesRefHash();
    private final BytesRef scratch = new BytesRef();
    private final TermEncoder encoder;
    private byte[] buffer = new byte[64];
    private long termBytes;
    private int invalidCount;
    /** Interned ids in sorted order, set by {@link #sort}. */
    private int[] sortedIds;

    KeywordDictionaryBuilder() {
        this(null);
    }

    KeywordDictionaryBuilder(TermEncoder encoder) {
        this.encoder = encoder;
    }

    /**
     * Intern the value at {@code index} of {@code vector} and return
     * its insertion-order id, or {@code -1} when the column's
     * {@link TermEncoder} rejects the value (counted in
     * {@link #invalidCount()}). The caller must have checked
     * {@code vector.isNull(index)} first.
     */
    int intern(VarCharVector vector, int index) {
        int length = vector.getValueLength(index);
        if (buffer.length < length) {
            buffer = ArrayUtil.grow(buffer, length);
        }
        vector.getDataBuffer().getBytes(vector.getStartOffset(index), buffer, 0, length);
        BytesRef value;
        if (encoder != null) {
            value = encoder.encode(buffer, length);
            if (value == null) {
                invalidCount++;
                return -1;
            }
        } else {
            scratch.bytes = buffer;
            scratch.offset = 0;
            scratch.length = length;
            value = scratch;
        }
        int id = hash.add(value);
        if (id >= 0) {
            termBytes += value.length;
            return id;
        }
        return -id - 1;
    }

    /** Number of values the {@link TermEncoder} rejected so far (rows served as missing). */
    int invalidCount() {
        return invalidCount;
    }

    /** Number of distinct terms interned so far. */
    int size() {
        return hash.size();
    }

    /** Total UTF-8 bytes of the distinct terms interned so far (the data buffer size a {@link VarCharVector} of them needs). */
    long termBytes() {
        return termBytes;
    }

    /**
     * Sort the interned terms. Destroys the builder for further
     * {@link #intern} calls; {@link #writeTerms} and {@link #finish}
     * remain available.
     *
     * @return {@code idToOrd} such that {@code idToOrd[id]} is the
     *         ordinal of the term {@link #intern} returned {@code id} for
     */
    int[] sort() {
        if (sortedIds == null) {
            sortedIds = hash.sort();
        }
        int size = hash.size();
        int[] idToOrd = new int[size];
        for (int ord = 0; ord < size; ord++) {
            idToOrd[sortedIds[ord]] = ord;
        }
        return idToOrd;
    }

    /**
     * Write the sorted terms into {@code target}, one per ordinal, and
     * set its value count. {@code target} must have been allocated for
     * {@link #termBytes} bytes and {@link #size} values; the bytes go
     * straight from the hash's pool into the vector.
     */
    void writeTerms(VarCharVector target) {
        sort();
        int size = hash.size();
        BytesRef view = new BytesRef();
        for (int ord = 0; ord < size; ord++) {
            hash.get(sortedIds[ord], view);
            target.set(ord, view.bytes, view.offset, view.length);
        }
        target.setValueCount(size);
    }

    /**
     * Sort the interned terms and return them as heap {@link BytesRef}s
     * with the id-to-ordinal remap.
     *
     * @return {@code terms} in unsigned byte order and {@code idToOrd}
     *         such that {@code idToOrd[id]} is the ordinal of the term
     *         {@link #intern} returned {@code id} for
     */
    Dictionary finish() {
        int[] idToOrd = sort();
        int size = hash.size();
        BytesRef[] terms = new BytesRef[size];
        BytesRef view = new BytesRef();
        for (int ord = 0; ord < size; ord++) {
            hash.get(sortedIds[ord], view);
            terms[ord] = BytesRef.deepCopyOf(view);
        }
        return new Dictionary(terms, idToOrd);
    }

    /** Remap a per-doc id array in place through {@code idToOrd}; {@code -1} (null) stays {@code -1}. */
    static void remap(int[] idToOrd, int[] ids) {
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] >= 0) {
                ids[i] = idToOrd[ids[i]];
            }
        }
    }

    /**
     * Remap a multi-valued doc's element ids into the strictly
     * ascending, duplicate-free ordinal array
     * {@link org.apache.lucene.index.SortedSetDocValues#nextOrd}
     * requires. {@code ids} holds only non-null elements.
     */
    static int[] remapSortedUnique(int[] idToOrd, int[] ids) {
        if (ids.length == 0) {
            return ids;
        }
        int[] ords = new int[ids.length];
        for (int i = 0; i < ids.length; i++) {
            ords[i] = idToOrd[ids[i]];
        }
        Arrays.sort(ords);
        int unique = 1;
        for (int i = 1; i < ords.length; i++) {
            if (ords[i] != ords[unique - 1]) {
                ords[unique++] = ords[i];
            }
        }
        return unique == ords.length ? ords : Arrays.copyOf(ords, unique);
    }

    /** Sorted terms plus the remap from interned id to sorted ordinal. */
    record Dictionary(BytesRef[] terms, int[] idToOrd) {
        /** Remap a per-doc id array in place; {@code -1} (null) stays {@code -1}. */
        void remap(int[] ids) {
            KeywordDictionaryBuilder.remap(idToOrd, ids);
        }

        /** See {@link KeywordDictionaryBuilder#remapSortedUnique}. */
        int[] remapSortedUnique(int[] ids) {
            return KeywordDictionaryBuilder.remapSortedUnique(idToOrd, ids);
        }
    }
}

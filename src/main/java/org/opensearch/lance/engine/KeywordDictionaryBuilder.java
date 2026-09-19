/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import org.apache.arrow.vector.VarCharVector;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * Builds the per-fragment keyword dictionary
 * ({@code BytesRef[] terms} sorted in unsigned byte order plus a
 * per-doc ordinal array) that backs
 * {@link LanceFragmentLeafReader#getSortedDocValues} and
 * {@link LanceFragmentLeafReader#getSortedSetDocValues}, straight from
 * Arrow {@link VarCharVector} bytes.
 *
 * <p>Values are interned into a {@link BytesRefHash} as they stream out
 * of the Lance scan. Each cell is copied once into a reusable scratch
 * buffer and hashed; only the first occurrence of a distinct term is
 * copied into the hash's byte pool. Nothing per row survives except one
 * {@code int} in the caller's ordinal array, so a 20M-row keyword
 * column with a few thousand distinct values costs about 80 MB of
 * ordinals plus the distinct terms, instead of the 20M {@link String}
 * objects (roughly 1.5 GB plus the garbage of allocating them) the
 * previous {@code String[maxDoc]} intermediate needed. See issue #52.
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

    private final BytesRefHash hash = new BytesRefHash();
    private final BytesRef scratch = new BytesRef();
    private byte[] buffer = new byte[64];

    /**
     * Intern the value at {@code index} of {@code vector} and return
     * its insertion-order id. The caller must have checked
     * {@code vector.isNull(index)} first.
     */
    int intern(VarCharVector vector, int index) {
        int length = vector.getValueLength(index);
        if (buffer.length < length) {
            buffer = ArrayUtil.grow(buffer, length);
        }
        vector.getDataBuffer().getBytes(vector.getStartOffset(index), buffer, 0, length);
        scratch.bytes = buffer;
        scratch.offset = 0;
        scratch.length = length;
        int id = hash.add(scratch);
        return id < 0 ? -id - 1 : id;
    }

    /** Number of distinct terms interned so far. */
    int size() {
        return hash.size();
    }

    /**
     * Sort the interned terms and return them with an id-to-ordinal
     * remap. Destroys the builder for further {@link #intern} calls.
     *
     * @return {@code terms} in unsigned byte order and {@code idToOrd}
     *         such that {@code idToOrd[id]} is the ordinal of the term
     *         {@link #intern} returned {@code id} for
     */
    Dictionary finish() {
        int size = hash.size();
        int[] sortedIds = hash.sort();
        BytesRef[] terms = new BytesRef[size];
        int[] idToOrd = new int[size];
        BytesRef view = new BytesRef();
        for (int ord = 0; ord < size; ord++) {
            int id = sortedIds[ord];
            hash.get(id, view);
            terms[ord] = BytesRef.deepCopyOf(view);
            idToOrd[id] = ord;
        }
        return new Dictionary(terms, idToOrd);
    }

    /** Sorted terms plus the remap from interned id to sorted ordinal. */
    record Dictionary(BytesRef[] terms, int[] idToOrd) {
        /** Remap a per-doc id array in place; {@code -1} (null) stays {@code -1}. */
        void remap(int[] ids) {
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
        int[] remapSortedUnique(int[] ids) {
            if (ids.length == 0) {
                return ids;
            }
            int[] ords = new int[ids.length];
            for (int i = 0; i < ids.length; i++) {
                ords[i] = idToOrd[ids[i]];
            }
            java.util.Arrays.sort(ords);
            int unique = 1;
            for (int i = 1; i < ords.length; i++) {
                if (ords[i] != ords[unique - 1]) {
                    ords[unique++] = ords[i];
                }
            }
            return unique == ords.length ? ords : java.util.Arrays.copyOf(ords, unique);
        }
    }
}

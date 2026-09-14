/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;

/**
 * Wraps a {@link LanceFragmentLeafReader} so it satisfies the {@code
 * SequentialStoredFieldsLeafReader} instanceof check that OpenSearch's
 * {@code FetchPhase} relies on. When a search's fetch batch is sequential
 * and >= 10 docs, FetchPhase calls {@link #getSequentialStoredFieldsReader}
 * and expects a {@link StoredFieldsReader} tuned for adjacent doc ids.
 * Without this wrapper, the default {@code SequentialStoredFieldsLeafReader}
 * plumbing throws "requires a CodecReader or a SequentialStoredFieldsLeafReader"
 * because a Lance-backed reader is neither.
 *
 * <p>Every accessor delegates back to the Lance-backed reader through the
 * {@code FilterLeafReader} default paths, so cache helpers, doc values,
 * stored fields, and field infos all reach the Lance data unchanged. The
 * only override we add is
 * {@link #doGetSequentialStoredFieldsReader(StoredFieldsReader)}, which
 * hands out a {@link StoredFieldsReader} that calls
 * {@link LanceFragmentLeafReader#materialiseStoredFields(int, StoredFieldVisitor)}
 * for each doc. Lance's source materialisation is already keyed by doc id,
 * so it does not benefit from the merge instance super passes in; we ignore
 * it.
 */
final class LanceSequentialLeafReader extends SequentialStoredFieldsLeafReader {

    private final LanceFragmentLeafReader lance;

    LanceSequentialLeafReader(LanceFragmentLeafReader lance) {
        super(lance);
        this.lance = lance;
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return new LanceStoredFieldsReader(lance);
    }

    @Override
    public StoredFieldsReader getSequentialStoredFieldsReader() throws IOException {
        // Bypass the super dispatch, which requires the delegate to be a
        // CodecReader or another SequentialStoredFieldsLeafReader. Our
        // delegate is a raw LanceFragmentLeafReader (LeafReader), so the
        // built-in path would throw. Lance's stored-field materialisation
        // does not need the merge instance super forwards either, so we
        // hand back the Lance-backed reader unconditionally.
        return doGetSequentialStoredFieldsReader(null);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return lance.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return lance.getReaderCacheHelper();
    }

    /**
     * StoredFieldsReader that forwards every doc lookup to
     * {@link LanceFragmentLeafReader#materialiseStoredFields}. Sequential
     * access has no separate optimisation path for Lance today; the reader
     * exists only so the FetchPhase code path type-checks.
     */
    private static final class LanceStoredFieldsReader extends StoredFieldsReader {
        private final LanceFragmentLeafReader lance;

        LanceStoredFieldsReader(LanceFragmentLeafReader lance) {
            this.lance = lance;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            lance.materialiseStoredFields(docID, visitor);
        }

        @Override
        public LanceStoredFieldsReader clone() {
            return this;
        }

        @Override
        public void close() {}

        @Override
        public void checkIntegrity() {}
    }

    static LeafReader wrap(LanceFragmentLeafReader lance) {
        return new LanceSequentialLeafReader(lance);
    }
}

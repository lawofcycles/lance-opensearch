/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.engine.ColumnStore;
import org.opensearch.lance.engine.FetchTakeStats;
import org.opensearch.lance.engine.FragmentGroupScan;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceFragmentLeafReader;
import org.opensearch.lance.engine.LanceFragmentSchema;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.query.ScanAdmission;
import org.opensearch.search.fetch.subphase.FetchDocValuesContext;

/**
 * What the query round ({@link TransportLanceFragmentQueryAction}) and
 * the fetch round ({@link TransportLanceFragmentFetchAction}) of the
 * fragment path share on a data node: the {@link IndexService} a request
 * runs against, the wrapped {@link DirectoryReader} over the request's
 * fragments, and the resolution of the body's {@code docvalue_fields}.
 */
final class FragmentExecutorSupport {

    private static final Logger LOGGER = LogManager.getLogger(FragmentExecutorSupport.class);

    /**
     * How many times a request re-resolves its {@link IndexService}
     * when the cluster state applier registers or removes the node's
     * instance while the request is between the lookup and the
     * temporary creation.
     */
    private static final int INDEX_SERVICE_RACE_RETRIES = 2;

    private FragmentExecutorSupport() {}

    /**
     * The admission kind the full text gate judged {@code shape} under,
     * for the request's profile. {@code ScanAdmission.admit} splits the
     * searched columns by {@code indexedColumns} (the snapshot's columns
     * with an inverted index), judges the indexed ones as
     * {@link ScanAdmission.Kind#FTS} and then the rest as
     * {@link ScanAdmission.Kind#FTS_FLAT}, and a shape that names no
     * column as {@link ScanAdmission.Kind#FTS}; the gate keeps no per
     * request record of that (its last kind is node wide), so the
     * profile derives the kind from the same inputs. A request judged
     * under both kinds reports the flat one, the one judged last and
     * the one whose cost grows with the table.
     */
    static String ftsAdmissionKind(ScanAdmission.Shape shape, Set<String> indexedColumns) {
        for (String column : shape.columns()) {
            if (indexedColumns == null || !indexedColumns.contains(column)) {
                return ScanAdmission.Kind.FTS_FLAT.key();
            }
        }
        return ScanAdmission.Kind.FTS.key();
    }

    /**
     * Run {@code body} against the {@link IndexService} of the index: the
     * node's own registered instance when it hosts the shard copy, else a
     * temporary one built from cluster state for the duration of the call
     * through {@link IndicesService#withTempIndexService}. Both go through
     * the plugins' {@code onIndexModule} hooks, so the security plugin's
     * reader wrapper is present in either case.
     *
     * <p>The cluster state applier can register or remove the node's
     * instance while this runs. {@code withTempIndexService} throws
     * {@link ResourceAlreadyExistsException} when a registered instance
     * appeared after the lookup, and that instance can be gone again by
     * the time it is looked up. The method therefore retries once per
     * such race, up to a small bound, and then gives up with an
     * {@link IllegalStateException} that names the flapping applier; the
     * index is present in cluster state, so an
     * {@code IndexNotFoundException} would misreport it as missing.
     */
    static <T> T withIndexService(
        IndicesService indicesService,
        IndexMetadata indexMetadata,
        String purpose,
        CheckedFunction<IndexService, T, Exception> body
    ) throws Exception {
        for (int attempt = 0;; attempt++) {
            IndexService localIndexService = indicesService.indexService(indexMetadata.getIndex());
            if (localIndexService != null) {
                return body.apply(localIndexService);
            }
            long tempStart = System.nanoTime();
            try {
                return indicesService.withTempIndexService(indexMetadata, tempIndexService -> {
                    // withTempIndexService leaves the MapperService
                    // empty; apply the cluster state mapping the same
                    // way IndicesClusterStateService does for a fresh
                    // IndexService.
                    tempIndexService.updateMapping(null, indexMetadata);
                    LOGGER.debug(
                        "lance.dispatch: temporary IndexService for [{}] ready in {} us ({})",
                        indexMetadata.getIndex().getName(),
                        (System.nanoTime() - tempStart) / 1_000L,
                        purpose
                    );
                    return body.apply(tempIndexService);
                });
            } catch (ResourceAlreadyExistsException raced) {
                if (attempt >= INDEX_SERVICE_RACE_RETRIES) {
                    throw new IllegalStateException(
                        "the cluster state applier on this node kept registering and removing the IndexService for ["
                            + indexMetadata.getIndex().getName()
                            + "] while a "
                            + purpose
                            + " tried to resolve it ("
                            + (attempt + 1)
                            + " attempts)",
                        raced
                    );
                }
                // The cluster state applier registered a local
                // IndexService between the lookup above and the temp
                // creation. Look it up again; if it has been removed in
                // the meantime the lookup misses and the temp path runs
                // once more.
            }
        }
    }

    /**
     * Open the Lance-backed {@link DirectoryReader} over
     * {@code fragmentIds} of {@code snapshot}, wrap it as an
     * {@link OpenSearchDirectoryReader} so downstream code that relies on
     * {@code ShardUtils.extractShardId(reader)} (the security plugin's
     * DLS/FLS wrapper, most importantly) can find the shard id, and apply
     * {@code readerWrapper} when the caller resolved one from the
     * {@link IndexService}.
     *
     * <p>Lance-backed indexes are single-shard fixed, so {@code shardId}
     * is always shard number 0 of the index, the same as the shard path
     * ({@code LanceReadOnlyEngine.openLanceReader}), so wrapper behaviour
     * is consistent across the two paths and no shard copy is needed.
     *
     * <p>The leaves are the request's own: every Lance leaf is handed the
     * request's take accumulator, so the take scans it issues are the
     * request's takes, and the request's take projection, so the same
     * leaves take only the columns the request renders. Column loads of
     * the reader (the store's and the heap fallback's) scan the fragments
     * in the groups of {@code groupScan}, under the request's
     * cancellation.
     *
     * <p>{@code close()} on the returned reader closes any nested reader,
     * so the caller only closes the return value; a failure during
     * construction closes the partially built chain here.
     */
    static DirectoryReader openWrappedReader(
        ShardId shardId,
        LanceWarmCache.Snapshot snapshot,
        ColumnStore columnStore,
        List<Integer> fragmentIds,
        String filterSql,
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper,
        FragmentGroupScan groupScan,
        FetchTakeStats.Accumulator takes,
        LanceFragmentSchema.TakeProjection takeProjection,
        CircuitBreaker requestBreaker
    ) throws IOException {
        DirectoryReader lanceReader = LanceDirectoryReader.openForSnapshot(
            new ByteBuffersDirectory(),
            snapshot,
            columnStore,
            fragmentIds,
            filterSql,
            requestBreaker,
            groupScan
        );
        OpenSearchDirectoryReader wrapped = null;
        try {
            for (LeafReaderContext leaf : lanceReader.leaves()) {
                LanceFragmentLeafReader lanceLeaf = LanceFragmentLeafReader.unwrap(leaf.reader());
                if (lanceLeaf != null) {
                    lanceLeaf.setTakeAccumulator(takes);
                    lanceLeaf.setTakeProjection(takeProjection);
                }
            }
            wrapped = OpenSearchDirectoryReader.wrap(lanceReader, shardId);
            if (readerWrapper == null) {
                return wrapped;
            }
            return readerWrapper.apply(wrapped);
        } catch (Exception e) {
            // Close the outermost reader that was built.
            // OpenSearchDirectoryReader.close() closes the inner Lance
            // reader; if wrap itself failed before returning, the inner
            // reader is still ours to close directly.
            DirectoryReader toClose = wrapped != null ? wrapped : lanceReader;
            try {
                toClose.close();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /**
     * The request's {@code docvalue_fields} resolved against the mapping
     * the way {@code SearchService.parseSource} resolves them: patterns
     * expanded to field names and the total bounded by
     * {@code index.max_docvalue_fields_search}. {@code null} when the
     * request has none.
     */
    static FetchDocValuesContext resolveDocValuesContext(HitProjection projection, IndexService indexService) {
        if (projection.docValueFields().isEmpty()) {
            return null;
        }
        return FetchDocValuesContext.create(
            indexService.mapperService()::simpleMatchToFullName,
            indexService.getIndexSettings().getMaxDocvalueFields(),
            projection.docValueFields()
        );
    }
}

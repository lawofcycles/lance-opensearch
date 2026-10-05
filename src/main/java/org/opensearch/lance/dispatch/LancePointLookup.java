/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.fieldvisitor.CustomFieldsVisitor;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceSettings;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.FetchTakeStats;
import org.opensearch.lance.engine.FragmentGroupScan;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceFragmentLeafReader;
import org.opensearch.lance.engine.LanceFragmentSchema;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

/**
 * The point lookup behind {@code GET /<index>/_doc/<id>} and the items of
 * {@code _mget} on a Lance backed index: the row whose primary key equals
 * the id, rendered as the {@link GetResult} the stock get path renders
 * from a shard.
 *
 * <p>The table is opened at the version the index follows the way the
 * coordinator opens it for a {@code _search} ({@code TransportLanceCoordinatorAction.resolvePinnedVersion}:
 * the latest manifest, the pinned version of {@code index.plugins.lance.version},
 * or the version a tag of {@code index.plugins.lance.tag} points at), through
 * the node's {@link LanceWarmCache}, so a GET and a {@code _search} on the
 * same index read the same manifest and share one snapshot per version.
 * The key is resolved with one Lance scan of the primary key column
 * filtered on {@code <field> = <literal>} over the whole table, which the
 * table's scalar index answers in log time when it has one; the row
 * address of the first hit names the fragment and the row.
 *
 * <p>The row is then read through a {@link LanceFragmentLeafReader} over
 * that one fragment, wrapped by the index's reader wrapper when one is
 * installed ({@code FragmentExecutorSupport.openWrappedReader}, the same
 * way the fragment path reads a page), so the security plugin's document
 * level security hides the row ({@code found: false}) and its field level
 * security drops the hidden fields from {@code _source}. The stored fields
 * path of the leaf renders {@code _source} from a take of the columns the
 * request keeps, through the node's fetch cache when no foreign wrapper
 * sits above the leaf; {@code _source} filtering and {@code stored_fields}
 * are applied as {@code ShardGetService} applies them.
 *
 * <p>{@code _version} is the Lance version the row was read at; {@code _seq_no}
 * and {@code _primary_term} are 1, as the shard engine reported them. An
 * index without a declared primary key, an id the key type cannot parse,
 * and a key the table does not hold answer {@code found: false}.
 */
public final class LancePointLookup {

    private final Supplier<IndicesService> indicesService;
    private final LanceWarmCache warmCache;

    /**
     * @param indicesService the node's {@code IndicesService}, read per
     *                       lookup because it is bound after the plugin's
     *                       components are created
     * @param warmCache      the node's snapshot cache
     */
    public LancePointLookup(Supplier<IndicesService> indicesService, LanceWarmCache warmCache) {
        this.indicesService = indicesService;
        this.warmCache = warmCache;
    }

    /**
     * The row of {@code indexMetadata}'s table whose primary key is
     * {@code id}, or a {@code found: false} result.
     *
     * @param storedFields       the request's {@code stored_fields}, nullable
     * @param fetchSourceContext the request's {@code _source} filter, nullable
     */
    GetResult lookup(IndexMetadata indexMetadata, String id, String[] storedFields, FetchSourceContext fetchSourceContext)
        throws Exception {
        String indexName = indexMetadata.getIndex().getName();
        Settings settings = indexMetadata.getSettings();
        String pkField = LanceSettings.PRIMARY_KEY_FIELD_SETTING.get(settings);
        LancePrimaryKeyType pkType = pkField.isEmpty()
            ? LancePrimaryKeyType.NONE
            : LancePrimaryKeyType.fromSetting(LanceSettings.PRIMARY_KEY_TYPE_SETTING.get(settings));
        String filter = keyFilter(pkField, pkType, id);
        if (filter == null) {
            return notFound(indexName, id);
        }
        IndicesService indices = indicesService.get();
        if (indices == null) {
            throw new IllegalStateException("the node's IndicesService is not available yet; GET on [" + indexName + "] cannot run");
        }
        String tableUri = LanceEngineFactory.tableOf(settings);
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(settings);
        long pinned = TransportLanceCoordinatorAction.resolvePinnedVersion(indexMetadata, tableUri, storageOptions);
        Optional<Long> version = pinned >= 0 ? Optional.of(pinned) : Optional.empty();
        try (
            LanceWarmCache.Lease lease = warmCache.acquire(
                indexMetadata.getIndexUUID(),
                tableUri,
                storageOptions,
                version,
                pkField,
                pkType,
                LanceOverrides.of(settings)
            )
        ) {
            LanceWarmCache.Snapshot snapshot = lease.snapshot();
            OptionalLong rowAddress = firstRowAddress(snapshot.dataset(), pkField, filter);
            if (rowAddress.isEmpty()) {
                return notFound(indexName, id);
            }
            int fragmentId = (int) (rowAddress.getAsLong() >>> 32);
            int offset = (int) (rowAddress.getAsLong() & 0xFFFFFFFFL);
            FetchSourceContext source = normalizeFetchSource(fetchSourceContext, storedFields);
            return FragmentExecutorSupport.withIndexService(
                indices,
                indexMetadata,
                "get",
                indexService -> render(indices, indexService, indexMetadata, snapshot, fragmentId, offset, id, storedFields, source)
            );
        }
    }

    /**
     * The Lance SQL filter that selects the row whose primary key is
     * {@code id}, or {@code null} when no row can match: the index declares
     * no key, or the id does not parse as a value of the key's type. A
     * {@link LancePrimaryKeyType#LONG} key takes {@code Long.parseLong};
     * an {@link LancePrimaryKeyType#UNSIGNED_LONG} key takes a
     * {@link BigInteger} within the UInt64 range, written as the decimal
     * literal Lance's SQL parser accepts against a UInt64 column; a
     * {@link LancePrimaryKeyType#KEYWORD} key is quoted with its single
     * quotes doubled, so an id such as {@code o'brien} neither breaks the
     * filter nor opens an injection path.
     */
    static String keyFilter(String field, LancePrimaryKeyType pkType, String id) {
        if (field == null || field.isEmpty() || pkType == null || id == null) {
            return null;
        }
        switch (pkType) {
            case LONG: {
                try {
                    return field + " = " + Long.parseLong(id);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
            case UNSIGNED_LONG: {
                BigInteger key;
                try {
                    key = new BigInteger(id);
                } catch (NumberFormatException e) {
                    return null;
                }
                if (key.signum() < 0 || key.bitLength() > 64) {
                    return null;
                }
                return field + " = " + key;
            }
            case KEYWORD: {
                if (id.isEmpty()) {
                    return null;
                }
                return field + " = '" + id.replace("'", "''") + "'";
            }
            default:
                return null;
        }
    }

    /**
     * The row address of the first row {@code filter} selects in
     * {@code dataset}, or empty when none does. Projects the key column
     * only and stops at the first batch with a row.
     */
    static OptionalLong firstRowAddress(Dataset dataset, String field, String filter) throws Exception {
        ScanOptions options = new ScanOptions.Builder().filter(filter).columns(List.of(field)).withRowAddress(true).limit(1L).build();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                if (root.getRowCount() > 0) {
                    UInt8Vector rowAddresses = (UInt8Vector) root.getVector("_rowaddr");
                    return OptionalLong.of(rowAddresses.get(0));
                }
            }
        }
        return OptionalLong.empty();
    }

    private GetResult render(
        IndicesService indices,
        IndexService indexService,
        IndexMetadata indexMetadata,
        LanceWarmCache.Snapshot snapshot,
        int fragmentId,
        int offset,
        String id,
        String[] storedFields,
        FetchSourceContext source
    ) throws Exception {
        String indexName = indexMetadata.getIndex().getName();
        checkStoredFieldsAreLeaves(indexService.mapperService(), storedFields);
        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper = TransportLanceFragmentQueryAction
            .resolveReaderWrapper(indexService);
        CircuitBreaker requestBreaker = indices.getCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST);
        try (
            DirectoryReader reader = FragmentExecutorSupport.openWrappedReader(
                shardId,
                snapshot,
                snapshot.isCached() ? warmCache.columnStore() : null,
                List.of(fragmentId),
                null,
                readerWrapper,
                FragmentGroupScan.SEQUENTIAL,
                new FetchTakeStats.Accumulator(),
                takeProjection(snapshot.schema(), source),
                requestBreaker
            )
        ) {
            for (LeafReaderContext ctx : reader.leaves()) {
                LanceFragmentLeafReader lanceLeaf = LanceFragmentLeafReader.unwrap(ctx.reader());
                if (lanceLeaf == null || lanceLeaf.fragmentId() != fragmentId) {
                    continue;
                }
                int docId = lanceLeaf.docOfRow(offset);
                // The wrapper the security plugin interposes for document
                // level security hides a row through the leaf's live docs;
                // a hidden row is answered as absent, as _search omits it.
                Bits liveDocs = ctx.reader().getLiveDocs();
                if (liveDocs != null && !liveDocs.get(docId)) {
                    return notFound(indexName, id);
                }
                lanceLeaf.setFetchCacheEligible(LanceFragmentLeafReader.wrappedOnlyByOwnReaders(ctx.reader()));
                return fromStoredFields(
                    ctx.reader(),
                    docId,
                    indexService.mapperService(),
                    indexName,
                    id,
                    snapshot.version(),
                    storedFields,
                    source
                );
            }
            // The wrapper dropped the leaf itself.
            return notFound(indexName, id);
        }
    }

    /**
     * The columns the row take projects for this request: the surfaced
     * columns the {@code _source} filter keeps, none when {@code _source}
     * is not rendered. {@code _id} is the request's id, so the primary key
     * column is not taken for it.
     */
    private static LanceFragmentSchema.TakeProjection takeProjection(LanceFragmentSchema schema, FetchSourceContext source) {
        if (!source.fetchSource()) {
            return schema.takeProjection(false, List.of(), List.of(), List.of());
        }
        List<String> includes = source.includes().length > 0 ? List.of(source.includes()) : null;
        return schema.takeProjection(false, includes, List.of(source.excludes()), List.of());
    }

    /**
     * Reads the stored fields of {@code docId} through {@code leaf} the way
     * {@code ShardGetService.innerGetLoadFromStoredFields} reads a shard's:
     * one {@link FieldsVisitor} for the whole {@code _source}, a
     * {@link CustomFieldsVisitor} when {@code stored_fields} names fields,
     * none when neither is asked for; the fields the visitor collected are
     * split into document and metadata fields after the mapping's post
     * processing, and the source is filtered by the request's includes
     * and excludes.
     */
    private static GetResult fromStoredFields(
        LeafReader leaf,
        int docId,
        MapperService mapperService,
        String indexName,
        String id,
        long version,
        String[] storedFields,
        FetchSourceContext source
    ) throws IOException {
        FieldsVisitor visitor;
        if (storedFields == null || storedFields.length == 0) {
            visitor = source.fetchSource() ? new FieldsVisitor(true) : null;
        } else {
            visitor = new CustomFieldsVisitor(new HashSet<>(Arrays.asList(storedFields)), source.fetchSource());
        }
        BytesReference sourceBytes = null;
        Map<String, DocumentField> documentFields = null;
        Map<String, DocumentField> metadataFields = null;
        if (visitor != null) {
            leaf.storedFields().document(docId, visitor);
            sourceBytes = visitor.source();
            if (!visitor.fields().isEmpty()) {
                visitor.postProcess(mapperService::fieldType);
                documentFields = new HashMap<>();
                metadataFields = new HashMap<>();
                for (Map.Entry<String, List<Object>> entry : visitor.fields().entrySet()) {
                    DocumentField field = new DocumentField(entry.getKey(), entry.getValue());
                    if (mapperService.isMetadataField(entry.getKey())) {
                        metadataFields.put(entry.getKey(), field);
                    } else {
                        documentFields.put(entry.getKey(), field);
                    }
                }
            }
        }
        sourceBytes = source.fetchSource() ? applySourceFilter(sourceBytes, source) : null;
        return new GetResult(indexName, id, 1L, 1L, version, true, sourceBytes, documentFields, metadataFields);
    }

    /** The stock get path's refusal of a {@code stored_fields} entry that names an object field. */
    private static void checkStoredFieldsAreLeaves(MapperService mapperService, String[] storedFields) {
        if (storedFields == null) {
            return;
        }
        DocumentMapper docMapper = mapperService.documentMapper();
        if (docMapper == null) {
            return;
        }
        for (String field : storedFields) {
            if (docMapper.mappers().getMapper(field) == null && docMapper.objectMappers().get(field) != null) {
                throw new IllegalArgumentException("field [" + field + "] isn't a leaf field");
            }
        }
    }

    /**
     * The {@code _source} decision the stock get path takes: the request's
     * own context when it has one, else the whole source unless
     * {@code stored_fields} is given without naming {@code _source}.
     */
    static FetchSourceContext normalizeFetchSource(FetchSourceContext context, String[] storedFields) {
        if (context != null) {
            return context;
        }
        if (storedFields == null) {
            return FetchSourceContext.FETCH_SOURCE;
        }
        for (String field : storedFields) {
            if (SourceFieldMapper.NAME.equals(field)) {
                return FetchSourceContext.FETCH_SOURCE;
            }
        }
        return FetchSourceContext.DO_NOT_FETCH_SOURCE;
    }

    /** {@code source} with the request's includes and excludes applied; {@code source} itself when it has none. */
    private static BytesReference applySourceFilter(BytesReference source, FetchSourceContext context) {
        if (source == null || (context.includes().length == 0 && context.excludes().length == 0)) {
            return source;
        }
        Tuple<XContentType, Map<String, Object>> typeAndMap = XContentHelper.convertToMap(source, true);
        Map<String, Object> filtered = XContentMapValues.filter(typeAndMap.v2(), context.includes(), context.excludes());
        try {
            return BytesReference.bytes(MediaTypeRegistry.contentBuilder(typeAndMap.v1()).map(filtered));
        } catch (IOException e) {
            throw new OpenSearchException("Failed to apply source includes/excludes filter", e);
        }
    }

    static GetResult notFound(String indexName, String id) {
        return new GetResult(
            indexName,
            id,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            -1,
            false,
            null,
            null,
            null
        );
    }
}

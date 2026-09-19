/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceInternalHeaders;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.namespace.AllowedTableRoots;
import org.opensearch.lance.namespace.LanceNamespaceService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/**
 * POST /_lance/attach {"table": "/path/to/table.lance"}
 *
 * The RFC's attach operation. Derives everything from the table and creates
 * a real engine backed index. The mapping follows the derivation defaults
 * (a string column carrying an FTS index maps to text, integers map to
 * numeric doc values fields), the shard count is always one, and the
 * primary key is detected from Lance field metadata. Optional override:
 * "name" (index name, defaults to the table directory name).
 *
 * <p>Attach always creates a single-shard index because the fragment path
 * (see {@code LanceDispatchActionFilter}) is the only search implementation
 * left, and it fans out to fragments regardless of shard count. Requests
 * carrying {@code number_of_shards} are rejected with 400.
 *
 * <p>Threading: the JNI work ({@link Dataset#open}, schema and row counts)
 * runs on {@link ThreadPool.Names#GENERIC}. The transport thread only
 * validates the request body so a bad payload returns 400 immediately.
 *
 * <p>Existing-index handling: when the target index already exists we look
 * up its settings and only report {@code already_attached: true} if the
 * existing index is a Lance index pointing to the same table. Any other
 * clash (plain index reusing the name, Lance index for a different table)
 * returns 409 so the operator picks a different name explicitly.
 */
public class RestAttachAction extends BaseRestHandler {

    private static final String PK_METADATA_KEY = "lance-schema:unenforced-primary-key";

    private final ThreadPool threadPool;
    private final AllowedTableRoots allowedRoots;
    private final LanceNamespaceService namespaceService;

    public RestAttachAction(ThreadPool threadPool, AllowedTableRoots allowedRoots, LanceNamespaceService namespaceService) {
        this.threadPool = threadPool;
        this.allowedRoots = allowedRoots;
        this.namespaceService = namespaceService;
    }

    @Override
    public String getName() {
        return "lance_attach";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_lance/attach"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        Map<String, Object> body = request.hasContent()
            ? XContentHelper.convertToMap(request.content(), false, request.getMediaType()).v2()
            : Map.of();

        String table;
        String explicitName;
        Long pinnedVersion;
        StorageOptions storageOptions;
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields;
        try {
            table = readOptionalString(body, "table");
            if (table == null || table.isEmpty()) {
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "[table] is required"));
            }
            explicitName = readOptionalString(body, "name");
            if (body.containsKey("number_of_shards")) {
                // Attach used to derive a shard count from the row count, but
                // the fragment path is now the only search implementation and
                // shards no longer influence fan-out. Rejecting the option is
                // cleaner than silently ignoring it — an operator setting
                // `number_of_shards: 5` would otherwise still get a
                // single-shard index and be confused about why fan-out did
                // not widen.
                return channel -> channel.sendResponse(
                    new BytesRestResponse(
                        RestStatus.BAD_REQUEST,
                        "[number_of_shards] is no longer accepted by /_lance/attach; the fragment path fans out at the fragment "
                            + "level regardless of shard count, and Lance-backed indices are always single-shard"
                    )
                );
            }
            pinnedVersion = readOptionalLong(body, "version");
            if (pinnedVersion != null && pinnedVersion < 0) {
                return channel -> channel.sendResponse(
                    new BytesRestResponse(RestStatus.BAD_REQUEST, "[version] must be a non-negative integer")
                );
            }
            storageOptions = StorageOptions.parseFromRequestField(body.get("storage_options"), "[lance_attach]");
            multiFields = parseMultiFields(body.get("multi_fields"));
            java.util.Map<String, java.util.LinkedHashMap<String, String>> overrides = parseOverrides(body.get("overrides"));
            if (!overrides.isEmpty()) {
                if (!multiFields.isEmpty()) {
                    // Same conceptual data (sub-field spec) coming in twice
                    // through both shapes is ambiguous: which one wins?
                    // Reject rather than pick a rule the operator did not
                    // know about. `overrides` is the forward-looking shape;
                    // the old `multi_fields` clause is kept for BC only.
                    for (String col : overrides.keySet()) {
                        if (multiFields.containsKey(col)) {
                            throw new IllegalArgumentException(
                                "attach body carries both [multi_fields] and [overrides] entries for column ["
                                    + col
                                    + "]; use [overrides] and drop the duplicate [multi_fields] entry"
                            );
                        }
                    }
                    // No column conflict; merge into one map. `overrides`
                    // wins on any later augmentation because it is the
                    // canonical shape.
                    java.util.LinkedHashMap<String, java.util.LinkedHashMap<String, String>> merged = new java.util.LinkedHashMap<>();
                    merged.putAll(multiFields);
                    merged.putAll(overrides);
                    multiFields = merged;
                } else {
                    multiFields = overrides;
                }
            }
        } catch (IllegalArgumentException e) {
            String message = e.getMessage();
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, message));
        }

        if (!allowedRoots.allows(table)) {
            String rejected = table;
            return channel -> channel.sendResponse(
                new BytesRestResponse(
                    RestStatus.FORBIDDEN,
                    "table [" + rejected + "] is not under any of the configured lance.allowed_table_roots"
                )
            );
        }

        final String tableFinal = table;
        final String indexName = explicitName != null ? explicitName : tableName(table);
        final StorageOptions storageOptionsFinal = storageOptions;
        final java.util.Optional<Long> pinnedVersionFinal = java.util.Optional.ofNullable(pinnedVersion);
        final java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFieldsFinal = multiFields;

        // Dispatch the JNI work to the generic pool. Dataset.open blocks on
        // native I/O and would trip the transport-thread assertion otherwise.
        return channel -> threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            Derivation derivation;
            try (Dataset dataset = LanceRegistry.openDataset(tableFinal, storageOptionsFinal, pinnedVersionFinal)) {
                derivation = derive(dataset, multiFieldsFinal);
            } catch (IllegalArgumentException e) {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, e.getMessage()));
                return;
            } catch (Exception e) {
                sendError(channel, e);
                return;
            }
            createIndex(client, channel, indexName, tableFinal, derivation, namespaceService, storageOptionsFinal, pinnedVersionFinal);
        });
    }

    private static void createIndex(
        NodeClient client,
        RestChannel channel,
        String indexName,
        String table,
        Derivation derivation,
        LanceNamespaceService namespaceService,
        StorageOptions storageOptions,
        java.util.Optional<Long> pinnedVersion
    ) {
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(LanceEngineFactory.TABLE_SETTING, table)
            .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, derivation.keyField)
            .put(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING, derivation.keyFieldType);
        if (!derivation.multiFieldsJson.isEmpty()) {
            settings.put(LanceEngineFactory.MULTI_FIELDS_SETTING, derivation.multiFieldsJson);
        }
        pinnedVersion.ifPresent(v -> settings.put(LanceEngineFactory.VERSION_SETTING, v));
        storageOptions.writeToSettings(settings);
        CreateIndexRequest create = new CreateIndexRequest(indexName).settings(settings.build()).mapping(derivation.mappingJson);

        // LanceCreateIndexActionFilter blocks user PUT /{index} that
        // tries to set index.lance.table. Stamp the internal header
        // so this plugin-issued call is recognised as legitimate.
        // stashContext preserves the caller's headers for the outer
        // REST handler.
        ThreadContext threadContext = client.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(LanceInternalHeaders.LANCE_INTERNAL_CREATE_INDEX, "true");
            client.admin().indices().create(create, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    // Register the attach-created index with the namespace poller
                    // only when the operator is following the latest version.
                    // Pinned indices stay on their manifest version by design
                    // (readonly snapshot for reproducibility), so the poll cycle
                    // does not need to touch them and would otherwise burn cycles
                    // probing for a manifest advance that must not change the
                    // reader.
                    if (pinnedVersion.isEmpty()) {
                        namespaceService.registerAttachedIndex(indexName, table, derivation.version, storageOptions);
                    }
                    writeAttachResponse(channel, indexName, table, derivation, false);
                }

                @Override
                public void onFailure(Exception e) {
                    if (!isAlreadyExists(e)) {
                        sendError(channel, e);
                        return;
                    }
                    // The index already exists. Verify it is a Lance index for the
                    // same table before claiming success; otherwise attach would
                    // silently take credit for an unrelated index.
                    verifyExistingLanceIndex(
                        client,
                        channel,
                        indexName,
                        table,
                        derivation,
                        namespaceService,
                        storageOptions,
                        pinnedVersion
                    );
                }
            });
        }
    }

    private static void verifyExistingLanceIndex(
        NodeClient client,
        RestChannel channel,
        String indexName,
        String table,
        Derivation derivation,
        LanceNamespaceService namespaceService,
        StorageOptions storageOptions,
        java.util.Optional<Long> pinnedVersion
    ) {
        ClusterStateRequest stateRequest = new ClusterStateRequest();
        stateRequest.clear().metadata(true).indices(indexName);
        client.admin().cluster().state(stateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(ClusterStateResponse response) {
                IndexMetadata md = response.getState().metadata().index(indexName);
                if (md == null) {
                    // Race: the index disappeared between create and state.
                    // Treat as conflict rather than pretend attach succeeded.
                    sendError(channel, RestStatus.CONFLICT, "index " + indexName + " conflicts with a concurrent request");
                    return;
                }
                String existing = md.getSettings().get(LanceEngineFactory.TABLE_SETTING);
                if (existing == null) {
                    sendError(
                        channel,
                        RestStatus.CONFLICT,
                        "index " + indexName + " already exists and is not a Lance index; choose a different `name`"
                    );
                    return;
                }
                if (!existing.equals(table)) {
                    sendError(channel, RestStatus.CONFLICT, "index " + indexName + " already attached to a different table: " + existing);
                    return;
                }
                // Same table, so record the (index, table) pair with the
                // namespace poller in case this node has forgotten it
                // (cluster restart after attach, for example). Pinned
                // indices are readonly snapshots and stay outside the
                // poll cycle so a manifest advance does not race with
                // the intended version.
                if (pinnedVersion.isEmpty()) {
                    namespaceService.registerAttachedIndex(indexName, table, derivation.version, storageOptions);
                }
                writeAttachResponse(channel, indexName, table, derivation, true);
            }

            @Override
            public void onFailure(Exception e) {
                sendError(channel, e);
            }
        });
    }

    private static boolean isAlreadyExists(Throwable e) {
        Throwable cursor = e;
        while (cursor != null) {
            if (cursor instanceof ResourceAlreadyExistsException) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private static void writeAttachResponse(
        RestChannel channel,
        String indexName,
        String table,
        Derivation derivation,
        boolean alreadyAttached
    ) {
        try (XContentBuilder b = channel.newBuilder()) {
            b.startObject();
            b.field("index", indexName);
            b.field("table", table);
            b.field("version", derivation.version);
            b.field("rows", derivation.rows);
            b.field("fragments", derivation.fragments);
            b.field("derived_key_field", derivation.keyField);
            b.rawField(
                "derived_mapping",
                new java.io.ByteArrayInputStream(derivation.mappingJson.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                org.opensearch.core.xcontent.MediaTypeRegistry.JSON
            );
            b.field("notes", derivation.notes);
            b.field("already_attached", alreadyAttached);
            b.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, b));
        } catch (Exception e) {
            sendError(channel, e);
        }
    }

    private static String readOptionalString(Map<String, Object> body, String key) {
        Object v = body.get(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof String)) {
            throw new IllegalArgumentException("[" + key + "] must be a string, got " + v.getClass().getSimpleName());
        }
        return (String) v;
    }

    private static Long readOptionalLong(Map<String, Object> body, String key) {
        Object v = body.get(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof Number)) {
            throw new IllegalArgumentException("[" + key + "] must be a number, got " + v.getClass().getSimpleName());
        }
        return ((Number) v).longValue();
    }

    private static void sendError(RestChannel channel, Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (Exception inner) {
            // channel already closed
        }
    }

    private static void sendError(RestChannel channel, RestStatus status, String message) {
        try {
            channel.sendResponse(new BytesRestResponse(status, message));
        } catch (Exception inner) {
            // channel already closed
        }
    }

    public record Derivation(String mappingJson, String keyField, String keyFieldType, String multiFieldsJson, long version, long rows,
        int fragments, List<String> notes, java.util.Set<String> ftsColumns, java.util.Set<String> scalarColumns, java.util.Set<
            String> vectorColumns) {
    }

    public static Derivation derive(Dataset dataset) throws Exception {
        return derive(dataset, java.util.Collections.emptyMap());
    }

    /**
     * Extended derive that honours the {@code multi_fields} clause on the
     * attach body. {@code multiFields} keys are base column names in the
     * Lance schema; each value is an ordered map of sub-field-name to
     * sub-field type (only {@code "keyword"} is accepted today; see design
     * note 36). The base column must resolve to a Utf8 mapping (either
     * {@code lance_text} or {@code keyword}); anything else is rejected
     * with {@link IllegalArgumentException} so a mistyped body surfaces as
     * a 400 rather than a silently unusable index. The mapping JSON gets a
     * {@code fields} block per base column, and the derivation carries a
     * JSON stringified form of the multi-fields spec so the engine can
     * rehydrate it on shard open.
     */
    public static Derivation derive(Dataset dataset, java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields)
        throws Exception {
        long rows = dataset.countRows();
        int fragments = dataset.getFragments().size();

        String keyField = null;
        // Track the Arrow type family of the declared primary key so the
        // engine can pick the right lookup strategy (signed integer via
        // Long.parseLong / Utf8 via SQL-quoted string). The setting is
        // ignored by the engine when keyField is empty, so leaving the
        // default "long" here matches the pre-#24 behaviour for tables
        // that never declared a PK.
        String keyFieldType = "long";
        java.util.List<String> notes = new java.util.ArrayList<>();
        java.util.Set<String> ftsColumns = new java.util.LinkedHashSet<>();
        java.util.Set<String> scalarColumns = new java.util.LinkedHashSet<>();
        java.util.Set<String> vectorColumns = new java.util.LinkedHashSet<>();

        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject().startObject("properties");
        LanceSchema lanceSchema = dataset.getLanceSchema();
        for (LanceField field : lanceSchema.fields()) {
            ArrowType type = field.getType();
            String name = field.getName();
            int fieldId = field.getId();
            boolean declaredPk = field.getMetadata() != null && field.getMetadata().containsKey(PK_METADATA_KEY);
            if (declaredPk) {
                // Lance's Rust schema validator refuses a table whose
                // primary key column is nullable
                // ("Primary key column and all its ancestors must not
                // be nullable", `rust/lance-core/src/datatypes/schema.rs`
                // in Lance 11.0.0). The validator fires both when
                // `Dataset.create` writes the schema and when
                // `Dataset.open` reads it back. On Lance 11 that
                // means a fresh nullable-PK table cannot be produced
                // through the current SDK, and any older or
                // hand-crafted table that does carry the shape
                // fails at attach's initial `Dataset.open` inside
                // `LanceRegistry.openDataset` with a
                // `LanceError(Schema)` wrapped in a generic
                // 500 response. That earlier path is loud enough
                // that the "attach 200 → shard settles red on
                // recovery" scenario the issue described is not
                // reachable through Lance 11 anymore.
                //
                // The check below is defense in depth for the
                // narrow case where the reader accepts a schema
                // with a nullable PK metadata attribute (an older
                // Lance format the current reader is permissive
                // about, or a schema that got the metadata
                // attached post-write) and still reaches
                // `derive`. When it does fire, a 400 that names
                // the column and the metadata key is a far
                // better operator experience than the
                // recovery-time `LanceError(Schema)` that
                // motivated the issue in the first place.
                //
                // Complementary note: only field-level
                // `lance-schema:unenforced-primary-key` metadata
                // is honoured. Schema-level metadata on the
                // Arrow root is silently ignored by both the
                // Lance reader and this loop; the attach guide
                // documents that so writers do not chase a
                // missing PK caused by placing the marker on
                // the wrong Arrow object.
                if (field.isNullable()) {
                    throw new IllegalArgumentException(
                        "column ["
                            + name
                            + "] is declared as primary key but is nullable; Lance requires the primary key column and "
                            + "all its ancestors to be non-nullable. Rewrite the table with a non-nullable primary key "
                            + "column, or drop the ["
                            + PK_METADATA_KEY
                            + "] metadata to attach it as a PK-less table."
                    );
                }
                // Signed integers up to 64 bits, unsigned 64 bit
                // integers, and Utf8 are the three PK shapes the
                // engine knows how to look up. Any other type gets
                // recorded as a note and left off keyField so the
                // engine treats the table as PK-less rather than
                // trying to serve GET on an unsupported column.
                // Unsigned int8 / int16 / int32 PKs are out of
                // scope for #24 too (no OpenSearch mapping type
                // covers unsigned <64 bit); they fall through to
                // the note branch.
                if (type instanceof ArrowType.Int intType && intType.getIsSigned() && intType.getBitWidth() <= 64) {
                    keyField = name;
                    keyFieldType = "long";
                } else if (type instanceof ArrowType.Int intType && !intType.getIsSigned() && intType.getBitWidth() == 64) {
                    keyField = name;
                    keyFieldType = "unsigned_long";
                } else if (type instanceof ArrowType.Utf8) {
                    keyField = name;
                    keyFieldType = "keyword";
                } else {
                    notes.add(
                        "column "
                            + name
                            + " is declared as primary key with unsupported Arrow type "
                            + type
                            + "; GET /_doc returns 404 and _id falls back to \"<fragment>-<offset>\""
                    );
                }
            }
            if (type instanceof ArrowType.Int intType) {
                int bitWidth = intType.getBitWidth();
                if (!intType.getIsSigned()) {
                    if (bitWidth == 64 && name.equals(keyField)) {
                        // UInt64 PK column: surface it as unsigned_long
                        // so term / range / sort / aggregation resolve
                        // through OpenSearch's built-in unsigned_long
                        // machinery. Non-PK UInt64 columns still stay
                        // unsurfaced today (they would need the reader
                        // to distinguish signed vs unsigned on read).
                        startFieldWithId(mapping, name, fieldId, "unsigned_long", arrowTypeIdentity(intType));
                        mapping.field("index", false).field("doc_values", true).endObject();
                        scalarColumns.add(name);
                        continue;
                    }
                    notes.add(
                        "column "
                            + name
                            + ": unsigned int"
                            + bitWidth
                            + " not surfaced (OpenSearch has no unsigned equivalent for byte/short/int, and non-PK unsigned_long doc values still require reader work)"
                    );
                    continue;
                }
                String osType;
                switch (bitWidth) {
                    case 8:
                        osType = "byte";
                        break;
                    case 16:
                        osType = "short";
                        break;
                    case 32:
                        osType = "integer";
                        break;
                    case 64:
                        osType = "long";
                        break;
                    default:
                        notes.add("column " + name + ": unsupported int width " + bitWidth);
                        continue;
                }
                startFieldWithId(mapping, name, fieldId, osType, arrowTypeIdentity(intType));
                mapping.field("index", false).field("doc_values", true).endObject();
                scalarColumns.add(name);
            } else if (type instanceof ArrowType.Bool) {
                startFieldWithId(mapping, name, fieldId, "boolean", arrowTypeIdentity(type));
                mapping.field("index", false).field("doc_values", true).endObject();
                scalarColumns.add(name);
            } else if (type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp) {
                // Date32/Date64 and every Timestamp unit are normalized to epoch millis
                // by the reader, so the default epoch_millis-friendly format applies.
                startFieldWithId(mapping, name, fieldId, "date", arrowTypeIdentity(type));
                mapping.field("index", false).field("doc_values", true).endObject();
                scalarColumns.add(name);
            } else if (type instanceof ArrowType.Utf8) {
                boolean hasFts = !dataset.describeIndices(new IndexCriteria.Builder().forColumn(name).mustSupportFts(true).build())
                    .isEmpty();
                if (hasFts) {
                    startFieldWithId(mapping, name, fieldId, "lance_text", arrowTypeIdentity(type));
                    writeMultiFieldsBlock(mapping, name, multiFields);
                    mapping.endObject();
                    ftsColumns.add(name);
                } else {
                    startFieldWithId(mapping, name, fieldId, "keyword", arrowTypeIdentity(type));
                    mapping.field("index", false).field("doc_values", true);
                    writeMultiFieldsBlock(mapping, name, multiFields);
                    mapping.endObject();
                    scalarColumns.add(name);
                }
            } else if (type instanceof ArrowType.FixedSizeList fsl) {
                // LanceField.getChildren() returns an empty list for
                // FixedSizeList columns — the item type is carried through
                // logicalType and materialised only when asArrowField() is
                // called. Go through the Arrow representation so we can
                // inspect the element type.
                org.apache.arrow.vector.types.pojo.Field arrow = field.asArrowField();
                ArrowType childType = arrow.getChildren().isEmpty() ? null : arrow.getChildren().get(0).getType();
                boolean float32 = childType instanceof ArrowType.FloatingPoint fp
                    && fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;
                if (float32) {
                    // Surface the column through the `lance_vector` field type
                    // so `LanceKnnQueryBuilder` can validate the field name
                    // and dimension against the mapping. The `dimension`
                    // parameter is required by the mapper; the query builder
                    // will reject a knn call whose vector length does not
                    // match.
                    startFieldWithId(mapping, name, fieldId, "lance_vector", "fixed_size_list<float32>[" + fsl.getListSize() + "]");
                    mapping.field("dimension", fsl.getListSize());
                    mapping.field("element_type", "Float32");
                    mapping.endObject();
                    vectorColumns.add(name);
                } else {
                    // Lance itself supports int8 / uint8 / float16 vectors, but the
                    // Java SDK's Query.Builder.setKey only takes float[], so a
                    // lance_knn call would hit a 500 inside Lance ("Column X has
                    // element type Y and the query vector is Float32"). Surface the
                    // column so the operator sees it, but leave it out of
                    // vectorColumns so no auto vector index is attempted and
                    // ensureVectorIndexes / optimizeExistingVectorIndexes skip it.
                    String elementType = childType == null ? "unknown" : childType.toString();
                    notes.add(
                        name
                            + ": vector column, dimension "
                            + fsl.getListSize()
                            + ", element type "
                            + elementType
                            + ", not queryable through lance_knn (Java SDK requires Float32)"
                    );
                }
            } else if (type instanceof ArrowType.List
                && field.getChildren().size() == 1
                && field.getChildren().get(0).getType() instanceof ArrowType.Utf8) {
                    // List<Utf8> maps to keyword. OpenSearch's keyword is multi-valued
                    // through SortedSetDocValues, and Lance stores the element list per
                    // row in a ListVector, so no additional mapping option is needed.
                    startFieldWithId(mapping, name, fieldId, "keyword", "list<utf8>");
                    mapping.field("index", false).field("doc_values", true).endObject();
                    scalarColumns.add(name);
                } else if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
                    // Binary / LargeBinary map to OpenSearch's binary type: the value is
                    // base64-encoded in _source and neither indexed nor loaded into doc
                    // values. Users can still retrieve raw bytes through _source.
                    startFieldWithId(mapping, name, fieldId, "binary", arrowTypeIdentity(type));
                    mapping.endObject();
                } else {
                    notes.add(name + ": " + type + ", stored only");
                }
        }
        mapping.endObject().endObject();

        if (keyField == null) {
            keyField = "";
            keyFieldType = "long";
            notes.add("no primary key declared; _id GET returns 404, `_id` values are not unique");
        }

        // Validate multi_fields against the actual schema: every declared
        // base column must exist and must be Utf8, because keyword-flavoured
        // sub-fields only make sense on a string column (they share the
        // underlying data with the base field). Sub-field type must be
        // "keyword" today; other types will land alongside issue #7 /#6.
        // Also refuse a sub-field name that collides with an existing
        // field id so the mapping stays unambiguous.
        java.util.Set<String> allColumnNames = new java.util.HashSet<>();
        for (LanceField field : lanceSchema.fields()) {
            allColumnNames.add(field.getName());
        }
        for (java.util.Map.Entry<String, java.util.LinkedHashMap<String, String>> entry : multiFields.entrySet()) {
            String baseName = entry.getKey();
            if (!allColumnNames.contains(baseName)) {
                throw new IllegalArgumentException("multi_fields references unknown column [" + baseName + "]");
            }
            if (!ftsColumns.contains(baseName) && !isKeywordScalar(lanceSchema, baseName)) {
                throw new IllegalArgumentException(
                    "multi_fields column [" + baseName + "] must be Utf8; other Arrow types cannot host a keyword sub-field"
                );
            }
            for (java.util.Map.Entry<String, String> sub : entry.getValue().entrySet()) {
                String subName = sub.getKey();
                String subType = sub.getValue();
                if (!"keyword".equals(subType)) {
                    throw new IllegalArgumentException(
                        "multi_fields sub-field [" + baseName + "." + subName + "] type must be [keyword], got [" + subType + "]"
                    );
                }
                if (allColumnNames.contains(baseName + "." + subName)) {
                    throw new IllegalArgumentException(
                        "multi_fields sub-field [" + baseName + "." + subName + "] collides with an existing schema column"
                    );
                }
            }
        }

        String multiFieldsJson = serialiseMultiFields(multiFields);

        return new Derivation(
            mapping.toString(),
            keyField,
            keyFieldType,
            multiFieldsJson,
            dataset.version(),
            rows,
            fragments,
            notes,
            ftsColumns,
            scalarColumns,
            vectorColumns
        );
    }

    /**
     * True when {@code baseName} exists in the schema as a Utf8 column
     * without a Lance FTS index (i.e. derivation maps it to keyword).
     * Utf8 with FTS is picked up separately via {@code ftsColumns.contains}
     * on the caller side.
     */
    private static boolean isKeywordScalar(LanceSchema lanceSchema, String baseName) {
        for (LanceField field : lanceSchema.fields()) {
            if (baseName.equals(field.getName())) {
                return field.getType() instanceof ArrowType.Utf8;
            }
        }
        return false;
    }

    /**
     * Parse the {@code multi_fields} block on the attach body into a
     * base column → ordered map (sub-field name → sub-field type) form
     * derived from the request map. Rejects malformed shapes with
     * {@link IllegalArgumentException} so the REST layer surfaces them
     * as a 400.
     *
     * <p>Expected shape:
     * <pre>
     *   "multi_fields": {
     *     "body": { "raw": { "type": "keyword" } }
     *   }
     * </pre>
     */
    public static java.util.Map<String, java.util.LinkedHashMap<String, String>> parseMultiFields(Object raw) {
        return parseSubFieldsBody(raw, "multi_fields");
    }

    /**
     * Parse the {@code overrides} block on the attach body. This is the
     * forward-looking receiver for per-column mapping overrides (RFC
     * Mapping interface: "optional override rules where the defaults
     * resolve a column differently than intended"). Today it accepts
     * only sub-field declarations, so structurally it is a superset of
     * {@link #parseMultiFields}: {@code overrides.[col].fields.[sub].type}
     * takes exactly the same shape as
     * {@code multi_fields.[col].[sub].type}.
     *
     * <p>The clause is versioned by shape rather than by a flag:
     * {@code type} on the base column (for {@code ip}, {@code wildcard},
     * an analyzer mode, or a preferred index type) is not accepted yet
     * and returns 400. That reservation lets subsequent tickets
     * (#6 / #7 / #11) grow the receiver without another wire-format
     * change.
     *
     * <p>Returns the same normalised shape as
     * {@link #parseMultiFields} so callers can persist it through the
     * existing {@code index.lance.multi_fields} setting and re-derive
     * from it on version advance.
     */
    public static java.util.Map<String, java.util.LinkedHashMap<String, String>> parseOverrides(Object raw) {
        if (raw == null) {
            return java.util.Collections.emptyMap();
        }
        if (!(raw instanceof java.util.Map<?, ?> rawMap)) {
            throw new IllegalArgumentException("[overrides] must be an object; per-column override rules");
        }
        java.util.LinkedHashMap<String, java.util.LinkedHashMap<String, String>> out = new java.util.LinkedHashMap<>();
        for (java.util.Map.Entry<?, ?> entry : rawMap.entrySet()) {
            if (!(entry.getKey() instanceof String baseName) || baseName.isEmpty()) {
                throw new IllegalArgumentException("[overrides] keys must be non-empty column names");
            }
            if (!(entry.getValue() instanceof java.util.Map<?, ?> spec)) {
                throw new IllegalArgumentException("[overrides." + baseName + "] must be an object");
            }
            if (spec.containsKey("type")) {
                // Base-column type override (ip / wildcard / analyzer /
                // scalar / vector index type) is reserved but not
                // implemented yet. Explicitly refuse rather than
                // silently ignore so an operator experimenting today
                // knows to wait for #6 / #7 / #11.
                Object t = spec.get("type");
                throw new IllegalArgumentException(
                    "[overrides."
                        + baseName
                        + ".type="
                        + t
                        + "] is not supported yet; only [overrides."
                        + baseName
                        + ".fields] is accepted today"
                );
            }
            Object rawFields = spec.get("fields");
            if (rawFields == null) {
                // Empty override entry is not useful; refuse so a stray
                // `"body": {}` does not silently accomplish nothing.
                throw new IllegalArgumentException("[overrides." + baseName + "] must declare at least [fields]");
            }
            if (!(rawFields instanceof java.util.Map<?, ?> fieldsMap)) {
                throw new IllegalArgumentException("[overrides." + baseName + ".fields] must be an object");
            }
            java.util.LinkedHashMap<String, String> subs = new java.util.LinkedHashMap<>();
            for (java.util.Map.Entry<?, ?> subEntry : fieldsMap.entrySet()) {
                if (!(subEntry.getKey() instanceof String subName) || subName.isEmpty()) {
                    throw new IllegalArgumentException("[overrides." + baseName + ".fields] sub-field names must be non-empty strings");
                }
                if (!(subEntry.getValue() instanceof java.util.Map<?, ?> subDefMap)) {
                    throw new IllegalArgumentException("[overrides." + baseName + ".fields." + subName + "] must be an object");
                }
                Object typeValue = subDefMap.get("type");
                if (!(typeValue instanceof String typeStr) || typeStr.isEmpty()) {
                    throw new IllegalArgumentException(
                        "[overrides." + baseName + ".fields." + subName + ".type] is required and must be a string"
                    );
                }
                subs.put(subName, typeStr);
            }
            if (!subs.isEmpty()) {
                out.put(baseName, subs);
            }
        }
        return out;
    }

    /**
     * Shared parser body for the two attach clauses that describe
     * sub-fields ({@code multi_fields} and {@code overrides.[col].fields}).
     * Kept as one helper so the two clauses agree on validation rules
     * without duplicating the loop.
     */
    private static java.util.Map<String, java.util.LinkedHashMap<String, String>> parseSubFieldsBody(Object raw, String clauseName) {
        if (raw == null) {
            return java.util.Collections.emptyMap();
        }
        if (!(raw instanceof java.util.Map<?, ?> rawMap)) {
            throw new IllegalArgumentException("[" + clauseName + "] must be an object; per-column key → per-sub-field type mapping");
        }
        java.util.LinkedHashMap<String, java.util.LinkedHashMap<String, String>> out = new java.util.LinkedHashMap<>();
        for (java.util.Map.Entry<?, ?> entry : rawMap.entrySet()) {
            if (!(entry.getKey() instanceof String baseName) || baseName.isEmpty()) {
                throw new IllegalArgumentException("[" + clauseName + "] keys must be non-empty column names");
            }
            if (!(entry.getValue() instanceof java.util.Map<?, ?> subMap)) {
                throw new IllegalArgumentException("[" + clauseName + "." + baseName + "] must be an object of sub-field definitions");
            }
            java.util.LinkedHashMap<String, String> subs = new java.util.LinkedHashMap<>();
            for (java.util.Map.Entry<?, ?> subEntry : subMap.entrySet()) {
                if (!(subEntry.getKey() instanceof String subName) || subName.isEmpty()) {
                    throw new IllegalArgumentException("[" + clauseName + "." + baseName + "] sub-field names must be non-empty strings");
                }
                if (!(subEntry.getValue() instanceof java.util.Map<?, ?> subDefMap)) {
                    throw new IllegalArgumentException("[" + clauseName + "." + baseName + "." + subName + "] must be an object");
                }
                Object typeValue = subDefMap.get("type");
                if (!(typeValue instanceof String typeStr) || typeStr.isEmpty()) {
                    throw new IllegalArgumentException(
                        "[" + clauseName + "." + baseName + "." + subName + ".type] is required and must be a string"
                    );
                }
                subs.put(subName, typeStr);
            }
            if (!subs.isEmpty()) {
                out.put(baseName, subs);
            }
        }
        return out;
    }

    /**
     * Emit the {@code "fields": {...}} sub-block on the mapping for a
     * base column, when the operator declared sub-fields for it. No-op
     * when {@code multiFields} has no entry for {@code baseName}. Sub-field
     * definition uses the same shape as the top-level keyword mapping
     * ({@code index:false, doc_values:true}) so the field is queryable
     * via doc values without duplicating the source string.
     */
    private static void writeMultiFieldsBlock(
        XContentBuilder mapping,
        String baseName,
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields
    ) throws Exception {
        java.util.LinkedHashMap<String, String> subs = multiFields.get(baseName);
        if (subs == null || subs.isEmpty()) {
            return;
        }
        mapping.startObject("fields");
        for (java.util.Map.Entry<String, String> sub : subs.entrySet()) {
            mapping.startObject(sub.getKey());
            mapping.field("type", sub.getValue());
            mapping.field("index", false);
            mapping.field("doc_values", true);
            mapping.endObject();
        }
        mapping.endObject();
    }

    /**
     * Compact JSON stringify of the multi-fields spec so it can be
     * carried through an OpenSearch index setting (which only accepts
     * strings). Empty on empty input so the caller can decide whether
     * to write the setting at all.
     */
    public static String serialiseMultiFields(java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields) {
        if (multiFields.isEmpty()) {
            return "";
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (java.util.Map.Entry<String, java.util.LinkedHashMap<String, String>> entry : multiFields.entrySet()) {
                builder.startObject(entry.getKey());
                for (java.util.Map.Entry<String, String> sub : entry.getValue().entrySet()) {
                    builder.field(sub.getKey(), sub.getValue());
                }
                builder.endObject();
            }
            builder.endObject();
            return builder.toString();
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialise multi_fields", e);
        }
    }

    /**
     * Parse the compact JSON form back into the base → (sub → type) map.
     * Reverse of {@link #serialiseMultiFields}. Empty on empty input,
     * throws {@link IllegalArgumentException} on malformed JSON so the
     * engine startup path can surface it as a shard-open failure.
     */
    public static java.util.Map<String, java.util.LinkedHashMap<String, String>> deserialiseMultiFields(String stringified) {
        if (stringified == null || stringified.isEmpty()) {
            return java.util.Collections.emptyMap();
        }
        try (
            org.opensearch.core.xcontent.XContentParser parser = org.opensearch.core.xcontent.MediaTypeRegistry.JSON.xContent()
                .createParser(org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY, null, stringified)
        ) {
            java.util.Map<String, Object> raw = parser.map();
            java.util.LinkedHashMap<String, java.util.LinkedHashMap<String, String>> out = new java.util.LinkedHashMap<>();
            for (java.util.Map.Entry<String, Object> entry : raw.entrySet()) {
                if (!(entry.getValue() instanceof java.util.Map<?, ?> subs)) {
                    throw new IllegalArgumentException("multi_fields[" + entry.getKey() + "] is not an object");
                }
                java.util.LinkedHashMap<String, String> flattened = new java.util.LinkedHashMap<>();
                for (java.util.Map.Entry<?, ?> sub : subs.entrySet()) {
                    flattened.put(String.valueOf(sub.getKey()), String.valueOf(sub.getValue()));
                }
                out.put(entry.getKey(), flattened);
            }
            return out;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse index.lance.multi_fields JSON: " + e.getMessage(), e);
        }
    }

    private static String tableName(String table) {
        String base = table.substring(table.lastIndexOf('/') + 1);
        return base.endsWith(".lance") ? base.substring(0, base.length() - 6) : base;
    }

    /**
     * Emit the leading portion of a field mapping and stash the Lance field id
     * plus the Arrow type name in the OpenSearch field meta so subsequent
     * checkouts can distinguish a column rename (same id, same type, different
     * name) from a schema reset (same id, different type) and mark stale
     * mappings as {@code lance_dropped}. See
     * {@link LanceNamespaceService#syncTable}. Callers finish the field with
     * any type specific options and a matching {@code endObject()}.
     *
     * <p>OpenSearch's field {@code meta} only accepts string values, so both
     * fields are stringified.
     */
    private static String arrowTypeIdentity(ArrowType type) {
        // Return a stable, comparable string that identifies an Arrow type
        // to the degree that matters for column identity across schema
        // revisions. Two same-id fields whose {@link #arrowTypeIdentity}
        // agree are treated as the same column; a mismatch signals a
        // schema reset (drop + re-add reusing the field id).
        //
        // {@link ArrowType#toString()} already covers Int width and
        // signedness, Timestamp unit, Date width, and the FloatingPoint
        // precision, which are the parts of the type that alter the
        // OpenSearch mapping. FixedSizeList and List identities are built
        // by the callers because their identity includes the child type.
        return type.toString();
    }

    private static void startFieldWithId(XContentBuilder mapping, String name, int fieldId, String type) throws Exception {
        startFieldWithId(mapping, name, fieldId, type, null);
    }

    private static void startFieldWithId(XContentBuilder mapping, String name, int fieldId, String type, String arrowType)
        throws Exception {
        mapping.startObject(name).field("type", type);
        mapping.startObject("meta").field("lance_field_id", Integer.toString(fieldId));
        if (arrowType != null) {
            mapping.field("lance_arrow_type", arrowType);
        }
        mapping.endObject();
    }
}

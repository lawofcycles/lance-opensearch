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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceRegistry;
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
 * numeric doc values fields), the shard count follows table size, and the
 * primary key is detected from Lance field metadata. Optional overrides:
 * "name" (index name, defaults to the table directory name) and
 * "number_of_shards" (pins the derived count).
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
    private static final long ROWS_PER_SHARD_TARGET = 500_000;

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
        Number pinnedShards;
        try {
            table = readOptionalString(body, "table");
            if (table == null || table.isEmpty()) {
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "[table] is required"));
            }
            explicitName = readOptionalString(body, "name");
            pinnedShards = readOptionalNumber(body, "number_of_shards");
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
        final Number pinnedShardsFinal = pinnedShards;

        // Dispatch the JNI work to the generic pool. Dataset.open blocks on
        // native I/O and would trip the transport-thread assertion otherwise.
        return channel -> threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            Derivation derivation;
            try (Dataset dataset = Dataset.open().allocator(LanceRegistry.allocator()).uri(tableFinal).build()) {
                derivation = derive(dataset, pinnedShardsFinal);
            } catch (Exception e) {
                sendError(channel, e);
                return;
            }
            createIndex(client, channel, indexName, tableFinal, derivation, namespaceService);
        });
    }

    private static void createIndex(
        NodeClient client,
        RestChannel channel,
        String indexName,
        String table,
        Derivation derivation,
        LanceNamespaceService namespaceService
    ) {
        CreateIndexRequest create = new CreateIndexRequest(indexName).settings(
            Settings.builder()
                .put("index.number_of_shards", derivation.shards)
                .put("index.number_of_replicas", 0)
                .put(LanceEngineFactory.TABLE_SETTING, table)
                .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, derivation.keyField)
                .build()
        ).mapping(derivation.mappingJson);

        client.admin().indices().create(create, new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse response) {
                // Register the attach-created index with the namespace
                // poller so subsequent appends surface without a manual
                // /_refresh. Idempotent: repeated attaches with the same
                // (name, table) just refresh the served version.
                namespaceService.registerAttachedIndex(indexName, table, derivation.version);
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
                verifyExistingLanceIndex(client, channel, indexName, table, derivation, namespaceService);
            }
        });
    }

    private static void verifyExistingLanceIndex(
        NodeClient client,
        RestChannel channel,
        String indexName,
        String table,
        Derivation derivation,
        LanceNamespaceService namespaceService
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
                // (cluster restart after attach, for example).
                namespaceService.registerAttachedIndex(indexName, table, derivation.version);
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
            b.field("derived_number_of_shards", derivation.shards);
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

    private static Number readOptionalNumber(Map<String, Object> body, String key) {
        Object v = body.get(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof Number)) {
            throw new IllegalArgumentException("[" + key + "] must be a number, got " + v.getClass().getSimpleName());
        }
        return (Number) v;
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

    public record Derivation(String mappingJson, int shards, String keyField, long version, long rows, int fragments, List<String> notes,
        java.util.Set<String> ftsColumns, java.util.Set<String> scalarColumns, java.util.Set<String> vectorColumns) {
    }

    public static Derivation derive(Dataset dataset, Number pinnedShards) throws Exception {
        long rows = dataset.countRows();
        int fragments = dataset.getFragments().size();
        int shards = pinnedShards != null
            ? pinnedShards.intValue()
            : (int) Math.max(1, Math.min(fragments == 0 ? 1 : fragments, (rows + ROWS_PER_SHARD_TARGET - 1) / ROWS_PER_SHARD_TARGET));

        String keyField = null;
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
                keyField = name;
            }
            if (type instanceof ArrowType.Int intType) {
                int bitWidth = intType.getBitWidth();
                if (!intType.getIsSigned()) {
                    notes.add(
                        "column "
                            + name
                            + ": unsigned int"
                            + bitWidth
                            + " not surfaced (OpenSearch has no unsigned equivalent for byte/short/int, and unsigned_long doc values require BigInteger which the reader does not synthesise yet)"
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
                    mapping.endObject();
                    ftsColumns.add(name);
                } else {
                    startFieldWithId(mapping, name, fieldId, "keyword", arrowTypeIdentity(type));
                    mapping.field("index", false).field("doc_values", true).endObject();
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
            notes.add("no primary key declared; _id GET returns 404, `_id` values are not unique");
        }
        return new Derivation(
            mapping.toString(),
            shards,
            keyField,
            dataset.version(),
            rows,
            fragments,
            notes,
            ftsColumns,
            scalarColumns,
            vectorColumns
        );
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
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
 */
public class RestAttachAction extends BaseRestHandler {

    private static final String PK_METADATA_KEY = "lance-schema:unenforced-primary-key";
    private static final long ROWS_PER_SHARD_TARGET = 500_000;

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
        Map<String, Object> body = XContentHelper.convertToMap(request.content(), false, request.getMediaType()).v2();
        String table = (String) body.get("table");
        String explicitName = (String) body.get("name");
        Number pinnedShards = (Number) body.get("number_of_shards");

        return channel -> {
            String indexName = explicitName != null ? explicitName : tableName(table);
            Derivation derivation;
            try (Dataset dataset = Dataset.open(table, LanceRegistry.allocator())) {
                derivation = derive(dataset, pinnedShards);
            }

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
                    writeAttachResponse(channel, indexName, table, derivation, false);
                }

                @Override
                public void onFailure(Exception e) {
                    if (isAlreadyExists(e)) {
                        // The RegistrIy entry has been refreshed on the way in, so we treat
                        // repeated attach calls (either by the caller or after a cluster
                        // restart that lost the in-memory registry) as success.
                        writeAttachResponse(channel, indexName, table, derivation, true);
                        return;
                    }
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    } catch (Exception inner) {
                        // channel already closed
                    }
                }
            });
        };
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
        org.opensearch.rest.RestChannel channel,
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
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (Exception inner) {
                // channel already closed
            }
        }
    }

    record Derivation(String mappingJson, int shards, String keyField, long version, long rows, int fragments, List<String> notes,
        java.util.Set<String> ftsColumns, java.util.Set<String> scalarColumns, java.util.Set<String> vectorColumns) {
    }

    static Derivation derive(Dataset dataset, Number pinnedShards) throws Exception {
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
            if (type instanceof ArrowType.Int) {
                startFieldWithId(mapping, name, fieldId, "integer");
                mapping.field("index", false).field("doc_values", true).endObject();
                if (keyField == null) {
                    keyField = name;
                }
                scalarColumns.add(name);
            } else if (type instanceof ArrowType.Bool) {
                startFieldWithId(mapping, name, fieldId, "boolean");
                mapping.field("index", false).field("doc_values", true).endObject();
                scalarColumns.add(name);
            } else if (type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp) {
                // Date32/Date64 and every Timestamp unit are normalized to epoch millis
                // by the reader, so the default epoch_millis-friendly format applies.
                startFieldWithId(mapping, name, fieldId, "date");
                mapping.field("index", false).field("doc_values", true).endObject();
                scalarColumns.add(name);
            } else if (type instanceof ArrowType.Utf8) {
                boolean hasFts = !dataset.describeIndices(new IndexCriteria.Builder().forColumn(name).mustSupportFts(true).build())
                    .isEmpty();
                if (hasFts) {
                    startFieldWithId(mapping, name, fieldId, "lance_text");
                    mapping.endObject();
                    ftsColumns.add(name);
                } else {
                    startFieldWithId(mapping, name, fieldId, "keyword");
                    mapping.field("index", false).field("doc_values", true).endObject();
                    scalarColumns.add(name);
                }
            } else if (type instanceof ArrowType.FixedSizeList) {
                notes.add(
                    name + ": vector column, dimension " + ((ArrowType.FixedSizeList) type).getListSize() + ", queryable through lance_knn"
                );
                vectorColumns.add(name);
            } else if (type instanceof ArrowType.List
                && field.getChildren().size() == 1
                && field.getChildren().get(0).getType() instanceof ArrowType.Utf8) {
                    // List<Utf8> maps to keyword. OpenSearch's keyword is multi-valued
                    // through SortedSetDocValues, and Lance stores the element list per
                    // row in a ListVector, so no additional mapping option is needed.
                    startFieldWithId(mapping, name, fieldId, "keyword");
                    mapping.field("index", false).field("doc_values", true).endObject();
                    scalarColumns.add(name);
                } else if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
                    // Binary / LargeBinary map to OpenSearch's binary type: the value is
                    // base64-encoded in _source and neither indexed nor loaded into doc
                    // values. Users can still retrieve raw bytes through _source.
                    startFieldWithId(mapping, name, fieldId, "binary");
                    mapping.endObject();
                } else {
                    notes.add(name + ": " + type + ", stored only");
                }
        }
        mapping.endObject().endObject();

        if (keyField == null) {
            keyField = "id";
            notes.add("no integer column found; _id get unavailable");
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
     * Emit the leading portion of a field mapping and stash the Lance field id in the
     * OpenSearch field meta so subsequent checkouts can spot a rename on the Lance
     * side (see {@link LanceNamespaceService#syncTable}). Callers finish the field
     * with any type specific options and a matching {@code endObject()}.
     *
     * <p>OpenSearch's field {@code meta} only accepts string values, so the id is
     * stringified.
     */
    private static void startFieldWithId(XContentBuilder mapping, String name, int fieldId, String type) throws Exception {
        mapping.startObject(name).field("type", type);
        mapping.startObject("meta").field("lance_field_id", Integer.toString(fieldId)).endObject();
    }
}

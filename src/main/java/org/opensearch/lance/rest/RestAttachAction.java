/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.engine.LanceLocalClones;
import org.opensearch.lance.namespace.LanceNamespaceService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * POST /_lance/attach {"table": "/path/to/table.lance"}
 *
 * The RFC's attach operation. Derives everything from the table and creates
 * a real engine backed index. The mapping follows the derivation defaults
 * (a string column carrying an FTS index maps to text, integers map to
 * numeric doc values fields), the shard count is always one, and the
 * primary key is detected from Lance field metadata. Optional overrides:
 * "name" (index name, defaults to the table directory name), "version"
 * (pin a manifest version), "tag" (follow a Lance tag; not together
 * with "version") or "index_placement" ("node_local" builds and reads
 * search structures from per-node shallow clones so the source stays
 * read-only; default "in_table").
 *
 * <p>Attach always creates a single-shard index because the fragment path
 * (see {@code LanceDispatchActionFilter}) is the only search implementation
 * left, and it fans out to fragments regardless of shard count. Requests
 * carrying {@code number_of_shards} are rejected with 400.
 *
 * <p>The handler parses the body and hands a {@link LanceAttachRequest} to
 * {@link LanceAttachAction}; opening the table, deriving the mapping, and
 * creating the index happen in the transport action so a security plugin
 * evaluates the caller before any of that starts. The static
 * {@link #derive} helpers stay here because the namespace poller reuses
 * them for tables it surfaces on its own.
 */
public class RestAttachAction extends BaseRestHandler {

    private static final String PK_METADATA_KEY = "lance-schema:unenforced-primary-key";

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
        String tag;
        StorageOptions storageOptions;
        LanceOverrides overrides;
        String placement;
        try {
            table = readOptionalString(body, "table");
            if (table == null || table.isEmpty()) {
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "[table] is required"));
            }
            explicitName = readOptionalString(body, "name");
            if (body.containsKey("number_of_shards")) {
                // Lance-backed indices are single-shard and fan-out happens
                // per fragment, so a shard count has nothing to control.
                // Reject rather than silently ignore so an operator setting
                // `number_of_shards: 5` is not left wondering why fan-out
                // did not widen.
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
            tag = readOptionalString(body, "tag");
            if (tag != null && tag.isEmpty()) {
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "[tag] must not be empty"));
            }
            if (pinnedVersion != null && tag != null) {
                // `version` is a fixed pin, `tag` follows wherever the tag
                // points. The engine can honour only one of them per index.
                return channel -> channel.sendResponse(
                    new BytesRestResponse(RestStatus.BAD_REQUEST, "[version] and [tag] are mutually exclusive")
                );
            }
            storageOptions = StorageOptions.parseFromRequestField(body.get("storage_options"), "[lance_attach]");
            // `overrides` is the forward-looking clause; the legacy
            // `multi_fields` clause folds into `overrides.[col].fields`
            // at parse time so everything downstream sees one shape.
            // `indexes` declares per-column Lance index type preferences
            // and rides in the same LanceOverrides object (persisted in
            // the same setting).
            overrides = LanceOverrides.parseAttachClauses(body.get("overrides"), body.get("multi_fields"), body.get("indexes"));
            String indexPlacement = readOptionalString(body, "index_placement");
            if (indexPlacement != null
                && !LanceLocalClones.PLACEMENT_IN_TABLE.equals(indexPlacement)
                && !LanceLocalClones.PLACEMENT_NODE_LOCAL.equals(indexPlacement)) {
                return channel -> channel.sendResponse(
                    new BytesRestResponse(RestStatus.BAD_REQUEST, "[index_placement] must be 'in_table' or 'node_local'")
                );
            }
            placement = indexPlacement;
        } catch (IllegalArgumentException e) {
            String message = e.getMessage();
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, message));
        }

        LanceAttachRequest attach = new LanceAttachRequest(table, explicitName, pinnedVersion, tag, storageOptions, overrides, placement);
        return channel -> client.execute(LanceAttachAction.INSTANCE, attach, new RestToXContentListener<>(channel));
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

    public record Derivation(String mappingJson, String keyField, String keyFieldType, String overridesJson, long version, long rows,
        int fragments, List<String> notes, java.util.Set<String> ftsColumns, java.util.Set<String> scalarColumns, java.util.Set<
            String> vectorColumns, java.util.Set<String> nestedColumns) {
    }

    public static Derivation derive(Dataset dataset) throws Exception {
        return derive(dataset, LanceOverrides.EMPTY, false);
    }

    /** Strict derivation: any override the schema cannot honour is an {@link IllegalArgumentException} (a 400 at attach). */
    public static Derivation derive(Dataset dataset, LanceOverrides overrides) throws Exception {
        return derive(dataset, overrides, false);
    }

    /**
     * Extended derive that honours the per-column mapping overrides of the
     * attach or namespace-register body (see {@link LanceOverrides}).
     * {@code type: date} on a signed 32 or 64 bit integer column emits a
     * {@code date} mapping (the stored value is read as epoch millis) with
     * the declared {@code format} (default {@code epoch_millis}) while the
     * field meta keeps the real Arrow type; on a Date / Timestamp column
     * it pins the type the derivation picks anyway, so an override list
     * can be applied uniformly to several tables. {@code type: keyword} on
     * a Utf8 column maps it to {@code keyword} even when the column
     * carries a Lance inverted index, which also keeps it out of
     * {@code ftsColumns} so no FTS index is built or optimised for it; on
     * a List&lt;Utf8&gt; column it pins the derived type. Sub-field
     * declarations ({@code fields}) behave as the {@code multi_fields}
     * clause always has.
     *
     * <p>Schema-dependent validation happens here, where the dataset is
     * open. With {@code lenient} false every violation (unknown column,
     * primary key column, Arrow type outside the accepted set) is an
     * {@link IllegalArgumentException} the REST layer turns into a 400.
     * With {@code lenient} true, used by the namespace surface and the
     * poll re-derivation where one override list applies to many tables
     * and a table may lack or have changed a column, the offending
     * override is skipped with a note and the rest apply; the caller
     * keeps the full override list in the index setting so the column
     * picks the override back up if a later manifest restores it.
     */
    public static Derivation derive(Dataset dataset, LanceOverrides overrides, boolean lenient) throws Exception {
        Map<String, LinkedHashMap<String, String>> multiFields = overrides.subFields();
        long rows = dataset.countRows();
        int fragments = dataset.getFragments().size();

        String keyField = null;
        // Track the Arrow type family of the declared primary key so the
        // engine can pick the right lookup strategy (signed integer via
        // Long.parseLong / Utf8 via SQL-quoted string). The setting is
        // ignored by the engine when keyField is empty, so the default
        // "long" is harmless for tables that never declared a PK.
        String keyFieldType = "long";
        java.util.List<String> notes = new java.util.ArrayList<>();
        java.util.Set<String> ftsColumns = new java.util.LinkedHashSet<>();
        java.util.Set<String> scalarColumns = new java.util.LinkedHashSet<>();
        java.util.Set<String> vectorColumns = new java.util.LinkedHashSet<>();
        java.util.Set<String> nestedColumns = new java.util.LinkedHashSet<>();

        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject().startObject("properties");
        LanceSchema lanceSchema = dataset.getLanceSchema();
        LanceOverrides effective = validateOverrides(overrides, lanceSchema, lenient, notes);
        validateIndexPreferences(overrides.indexPreferences(), lanceSchema, lenient, notes);
        multiFields = effective.subFields();
        Map<String, String> dateOverrides = effective.dateColumns();
        java.util.Set<String> keywordOverrides = effective.keywordColumns();
        java.util.Set<String> ipOverrides = effective.ipColumns();
        java.util.Set<String> wildcardOverrides = effective.wildcardColumns();
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
                // Unsigned int8 / int16 / int32 PKs also fall through
                // to the note branch: no OpenSearch mapping type
                // covers unsigned <64 bit.
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
                if (dateOverrides.containsKey(name)) {
                    // Epoch millis stored as a signed integer column. The
                    // reader already serves the raw value through numeric
                    // doc values; the `date` field type interprets it as
                    // millis, so range / sort / date_histogram parse and
                    // format through the declared date format. The meta
                    // keeps the real Arrow type for column identity
                    // across manifest versions.
                    String format = dateOverrides.get(name);
                    startFieldWithId(mapping, name, fieldId, "date", arrowTypeIdentity(intType));
                    mapping.field("format", format == null ? LanceOverrides.DEFAULT_DATE_FORMAT : format);
                    mapping.field("index", false).field("doc_values", true).endObject();
                    scalarColumns.add(name);
                    continue;
                }
                startFieldWithId(mapping, name, fieldId, osType, arrowTypeIdentity(intType));
                mapping.field("index", false).field("doc_values", true).endObject();
                scalarColumns.add(name);
            } else if (type instanceof ArrowType.Bool) {
                startFieldWithId(mapping, name, fieldId, "boolean", arrowTypeIdentity(type));
                mapping.field("index", false).field("doc_values", true).endObject();
                scalarColumns.add(name);
            } else if (type instanceof ArrowType.FloatingPoint fp) {
                // Scalar float columns. FixedSizeList<float32> vectors go
                // through the branch below and pick up the lance_vector
                // mapping; this branch handles bare Float32 / Float64
                // columns. Both surface with
                // doc values enabled: OpenSearch's `float` / `double`
                // mappers pair with the NumericDocValues path the reader
                // already serves through the shared long[] storage
                // (see NumericPrecision inside LanceFragmentLeafReader
                // for the encoding trick). HALF stays unmapped because
                // the reader does not carry a half-precision decoder
                // today.
                String osType;
                switch (fp.getPrecision()) {
                    case SINGLE:
                        osType = "float";
                        break;
                    case DOUBLE:
                        osType = "double";
                        break;
                    case HALF:
                    default:
                        notes.add("column " + name + ": FloatingPoint precision " + fp.getPrecision() + " not surfaced");
                        continue;
                }
                startFieldWithId(mapping, name, fieldId, osType, arrowTypeIdentity(fp));
                mapping.field("index", false).field("doc_values", true).endObject();
                scalarColumns.add(name);
            } else if (type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp) {
                // Date32/Date64 and every Timestamp unit are normalized to epoch millis
                // by the reader, so the default epoch_millis-friendly format applies.
                // A `type: date` override on such a column pins the type the
                // derivation picks anyway; a declared `format` is emitted.
                startFieldWithId(mapping, name, fieldId, "date", arrowTypeIdentity(type));
                if (dateOverrides.containsKey(name) && dateOverrides.get(name) != null) {
                    mapping.field("format", dateOverrides.get(name));
                }
                mapping.field("index", false).field("doc_values", true).endObject();
                scalarColumns.add(name);
            } else if (type instanceof ArrowType.Utf8) {
                boolean hasFts = !dataset.describeIndices(new IndexCriteria.Builder().forColumn(name).mustSupportFts(true).build())
                    .isEmpty();
                if (ipOverrides.contains(name)) {
                    // Utf8 column holding IP address strings. The mapping
                    // becomes `ip` over doc values: the fragment reader
                    // parses each string with InetAddresses.forString and
                    // serves the 16 byte InetAddressPoint encoding through
                    // SortedSetDocValues, which is exactly the doc-values
                    // shape IpFieldType queries, sorts and aggregates
                    // over. The meta keeps the real Arrow type. Like a
                    // keyword override, the column leaves the FTS build
                    // and optimise targets.
                    startFieldWithId(mapping, name, fieldId, "ip", arrowTypeIdentity(type));
                    mapping.field("index", false).field("doc_values", true);
                    writeMultiFieldsBlock(mapping, name, multiFields);
                    mapping.endObject();
                    scalarColumns.add(name);
                } else if (hasFts && !keywordOverrides.contains(name) && !wildcardOverrides.contains(name)) {
                    startFieldWithId(mapping, name, fieldId, "lance_text", arrowTypeIdentity(type));
                    writeMultiFieldsBlock(mapping, name, multiFields);
                    mapping.endObject();
                    ftsColumns.add(name);
                } else {
                    // Either no FTS index, or the operator overrode the
                    // column to keyword or wildcard: map it onto the
                    // doc-values path and leave it out of ftsColumns so
                    // the index build paths do not create or optimise an
                    // FTS index for it. The Lance inverted index the
                    // table may carry stays untouched; this index just
                    // does not use it. A `wildcard` override emits the
                    // same keyword mapping (the reader has no postings
                    // for the n gram accelerated wildcard field type of
                    // OpenSearch core, and keyword doc values already
                    // answer wildcard / prefix / regexp / term) with the
                    // declared type recorded in the field meta.
                    String overrideType = wildcardOverrides.contains(name) ? LanceOverrides.TYPE_WILDCARD : null;
                    startFieldWithId(mapping, name, fieldId, "keyword", arrowTypeIdentity(type), overrideType);
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
                Field arrow = field.asArrowField();
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
                    // An `ip` override maps the multi-valued column to `ip` instead;
                    // the reader encodes each element like a scalar ip column.
                    String listType = ipOverrides.contains(name) ? "ip" : "keyword";
                    startFieldWithId(mapping, name, fieldId, listType, "list<utf8>");
                    mapping.field("index", false).field("doc_values", true).endObject();
                    scalarColumns.add(name);
                } else if (type instanceof ArrowType.List
                    && field.getChildren().size() == 1
                    && field.getChildren().get(0).getType() instanceof ArrowType.Struct) {
                        // List<Struct> maps to the nested field type: each
                        // element becomes a hidden child document of its
                        // row, so a nested query can match several
                        // attributes of the same element. Children follow
                        // the struct child derivation (Utf8 → keyword, no
                        // FTS / knn children, unsupported children noted);
                        // multi-valued children (List<Utf8>) and nested in
                        // nested (List<Struct> inside an element) are
                        // skipped with a note. Like the object mapper, the
                        // nested mapper accepts no `meta` parameter, so the
                        // identity metadata rides on the children.
                        LanceField element = field.getChildren().get(0);
                        if (nestedHasSupportedProperty(element)) {
                            mapping.startObject(name).field("type", "nested").startObject("properties");
                            writeNestedProperties(mapping, element, name, notes);
                            mapping.endObject().endObject();
                            nestedColumns.add(name);
                        } else {
                            notes.add(name + ": List<Struct> with no supported children, not surfaced");
                        }
                    } else if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
                        // Binary / LargeBinary map to OpenSearch's binary type: the value is
                        // base64-encoded in _source and neither indexed nor loaded into doc
                        // values. Users can still retrieve raw bytes through _source.
                        startFieldWithId(mapping, name, fieldId, "binary", arrowTypeIdentity(type));
                        mapping.endObject();
                    } else if (type instanceof ArrowType.Struct) {
                        // A Struct column surfaces as an `object` field whose
                        // properties derive from the struct's children,
                        // recursing into nested structs. OpenSearch's object
                        // mapper accepts no `meta` parameter, so the identity
                        // metadata (lance_field_id / lance_arrow_type) rides on
                        // the children instead; each Lance child field carries
                        // its own field id. Children the derivation does not
                        // support are noted and left out while the parent
                        // object is still emitted, unless no descendant is
                        // supported at all, in which case the whole column is
                        // skipped with a note (the reader keeps such a struct
                        // out of the row take, so an empty object mapping
                        // would never show up in _source).
                        if (structHasSupportedProperty(field)) {
                            mapping.startObject(name).field("type", "object").startObject("properties");
                            writeStructProperties(mapping, field, name, notes);
                            mapping.endObject().endObject();
                        } else {
                            notes.add(name + ": Struct with no supported children, not surfaced");
                        }
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

        String overridesJson = overrides.toJson();

        return new Derivation(
            mapping.toString(),
            keyField,
            keyFieldType,
            overridesJson,
            dataset.version(),
            rows,
            fragments,
            notes,
            ftsColumns,
            scalarColumns,
            vectorColumns,
            nestedColumns
        );
    }

    /**
     * Validate {@code overrides} against the actual schema and return the
     * overrides derivation applies. Strict mode ({@code lenient} false)
     * throws {@link IllegalArgumentException} on the first violation so
     * attach answers 400 naming the column and the reason; lenient mode
     * skips the offending column's override with a note and returns the
     * rest, so one override list can apply to several tables that do not
     * all carry every column.
     *
     * <p>Rules: the column must exist at the top level and must not be
     * the declared primary key; {@code type: date} needs a signed 32 or
     * 64 bit integer column (read as epoch millis) or a Date / Timestamp
     * column (a pin of the derived type); {@code type: keyword} needs a
     * Utf8 column (with or without an inverted index) or a List&lt;Utf8&gt;
     * column (a pin); {@code type: ip} needs a Utf8 or List&lt;Utf8&gt;
     * column whose strings are IP addresses; {@code type: wildcard}
     * needs a Utf8 column (it is served through the keyword mapping);
     * {@code fields} needs a Utf8
     * column, sub-field types must be {@code keyword}, and a sub-field
     * path must not collide with an existing schema column.
     */
    private static LanceOverrides validateOverrides(
        LanceOverrides overrides,
        LanceSchema lanceSchema,
        boolean lenient,
        List<String> notes
    ) {
        if (overrides.isEmpty()) {
            return overrides;
        }
        Map<String, LanceField> fieldsByName = new LinkedHashMap<>();
        for (LanceField field : lanceSchema.fields()) {
            fieldsByName.put(field.getName(), field);
        }
        LinkedHashMap<String, LanceOverrides.Column> accepted = new LinkedHashMap<>();
        for (Map.Entry<String, LanceOverrides.Column> entry : overrides.columns().entrySet()) {
            String baseName = entry.getKey();
            LanceOverrides.Column column = entry.getValue();
            try {
                LanceField field = fieldsByName.get(baseName);
                if (field == null) {
                    throw new IllegalArgumentException("[overrides] references unknown column [" + baseName + "]");
                }
                validateColumnOverride(baseName, column, field, fieldsByName.keySet());
                accepted.put(baseName, column);
            } catch (IllegalArgumentException e) {
                if (!lenient) {
                    throw e;
                }
                notes.add(baseName + ": override skipped (" + e.getMessage() + ")");
            }
        }
        return LanceOverrides.fromColumns(accepted);
    }

    /**
     * Validate one column's override against the Lance field that
     * currently carries the column name. Shared by attach-time
     * validation and by the namespace poll, which re-checks a stored
     * override after a schema reset changed the column's Arrow type.
     *
     * @param baseName the column name the override is keyed by
     * @param column the override's rules
     * @param field the Lance field of that name in the current schema
     * @param schemaColumnNames every top-level column name of the
     *     current schema, for the sub-field collision check
     * @throws IllegalArgumentException naming the column and the reason
     *     when the field's type or metadata does not admit the override
     */
    public static void validateColumnOverride(
        String baseName,
        LanceOverrides.Column column,
        LanceField field,
        Set<String> schemaColumnNames
    ) {
        if (field.getMetadata() != null && field.getMetadata().containsKey(PK_METADATA_KEY)) {
            throw new IllegalArgumentException(
                "[overrides." + baseName + "] targets the primary key column; the primary key mapping cannot be overridden"
            );
        }
        ArrowType type = field.getType();
        if (LanceOverrides.TYPE_DATE.equals(column.type())) {
            boolean signedInt = type instanceof ArrowType.Int intType
                && intType.getIsSigned()
                && (intType.getBitWidth() == 32 || intType.getBitWidth() == 64);
            boolean alreadyDate = type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp;
            if (!signedInt && !alreadyDate) {
                throw new IllegalArgumentException(
                    "[overrides."
                        + baseName
                        + ".type=date] needs a signed 32 or 64 bit integer column holding epoch millis, or a "
                        + "Date / Timestamp column; ["
                        + baseName
                        + "] is "
                        + type
                        + " (unsigned integer columns are not surfaced by the reader)"
                );
            }
        }
        if (LanceOverrides.TYPE_KEYWORD.equals(column.type())) {
            boolean utf8 = type instanceof ArrowType.Utf8;
            boolean listOfUtf8 = type instanceof ArrowType.List
                && field.getChildren().size() == 1
                && field.getChildren().get(0).getType() instanceof ArrowType.Utf8;
            if (!utf8 && !listOfUtf8) {
                throw new IllegalArgumentException(
                    "[overrides." + baseName + ".type=keyword] needs a Utf8 or List<Utf8> column; [" + baseName + "] is " + type
                );
            }
        }
        if (LanceOverrides.TYPE_IP.equals(column.type())) {
            boolean utf8 = type instanceof ArrowType.Utf8;
            boolean listOfUtf8 = type instanceof ArrowType.List
                && field.getChildren().size() == 1
                && field.getChildren().get(0).getType() instanceof ArrowType.Utf8;
            if (!utf8 && !listOfUtf8) {
                throw new IllegalArgumentException(
                    "[overrides."
                        + baseName
                        + ".type=ip] needs a Utf8 or List<Utf8> column holding IP address strings; ["
                        + baseName
                        + "] is "
                        + type
                );
            }
        }
        if (LanceOverrides.TYPE_WILDCARD.equals(column.type()) && !(type instanceof ArrowType.Utf8)) {
            throw new IllegalArgumentException(
                "[overrides." + baseName + ".type=wildcard] needs a Utf8 column; [" + baseName + "] is " + type
            );
        }
        if (!column.subFields().isEmpty()) {
            if (!(type instanceof ArrowType.Utf8)) {
                throw new IllegalArgumentException(
                    "[overrides."
                        + baseName
                        + ".fields] column must be Utf8; other Arrow types cannot host a keyword sub-field, ["
                        + baseName
                        + "] is "
                        + type
                );
            }
            for (Map.Entry<String, String> sub : column.subFields().entrySet()) {
                if (!"keyword".equals(sub.getValue())) {
                    throw new IllegalArgumentException(
                        "sub-field [" + baseName + "." + sub.getKey() + "] type must be [keyword], got [" + sub.getValue() + "]"
                    );
                }
                if (schemaColumnNames.contains(baseName + "." + sub.getKey())) {
                    throw new IllegalArgumentException(
                        "sub-field [" + baseName + "." + sub.getKey() + "] collides with an existing schema column"
                    );
                }
            }
        }
    }

    /**
     * Validate the {@code indexes} clause's per-column index type
     * preferences against the actual schema. A {@code scalar} preference
     * needs a column the derivation classifies as scalar (signed
     * integer, float, boolean, Date / Timestamp, Utf8 or
     * List&lt;Utf8&gt;); a {@code vector} preference needs a
     * FixedSizeList&lt;Float32&gt; column. Strict mode ({@code lenient}
     * false) throws {@link IllegalArgumentException} naming the column
     * and its Arrow type, which attach answers as a 400; lenient mode
     * (namespace surface and poll re-derivation, where one preference
     * list applies to many tables) records a note and goes on. The full
     * preference list stays persisted either way, so a column that
     * appears in a later manifest picks its preference up.
     *
     * <p>A {@code scalar} preference on a Utf8 column that carries an
     * FTS index passes this check (the Arrow type admits a scalar
     * index) but never applies: the column classifies as
     * {@code lance_text} and only the FTS build targets it.
     */
    static void validateIndexPreferences(
        Map<String, LanceOverrides.IndexPreference> preferences,
        LanceSchema lanceSchema,
        boolean lenient,
        List<String> notes
    ) {
        if (preferences.isEmpty()) {
            return;
        }
        Map<String, LanceField> fieldsByName = new LinkedHashMap<>();
        for (LanceField field : lanceSchema.fields()) {
            fieldsByName.put(field.getName(), field);
        }
        for (Map.Entry<String, LanceOverrides.IndexPreference> entry : preferences.entrySet()) {
            String column = entry.getKey();
            LanceOverrides.IndexPreference preference = entry.getValue();
            try {
                LanceField field = fieldsByName.get(column);
                if (field == null) {
                    throw new IllegalArgumentException("[indexes] references unknown column [" + column + "]");
                }
                ArrowType type = field.getType();
                if (preference.scalar() != null && !isScalarIndexCapable(field, type)) {
                    throw new IllegalArgumentException(
                        "[indexes."
                            + column
                            + ".scalar="
                            + preference.scalar()
                            + "] needs a scalar column (signed integer, float, boolean, date, timestamp, Utf8 or List<Utf8>); ["
                            + column
                            + "] is "
                            + type
                    );
                }
                if (preference.vector() != null && !isVectorIndexCapable(field, type)) {
                    throw new IllegalArgumentException(
                        "[indexes."
                            + column
                            + ".vector="
                            + preference.vector()
                            + "] needs a FixedSizeList<Float32> column; ["
                            + column
                            + "] is "
                            + type
                    );
                }
            } catch (IllegalArgumentException e) {
                if (!lenient) {
                    throw e;
                }
                notes.add(column + ": index preference skipped (" + e.getMessage() + ")");
            }
        }
    }

    private static boolean isScalarIndexCapable(LanceField field, ArrowType type) {
        if (type instanceof ArrowType.Int intType) {
            return intType.getIsSigned() && intType.getBitWidth() <= 64;
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE || fp.getPrecision() == FloatingPointPrecision.DOUBLE;
        }
        if (type instanceof ArrowType.Bool || type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp) {
            return true;
        }
        if (type instanceof ArrowType.Utf8) {
            return true;
        }
        return type instanceof ArrowType.List
            && field.getChildren().size() == 1
            && field.getChildren().get(0).getType() instanceof ArrowType.Utf8;
    }

    private static boolean isVectorIndexCapable(LanceField field, ArrowType type) {
        if (!(type instanceof ArrowType.FixedSizeList)) {
            return false;
        }
        // LanceField.getChildren() is empty for FixedSizeList; the item
        // type only materialises through the Arrow representation.
        Field arrow = field.asArrowField();
        ArrowType childType = arrow.getChildren().isEmpty() ? null : arrow.getChildren().get(0).getType();
        return childType instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == FloatingPointPrecision.SINGLE;
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

    /**
     * Emit the {@code properties} entries of a Struct column, one per
     * supported child, recursing into nested structs. Children follow
     * the scalar derivation of the top-level loop with two deliberate
     * differences: Utf8 children always map to {@code keyword} (Lance
     * FTS indexes target top-level columns, so no {@code lance_text}
     * inside a struct), and {@code FixedSizeList} vector children are
     * skipped with a note ({@code lance_knn} does not reach struct
     * children). Any other child type the derivation does not support
     * is noted with its dotted path and left out; the parent object is
     * still emitted as long as at least one descendant is supported. A
     * struct child with no supported descendants is skipped whole with
     * one note, matching the reader, which keeps such a struct out of
     * the row take (an empty {@code properties} object would put a key
     * in the mapping that {@code _source} never renders).
     *
     * <p>The caller has already opened the {@code properties} object
     * and closes it after this returns.
     */
    private static void writeStructProperties(XContentBuilder mapping, LanceField structField, String path, List<String> notes)
        throws Exception {
        for (LanceField child : structField.getChildren()) {
            String childPath = path + "." + child.getName();
            ArrowType childType = child.getType();
            int childId = child.getId();
            if (childType instanceof ArrowType.Struct) {
                if (!structHasSupportedProperty(child)) {
                    notes.add(childPath + ": Struct with no supported children, not surfaced");
                    continue;
                }
                mapping.startObject(child.getName()).field("type", "object").startObject("properties");
                writeStructProperties(mapping, child, childPath, notes);
                mapping.endObject().endObject();
                continue;
            }
            String osType = structChildMappingType(child);
            if (osType == null) {
                if (childType instanceof ArrowType.FixedSizeList) {
                    notes.add(childPath + ": vector column inside a struct, not surfaced (lance_knn does not reach struct children)");
                } else {
                    notes.add(childPath + ": " + childType + ", not surfaced inside a struct");
                }
                continue;
            }
            String identity = childType instanceof ArrowType.List ? "list<utf8>" : arrowTypeIdentity(childType);
            startFieldWithId(mapping, child.getName(), childId, osType, identity);
            mapping.field("index", false).field("doc_values", true).endObject();
        }
    }

    /**
     * The OpenSearch mapping type of a non-struct struct child, or
     * {@code null} when the derivation does not support it inside a
     * struct. Shared by {@link #writeStructProperties} and
     * {@link #structHasSupportedProperty} so the two agree on what
     * counts as supported.
     */
    private static String structChildMappingType(LanceField child) {
        ArrowType childType = child.getType();
        if (childType instanceof ArrowType.Int intType && intType.getIsSigned()) {
            return switch (intType.getBitWidth()) {
                case 8 -> "byte";
                case 16 -> "short";
                case 32 -> "integer";
                case 64 -> "long";
                default -> null;
            };
        }
        if (childType instanceof ArrowType.Bool) {
            return "boolean";
        }
        if (childType instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> "float";
                case DOUBLE -> "double";
                default -> null;
            };
        }
        if (childType instanceof ArrowType.Date || childType instanceof ArrowType.Timestamp) {
            return "date";
        }
        if (childType instanceof ArrowType.Utf8) {
            return "keyword";
        }
        if (childType instanceof ArrowType.List
            && child.getChildren().size() == 1
            && child.getChildren().get(0).getType() instanceof ArrowType.Utf8) {
            return "keyword";
        }
        return null;
    }

    /** Whether {@code structField} has at least one supported descendant, recursing into nested structs. */
    private static boolean structHasSupportedProperty(LanceField structField) {
        for (LanceField child : structField.getChildren()) {
            if (child.getType() instanceof ArrowType.Struct) {
                if (structHasSupportedProperty(child)) {
                    return true;
                }
                continue;
            }
            if (structChildMappingType(child) != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Emit the {@code properties} entries of a nested field's element
     * struct, one per supported child, recursing into struct children
     * (which map as {@code object} inside the nested field). The rules
     * are the struct derivation's minus the multi-valued shapes: a
     * {@code List<Utf8>} child would need multi-valued doc values per
     * child doc, and a {@code List<Struct>} child would need nested in
     * nested, so both are skipped with a note; {@code FixedSizeList}
     * vector children and Binary children are skipped as in structs.
     */
    private static void writeNestedProperties(XContentBuilder mapping, LanceField element, String path, List<String> notes)
        throws Exception {
        for (LanceField child : element.getChildren()) {
            String childPath = path + "." + child.getName();
            ArrowType childType = child.getType();
            if (childType instanceof ArrowType.Struct) {
                if (!nestedHasSupportedProperty(child)) {
                    notes.add(childPath + ": Struct with no supported children, not surfaced");
                    continue;
                }
                mapping.startObject(child.getName()).field("type", "object").startObject("properties");
                writeNestedProperties(mapping, child, childPath, notes);
                mapping.endObject().endObject();
                continue;
            }
            String osType = nestedChildMappingType(child);
            if (osType == null) {
                if (childType instanceof ArrowType.FixedSizeList) {
                    notes.add(childPath + ": vector column inside a nested field, not surfaced (lance_knn does not reach it)");
                } else if (isListOfStruct(child)) {
                    notes.add(childPath + ": List<Struct> inside a nested field (nested in nested), not surfaced");
                } else {
                    notes.add(childPath + ": " + childType + ", not surfaced inside a nested field");
                }
                continue;
            }
            startFieldWithId(mapping, child.getName(), child.getId(), osType, arrowTypeIdentity(childType));
            mapping.field("index", false).field("doc_values", true).endObject();
        }
    }

    /**
     * The OpenSearch mapping type of a non-struct child of a nested
     * field's element, or {@code null} when the derivation does not
     * support it there. Scalar-only: unlike a struct child, a nested
     * child's values live on the element's own child doc, and the reader
     * serves single-valued doc values per child doc.
     */
    private static String nestedChildMappingType(LanceField child) {
        ArrowType childType = child.getType();
        if (childType instanceof ArrowType.Int intType && intType.getIsSigned()) {
            return switch (intType.getBitWidth()) {
                case 8 -> "byte";
                case 16 -> "short";
                case 32 -> "integer";
                case 64 -> "long";
                default -> null;
            };
        }
        if (childType instanceof ArrowType.Bool) {
            return "boolean";
        }
        if (childType instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> "float";
                case DOUBLE -> "double";
                default -> null;
            };
        }
        if (childType instanceof ArrowType.Date || childType instanceof ArrowType.Timestamp) {
            return "date";
        }
        if (childType instanceof ArrowType.Utf8) {
            return "keyword";
        }
        return null;
    }

    /** Whether the nested element struct has at least one supported descendant, recursing into struct children. */
    private static boolean nestedHasSupportedProperty(LanceField element) {
        for (LanceField child : element.getChildren()) {
            if (child.getType() instanceof ArrowType.Struct) {
                if (nestedHasSupportedProperty(child)) {
                    return true;
                }
                continue;
            }
            if (nestedChildMappingType(child) != null) {
                return true;
            }
        }
        return false;
    }

    /** Whether {@code field} is a {@code List} whose single child is a {@code Struct}. */
    private static boolean isListOfStruct(LanceField field) {
        return field.getType() instanceof ArrowType.List
            && field.getChildren().size() == 1
            && field.getChildren().get(0).getType() instanceof ArrowType.Struct;
    }

    private static void startFieldWithId(XContentBuilder mapping, String name, int fieldId, String type, String arrowType)
        throws Exception {
        startFieldWithId(mapping, name, fieldId, type, arrowType, null);
    }

    private static void startFieldWithId(
        XContentBuilder mapping,
        String name,
        int fieldId,
        String type,
        String arrowType,
        String overrideType
    ) throws Exception {
        mapping.startObject(name).field("type", type);
        mapping.startObject("meta").field("lance_field_id", Integer.toString(fieldId));
        if (arrowType != null) {
            mapping.field("lance_arrow_type", arrowType);
        }
        if (overrideType != null) {
            // The operator's declared override when it differs from the
            // emitted mapping type (a `wildcard` override served by the
            // keyword mapping), so GET _mapping shows the intent.
            mapping.field("lance_override_type", overrideType);
        }
        mapping.endObject();
    }
}

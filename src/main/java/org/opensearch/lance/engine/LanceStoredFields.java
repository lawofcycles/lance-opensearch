/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.Uid;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceFragmentSchema.ColumnKind;
import org.opensearch.lance.engine.LanceFragmentSchema.NumericPrecision;

/**
 * The stored fields path of one {@link LanceFragmentLeafReader}:
 * {@code _id} and {@code _source} for the docs a request returns.
 *
 * <p>Rows are fetched per hit through {@link #prefetchRows} (one
 * {@code _rowaddr IN (...)} take scan per {@link #TAKE_CHUNK} doc ids,
 * which Lance executes as a take by address) and rendered by
 * {@link #materialiseStoredFields}: {@code _id} from the declared
 * primary key column or a synthesised {@code "<fragment>-<offset>"},
 * {@code _source} as JSON in schema order through
 * {@link XContentBuilder}. The take projects only the surfaced columns
 * plus the primary key, so the cost of a page is proportional to
 * {@code size}, not to the fragment's row count. Every take scan is
 * counted and timed in {@link FetchTakeStats}.
 *
 * <p>Owns the request scoped rows taken so far and the decoding of
 * take batches (scalars, structs, nested arrays, geo points, binary).
 * Does not own doc values, column loads or the doc id layout: it asks
 * the leaf to map doc ids to rows and back, and reads no whole column.
 * One instance per leaf, created by the leaf's constructor; the leaf's
 * {@code storedFields()} returns it and the sequential wrapper
 * ({@link LanceSequentialLeafReader}) calls
 * {@link #materialiseStoredFields} on the leaf, which delegates here.
 */
final class LanceStoredFields extends StoredFields {

    /** Leaf whose rows this renders; maps doc ids to physical rows and back. */
    private final LanceFragmentLeafReader leaf;
    private final Dataset dataset;
    private final int fragmentId;
    // Column kind in schema order, from the shared schema. Preserves schema
    // order so materialiseStoredFields emits _source keys in schema order
    // regardless of which columns have been loaded so far.
    private final Map<String, ColumnKind> columnKind;
    /**
     * Top-level {@code List<Struct>} column names with at least one
     * surfaced child, for {@link #decodeTakeValue}; see
     * {@link LanceFragmentSchema#nestedColumns}.
     */
    private final Set<String> nestedColumns;
    /** How each geo_point column stores its point, for the take decode; see {@link LanceFragmentSchema#geoPointColumns}. */
    private final Map<String, LanceFragmentSchema.GeoPointColumn> geoPointColumns;
    private final String fieldName;
    /**
     * Arrow type family of the declared primary key. Drives {@code _id}
     * materialisation in {@link #materialiseStoredFields}:
     * {@link org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType#LONG}
     * / {@code UNSIGNED_LONG} render the PK value the row take returned
     * as a decimal string, {@code KEYWORD} echoes the Utf8 value
     * verbatim, and
     * {@link org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType#NONE}
     * falls back to a synthesised {@code "<fragment>-<offset>"}. Set to
     * {@code NONE} whenever {@link #fieldName} is empty regardless of what
     * the caller passed in.
     */
    private final LancePrimaryKeyType pkType;
    /**
     * Rows fetched by {@link #prefetchRows} keyed by doc id (physical
     * row offset within the fragment). Each entry holds the decoded
     * values of {@link #takeColumns} in that order; {@link #MISSING_ROW}
     * marks a doc id the take scan did not return (deleted between the
     * scan that produced the doc id and the fetch, which should not
     * happen because the reader pins one Dataset version, but is
     * handled so a miss does not trigger a second scan for the same
     * doc).
     *
     * <p>Fetching only the hit rows keeps the per-request cost of
     * {@code _id} / {@code _source} proportional to {@code size} rather
     * than to the number of rows in the fragment; a constructor-time
     * {@code _rowaddr + PK} scan would make even a 1-hit query scale
     * with total row count.
     */
    private final Map<Integer, Object[]> takenRows = new ConcurrentHashMap<>();
    private static final Object[] MISSING_ROW = new Object[0];
    /**
     * Upper bound on how many row addresses one {@code _rowaddr IN
     * (...)} take scan carries. Lance turns the IN list into a direct
     * take by address (no filter evaluation), so the cap only bounds
     * the SQL string the JNI layer has to parse. {@code
     * index.max_result_window} defaults to 10000, so a normal fetch
     * needs at most three chunks.
     */
    static final int TAKE_CHUNK = 4096;
    /**
     * Column names the row take projects, in {@code _source} emission
     * order. The first {@link #sourceColumnCount} entries are the
     * surfaced columns from {@link #columnKind} (schema order); when
     * the primary key column is not itself surfaced (its Arrow type is
     * one {@link #classify} declines) it is appended after them so
     * {@code _id} can still be rendered.
     */
    private final List<String> takeColumns;
    private final int sourceColumnCount;
    /**
     * Index of the primary key column inside {@link #takeColumns}, or
     * {@code -1} when {@link #pkType} is {@code NONE}.
     */
    private final int pkTakeIndex;
    /**
     * Top-level Struct column names with at least one surfaced child.
     * The row take projects the whole struct under the parent name;
     * {@link #decodeTakeValue} turns it into a nested map so
     * {@link #materialiseStoredFields} renders a JSON object, while the
     * children's doc values live under dotted paths in
     * {@link #columnKind} and load through the same per-column scans as
     * top-level columns (Lance projects a dotted path as a flat column
     * aliased to it).
     */
    private final Set<String> structColumns;
    /**
     * Precision override for numeric columns whose {@link ColumnKind} is
     * {@link ColumnKind#NUMERIC} but whose underlying Arrow type is not
     * a plain integer or a date/timestamp ({@code Float32} and
     * {@code Float64}); every other numeric column is absent from the
     * map and defaults to {@link NumericPrecision#INTEGER} inside the
     * {@code getOrDefault} lookups.
     */
    private final Map<String, NumericPrecision> numericPrecision;

    LanceStoredFields(LanceFragmentLeafReader leaf, Dataset dataset, int fragmentId, LanceFragmentSchema schema) {
        this.leaf = leaf;
        this.dataset = dataset;
        this.fragmentId = fragmentId;
        this.fieldName = schema.fieldName();
        this.pkType = schema.pkType();
        this.structColumns = schema.structColumns();
        this.nestedColumns = schema.nestedColumns();
        this.columnKind = schema.columnKind();
        this.numericPrecision = schema.numericPrecision();
        this.geoPointColumns = schema.geoPointColumns();
        this.sourceColumnCount = schema.sourceColumnCount();
        this.pkTakeIndex = schema.pkTakeIndex();
        this.takeColumns = schema.takeColumns();
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        materialiseStoredFields(docID, visitor);
    }

    /**
     * Fetch the rows behind {@code docIds} from Lance in one take scan
     * per {@link #TAKE_CHUNK} doc ids and stash them in
     * {@link #takenRows} for {@link #materialiseStoredFields}.
     *
     * <p>The scan filters on {@code _rowaddr IN (...)}. Lance recognises
     * that predicate shape as a take-by-address (see
     * {@code TakeOperation::try_from_expr} in
     * {@code rust/lance/src/dataset/scanner.rs}) and reads exactly the
     * requested rows without evaluating a filter or walking the
     * fragment, so a {@code size:10} fetch touches 10 rows of the
     * projected columns regardless of how many rows the fragment
     * holds. Row addresses are {@code (fragmentId << 32) | docId},
     * which is the same encoding the FTS / knn / scalar-filter scans
     * decode doc ids from, and unlike {@code Dataset.takeRows} it does
     * not depend on whether the table uses stable row ids.
     *
     * <p>Doc ids already present in {@link #takenRows} are skipped so a
     * caller can prefetch a whole page and then let the per-doc
     * {@code document(...)} calls hit the cache. Doc ids the scan does
     * not return are recorded as {@link #MISSING_ROW} so the fallback
     * single-doc prefetch inside {@link #materialiseStoredFields} does
     * not issue a second scan for them.
     *
     * <p>the leaf's {@code filterSql} is deliberately not layered in: the doc ids
     * were produced by a scan that already applied it (or by Lucene
     * iteration the caller chose), so re-applying it could only drop
     * rows the caller has decided to return.
     */
    void prefetchRows(int[] docIds) throws IOException {
        List<Long> addresses = new ArrayList<>(docIds.length);
        Set<Integer> requested = new HashSet<>();
        for (int docId : docIds) {
            if (takenRows.containsKey(docId) || !requested.add(docId)) {
                continue;
            }
            // Doc ids address rows through the layout (identity without
            // nested columns); only parent docs reach here, because hits
            // are parents.
            addresses.add(((long) fragmentId << 32) | (leaf.rowOf(docId) & 0xFFFFFFFFL));
        }
        if (addresses.isEmpty()) {
            return;
        }
        if (takeColumns.isEmpty()) {
            // Nothing to project (PK-less table whose columns are all
            // unsurfaced): every row renders as a synthesised _id and
            // an empty _source, so there is no reason to call into
            // Lance.
            for (int docId : requested) {
                takenRows.putIfAbsent(docId, new Object[0]);
            }
            return;
        }
        for (int from = 0; from < addresses.size(); from += TAKE_CHUNK) {
            List<Long> chunk = addresses.subList(from, Math.min(from + TAKE_CHUNK, addresses.size()));
            StringBuilder sql = new StringBuilder(chunk.size() * 12 + 16).append("_rowaddr IN (");
            for (int i = 0; i < chunk.size(); i++) {
                if (i > 0) {
                    sql.append(',');
                }
                sql.append(chunk.get(i).longValue());
            }
            sql.append(')');
            ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(fragmentId))
                .columns(takeColumns)
                .filter(sql.toString())
                .withRowAddress(true)
                .build();
            // Timed from the scan's creation to its close, decoding
            // included: that is the wall time the request spends on
            // this take, and what the node's fetch counters report.
            long start = System.nanoTime();
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    FieldVector[] vectors = new FieldVector[takeColumns.size()];
                    for (int c = 0; c < vectors.length; c++) {
                        vectors[c] = root.getVector(takeColumns.get(c));
                    }
                    for (int i = 0; i < root.getRowCount(); i++) {
                        int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                        Object[] row = new Object[vectors.length];
                        for (int c = 0; c < vectors.length; c++) {
                            row[c] = decodeTakeValue(takeColumns.get(c), vectors[c], i);
                        }
                        takenRows.put(leaf.docOfRow(offset), row);
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                FetchTakeStats.record(
                    FetchTakeStats.Kind.STORED_FIELDS,
                    chunk.size(),
                    takeColumns.size(),
                    System.nanoTime() - start,
                    leaf.takeAccumulator()
                );
            }
        }
        for (int docId : requested) {
            takenRows.putIfAbsent(docId, MISSING_ROW);
        }
    }

    /**
     * Decode one cell of a take-scan batch into the representation
     * {@link #materialiseStoredFields} renders from. Numeric columns
     * go through {@link LanceColumnLoader#readAsLong} so the value carries the same
     * encoding as the doc value path (sortable-int / sortable-long for
     * floats, epoch millis for dates and timestamps, raw bit pattern
     * for UInt64); booleans become {@link Boolean}; Utf8 becomes
     * {@link String}; List&lt;Utf8&gt; becomes {@code String[]}; Binary
     * / LargeBinary become {@code byte[]}. A column that is not in
     * {@link #columnKind} (only the appended PK can be) is decoded by
     * vector type: Utf8 as a string, anything {@link LanceColumnLoader#readAsLong}
     * understands as a long, otherwise {@code null}. Arrow nulls
     * return {@code null}.
     */
    private Object decodeTakeValue(String name, FieldVector vector, int i) {
        if (vector == null || vector.isNull(i)) {
            return null;
        }
        ColumnKind kind = columnKind.get(name);
        if (kind == null) {
            if (vector instanceof StructVector struct && structColumns.contains(name)) {
                return decodeStructValue(name, struct, i);
            }
            if (vector instanceof ListVector list && nestedColumns.contains(name)) {
                return decodeNestedValue(name, list, i);
            }
            if (vector instanceof VarCharVector vc) {
                return new String(vc.get(i), StandardCharsets.UTF_8);
            }
            try {
                return LanceColumnLoader.readAsLong(vector, i);
            } catch (IllegalStateException unsupported) {
                return null;
            }
        }
        return switch (kind) {
            case NUMERIC -> LanceColumnLoader.readAsLong(vector, i);
            case BOOLEAN -> ((BitVector) vector).get(i) == 1;
            case TEXT_FTS, TEXT_KEYWORD -> new String(((VarCharVector) vector).get(i), StandardCharsets.UTF_8);
            case GEO_POINT -> LanceColumnLoader.decodeGeoPoint(geoPointColumns.get(name), vector, i);
            case KEYWORD_ARRAY -> {
                ListVector list = (ListVector) vector;
                VarCharVector elements = (VarCharVector) list.getDataVector();
                int start = list.getElementStartIndex(i);
                int end = list.getElementEndIndex(i);
                String[] arr = new String[end - start];
                for (int e = start; e < end; e++) {
                    arr[e - start] = elements.isNull(e) ? null : new String(elements.get(e), StandardCharsets.UTF_8);
                }
                yield arr;
            }
            case BINARY -> {
                if (vector instanceof VarBinaryVector vb) {
                    yield vb.get(i);
                }
                if (vector instanceof LargeVarBinaryVector lb) {
                    yield lb.get(i);
                }
                yield null;
            }
        };
    }

    /**
     * Decode one struct cell of a take-scan batch into an ordered map of
     * child name to decoded child value, recursing into nested structs.
     * Only children the schema surfaced (present in {@link #columnKind}
     * under their dotted path, or a nested struct with at least one such
     * descendant) appear as keys, so an unsupported child is omitted
     * from {@code _source} the same way its mapping entry is. A child
     * that is Arrow null inside a present struct maps to a {@code null}
     * value, which {@link #materialiseStoredFields} renders as an
     * explicit JSON {@code null}; a struct that is itself null returns
     * {@code null} here (an omitted top-level key, a {@code null} child
     * inside an enclosing struct).
     */
    private Object decodeStructValue(String path, StructVector vector, int i) {
        if (vector.isNull(i)) {
            return null;
        }
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        for (FieldVector child : vector.getChildrenFromFields()) {
            String childPath = path + "." + child.getName();
            if (child instanceof StructVector nested) {
                if (hasSurfacedDescendant(childPath)) {
                    out.put(child.getName(), decodeStructValue(childPath, nested, i));
                }
                continue;
            }
            if (!columnKind.containsKey(childPath)) {
                continue;
            }
            out.put(child.getName(), decodeTakeValue(childPath, child, i));
        }
        return out;
    }

    /** Whether any surfaced column sits under {@code path} (a nested struct with at least one supported leaf). */
    private boolean hasSurfacedDescendant(String path) {
        String prefix = path + ".";
        for (String column : columnKind.keySet()) {
            if (column.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Decode one {@code List<Struct>} cell of a take-scan batch into the
     * list of element objects {@link #materialiseStoredFields} renders as
     * a JSON array. Each element decodes through
     * {@link #decodeStructValue} under the nested column's dotted paths,
     * so only surfaced children appear as keys; a null element struct
     * decodes to {@code null} and renders as a JSON {@code null} element.
     * An Arrow-null list returns {@code null} (the key stays out of
     * {@code _source}); an empty list returns an empty array.
     */
    private Object decodeNestedValue(String path, ListVector vector, int i) {
        if (vector.isNull(i)) {
            return null;
        }
        StructVector elements = (StructVector) vector.getDataVector();
        int start = vector.getElementStartIndex(i);
        int end = vector.getElementEndIndex(i);
        List<Object> out = new ArrayList<>(end - start);
        for (int e = start; e < end; e++) {
            out.add(decodeStructValue(path, elements, e));
        }
        return out;
    }

    /**
     * Materialise stored fields for a single doc. Extracted from the {@code
     * storedFields()} anonymous class so the sequential wrapper (see
     * {@link LanceSequentialLeafReader}) can call the same routine when
     * {@code FetchPhase} switches to the sequential stored-fields path.
     *
     * <p>Reads from the row {@link #prefetchRows} fetched for
     * {@code docID}. Callers that know the whole page up front
     * (the fragment dispatch handler) prefetch every hit in one scan
     * per leaf; a doc that was not prefetched triggers a single-row
     * take here so the method stays correct for any caller.
     */
    void materialiseStoredFields(int docID, StoredFieldVisitor visitor) throws IOException {
        FieldInfo idInfo = storedOnly("_id", 1);
        FieldInfo sourceInfo = storedOnly("_source", 2);
        boolean needsId = visitor.needsField(idInfo) == StoredFieldVisitor.Status.YES;
        boolean needsSource = visitor.needsField(sourceInfo) == StoredFieldVisitor.Status.YES;
        if (!needsId && !needsSource) {
            return;
        }
        Object[] row = takenRows.get(docID);
        if (row == null) {
            prefetchRows(new int[] { docID });
            row = takenRows.getOrDefault(docID, MISSING_ROW);
        }
        if (needsId) {
            // Materialise _id from whichever column the declared primary key
            // lives on, or synthesise "<fragment>-<offset>" when no PK is
            // declared. Without this fallback every row on a PK-less table
            // would collapse to the same _id, silently breaking sort-by-_id
            // and _mget dedup. GET /_doc/{id} still returns 404 for PK-less
            // tables (see LanceReadOnlyEngine.get); the fallback is strictly
            // for _search response fidelity.
            //
            // A null PK value (nullable column, Arrow null in that row) or
            // a row the take did not return also fall through to the
            // synthesised form so the row still gets a unique id rather
            // than repeating an empty string.
            Object pk = pkTakeIndex >= 0 && pkTakeIndex < row.length ? row[pkTakeIndex] : null;
            int rowOffset = leaf.rowOf(docID);
            String idString = switch (pkType) {
                case KEYWORD -> pk instanceof String s ? s : fragmentId + "-" + rowOffset;
                case LONG -> pk instanceof Long l ? Long.toString(l) : fragmentId + "-" + rowOffset;
                // readAsLong returns the unsigned bit pattern for
                // UInt8Vector; Long.toUnsignedString decodes it back into
                // the 0..2^64-1 range the operator wrote.
                case UNSIGNED_LONG -> pk instanceof Long l ? Long.toUnsignedString(l) : fragmentId + "-" + rowOffset;
                default -> fragmentId + "-" + rowOffset;
            };
            BytesRef encoded = Uid.encodeId(idString);
            byte[] bytes = new byte[encoded.length];
            System.arraycopy(encoded.bytes, encoded.offset, bytes, 0, encoded.length);
            visitor.binaryField(idInfo, bytes);
        }
        if (needsSource) {
            // Build _source through XContentBuilder so string values get the
            // JSON escaping RFC 8259 requires (control characters U+0000
            // through U+001F, quotes, backslashes). Hand-rolled string
            // concatenation only escaped \" and \\, which meant a body
            // containing a newline emitted invalid JSON and every client
            // that parsed the response strictly (jackson, python json,
            // Dashboards) rejected it.
            //
            // Column iteration follows the schema pass order captured in
            // takeColumns (the leading sourceColumnCount entries mirror
            // columnKind), so _source keys land in the same order every
            // time. Values come from the per-hit take; no whole-column
            // load happens here.
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                int limit = Math.min(sourceColumnCount, row.length);
                for (int c = 0; c < limit; c++) {
                    Object value = row[c];
                    if (value == null) {
                        continue;
                    }
                    String name = takeColumns.get(c);
                    writeSourceField(builder, name, name, value);
                }
                builder.endObject();
                byte[] json = BytesReference.toBytes(BytesReference.bytes(builder));
                visitor.binaryField(sourceInfo, json);
            }
        }
    }

    /**
     * Render one {@code _source} value under {@code key}. {@code path}
     * is the dotted column path ({@code key} for top-level columns, the
     * full {@code parent.child} path inside a struct) so the
     * {@link ColumnKind} and {@link NumericPrecision} lookups resolve.
     * A {@link java.util.Map} value (a decoded struct) renders as a
     * JSON object, recursing per child; a {@code null} value renders as
     * an explicit JSON {@code null} (only struct children reach here as
     * {@code null} — the top-level loop skips absent columns, keeping
     * their keys out of {@code _source} as before).
     */
    private void writeSourceField(XContentBuilder builder, String path, String key, Object value) throws IOException {
        if (value == null) {
            builder.nullField(key);
            return;
        }
        if (value instanceof Map<?, ?> struct) {
            builder.startObject(key);
            writeStructBody(builder, path, struct);
            builder.endObject();
            return;
        }
        if (value instanceof List<?> array) {
            // A decoded List<Struct> (nested) column: render the array of
            // element objects. Elements are maps of the surfaced children
            // under the same dotted paths as the parent's mapping; a null
            // element struct renders as a JSON null element.
            builder.startArray(key);
            for (Object element : array) {
                if (element == null) {
                    builder.nullValue();
                } else {
                    builder.startObject();
                    writeStructBody(builder, path, (Map<?, ?>) element);
                    builder.endObject();
                }
            }
            builder.endArray();
            return;
        }
        switch (columnKind.get(path)) {
            case NUMERIC -> {
                long numericValue = (Long) value;
                NumericPrecision precision = numericPrecision.getOrDefault(path, NumericPrecision.INTEGER);
                switch (precision) {
                    case FLOAT -> builder.field(key, NumericUtils.sortableIntToFloat((int) numericValue));
                    case DOUBLE -> builder.field(key, NumericUtils.sortableLongToDouble(numericValue));
                    case INTEGER -> {
                        if (pkType == LancePrimaryKeyType.UNSIGNED_LONG && path.equals(fieldName)) {
                            // UInt64 PK column: emit as an unsigned
                            // decimal so the JSON number matches
                            // what the operator wrote. Other
                            // UInt64 columns are not surfaced by
                            // classify(), so this branch fires
                            // only for the PK.
                            builder.field(key, new BigInteger(Long.toUnsignedString(numericValue)));
                        } else {
                            builder.field(key, numericValue);
                        }
                    }
                }
            }
            case BOOLEAN -> builder.field(key, (Boolean) value);
            case TEXT_FTS, TEXT_KEYWORD -> builder.field(key, (String) value);
            case KEYWORD_ARRAY -> builder.field(key, (String[]) value);
            case GEO_POINT -> {
                // Render canonically as {"lat": .., "lon": ..} with the
                // original double values regardless of the Arrow storage
                // (Struct child names or FixedSizeList order): the field
                // maps as geo_point, so the object shape every geo_point
                // consumer understands is the faithful projection.
                double[] point = (double[]) value;
                builder.startObject(key).field("lat", point[0]).field("lon", point[1]).endObject();
            }
            case BINARY -> builder.field(key, Base64.getEncoder().encodeToString((byte[]) value));
        }
    }

    /** Render the children of one decoded struct (or nested element) under {@code path}. */
    private void writeStructBody(XContentBuilder builder, String path, Map<?, ?> struct) throws IOException {
        for (Map.Entry<?, ?> entry : struct.entrySet()) {
            String childName = (String) entry.getKey();
            writeSourceField(builder, path + "." + childName, childName, entry.getValue());
        }
    }

    private static FieldInfo storedOnly(String name, int number) {
        return new FieldInfo(
            name,
            number,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }
}

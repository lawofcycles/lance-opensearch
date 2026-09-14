/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Version;
import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

/**
 * LeafReader over one Lance fragment.
 *
 * Docids are physical row offsets within the fragment, liveDocs reflects the
 * fragment's deletion file (computed from row address gaps), and the numeric
 * field is served as DocValues from the Lance column. This is the RFC's
 * "leaf corresponds to a fragment group" contract in its minimal form.
 */
public final class LanceFragmentLeafReader extends LeafReader {

    private final String fieldName;
    private final int maxDoc;
    private final int numDocs;
    private final long[] values;
    private final Bits liveDocs;
    private final FieldInfos fieldInfos;
    private final Dataset dataset;
    private final int fragmentId;
    // eagerly loaded column values for _source synthesis (PoC; production streams from Lance)
    private final Map<String, long[]> numericColumns = new LinkedHashMap<>();
    private final Map<String, String[]> textColumns = new LinkedHashMap<>();
    private final Map<String, long[]> booleanColumns = new LinkedHashMap<>();
    // Utf8 columns without an FTS index surface as keyword. We keep them in
    // textColumns for _source synthesis and additionally build sorted term
    // dictionaries plus per-doc ordinals so getSortedSetDocValues can serve
    // term, terms, aggregation and sort requests through the doc value path.
    private final Map<String, BytesRef[]> keywordTerms = new LinkedHashMap<>();
    private final Map<String, int[]> keywordOrds = new LinkedHashMap<>();
    // List<Utf8> columns surface as multi-valued keyword. keywordArrayValues
    // holds the per-doc string arrays for _source synthesis; keywordArrayOrds
    // and keywordArrayTerms back a multi-valued SortedSetDocValues.
    private final Map<String, String[][]> keywordArrayValues = new LinkedHashMap<>();
    private final Map<String, int[][]> keywordArrayOrds = new LinkedHashMap<>();
    private final Map<String, BytesRef[]> keywordArrayTerms = new LinkedHashMap<>();
    // Binary / LargeBinary columns surface as OpenSearch binary type. The bytes
    // are held per-doc for _source synthesis (base64-encoded on output);
    // neither indexed nor loaded into doc values.
    private final Map<String, byte[][]> binaryColumns = new LinkedHashMap<>();
    // Bridge to Lucene's cache lifecycle. IndicesQueryCache, IndicesFieldDataCache
    // and IndicesRequestCache all key entries by IndexReader.CacheKey and rely on
    // IndexReader.ClosedListener to invalidate them. IndexReader.CacheKey has a
    // package-private constructor, so a plugin sitting outside the
    // org.apache.lucene.index package cannot mint its own key. We instead hold a
    // tiny one-doc Lucene reader whose lifetime is bound to this fragment reader:
    // its CacheHelper is exposed as ours, and closing this reader closes the
    // bridge, which fires the listeners registered by the OpenSearch caches.
    private final DirectoryReader cacheLifetimeBridge;

    public LanceFragmentLeafReader(Dataset dataset, int fragmentId, long physicalRows, String intField) throws IOException {
        this.dataset = dataset;
        this.fragmentId = fragmentId;
        this.fieldName = intField;
        this.maxDoc = (int) physicalRows;
        this.values = new long[maxDoc];
        FixedBitSet live = new FixedBitSet(maxDoc);
        int liveCount = 0;

        // load all scalar columns (int/utf8/bool/date/timestamp/list<utf8>/binary) for _source; skip vectors etc.
        List<String> scanColumns = new java.util.ArrayList<>();
        for (org.apache.arrow.vector.types.pojo.Field field : dataset.getSchema().getFields()) {
            ArrowType type = field.getType();
            boolean isKeywordArray = type instanceof ArrowType.List
                && field.getChildren().size() == 1
                && field.getChildren().get(0).getType() instanceof ArrowType.Utf8;
            boolean isBinary = type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary;
            if (type instanceof ArrowType.Int
                || type instanceof ArrowType.Utf8
                || type instanceof ArrowType.Bool
                || type instanceof ArrowType.Date
                || type instanceof ArrowType.Timestamp
                || isKeywordArray
                || isBinary) {
                scanColumns.add(field.getName());
                if (type instanceof ArrowType.Bool) {
                    booleanColumns.put(field.getName(), new long[maxDoc]);
                } else if (type instanceof ArrowType.Utf8) {
                    textColumns.put(field.getName(), new String[maxDoc]);
                } else if (isKeywordArray) {
                    keywordArrayValues.put(field.getName(), new String[maxDoc][]);
                } else if (isBinary) {
                    binaryColumns.put(field.getName(), new byte[maxDoc][]);
                } else {
                    // Int, Date, Timestamp all backed by long
                    numericColumns.put(field.getName(), new long[maxDoc]);
                }
            }
        }

        ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(fragmentId))
            .columns(scanColumns)
            .withRowAddress(true)
            .build();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                for (int i = 0; i < root.getRowCount(); i++) {
                    int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                    live.set(offset);
                    liveCount++;
                    for (Map.Entry<String, long[]> entry : numericColumns.entrySet()) {
                        FieldVector vector = root.getVector(entry.getKey());
                        entry.getValue()[offset] = readAsLong(vector, i);
                    }
                    for (Map.Entry<String, long[]> entry : booleanColumns.entrySet()) {
                        BitVector vector = (BitVector) root.getVector(entry.getKey());
                        entry.getValue()[offset] = vector.isNull(i) ? 0 : vector.get(i);
                    }
                    for (Map.Entry<String, String[]> entry : textColumns.entrySet()) {
                        VarCharVector vector = (VarCharVector) root.getVector(entry.getKey());
                        entry.getValue()[offset] = vector.isNull(i)
                            ? null
                            : new String(vector.get(i), java.nio.charset.StandardCharsets.UTF_8);
                    }
                    for (Map.Entry<String, String[][]> entry : keywordArrayValues.entrySet()) {
                        ListVector vector = (ListVector) root.getVector(entry.getKey());
                        if (vector.isNull(i)) {
                            entry.getValue()[offset] = null;
                            continue;
                        }
                        VarCharVector elements = (VarCharVector) vector.getDataVector();
                        int start = vector.getElementStartIndex(i);
                        int end = vector.getElementEndIndex(i);
                        String[] arr = new String[end - start];
                        for (int e = start; e < end; e++) {
                            arr[e - start] = elements.isNull(e)
                                ? null
                                : new String(elements.get(e), java.nio.charset.StandardCharsets.UTF_8);
                        }
                        entry.getValue()[offset] = arr;
                    }
                    for (Map.Entry<String, byte[][]> entry : binaryColumns.entrySet()) {
                        FieldVector v = root.getVector(entry.getKey());
                        if (v.isNull(i)) {
                            entry.getValue()[offset] = null;
                        } else if (v instanceof VarBinaryVector vb) {
                            entry.getValue()[offset] = vb.get(i);
                        } else if (v instanceof LargeVarBinaryVector lb) {
                            entry.getValue()[offset] = lb.get(i);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        // Post-load: for every Utf8 column without an FTS index, build a sorted
        // term dictionary and per-doc ords so getSortedSetDocValues can serve it
        // through the keyword doc value path.
        try {
            for (org.apache.arrow.vector.types.pojo.Field field : dataset.getSchema().getFields()) {
                if (!(field.getType() instanceof ArrowType.Utf8)) {
                    continue;
                }
                String name = field.getName();
                boolean hasFts = !dataset.describeIndices(new IndexCriteria.Builder().forColumn(name).mustSupportFts(true).build())
                    .isEmpty();
                if (hasFts) {
                    continue;
                }
                String[] raw = textColumns.get(name);
                TreeSet<String> unique = new TreeSet<>();
                for (String v : raw) {
                    if (v != null) {
                        unique.add(v);
                    }
                }
                BytesRef[] terms = new BytesRef[unique.size()];
                Map<String, Integer> lookup = new HashMap<>();
                int idx = 0;
                for (String t : unique) {
                    terms[idx] = new BytesRef(t);
                    lookup.put(t, idx);
                    idx++;
                }
                int[] ords = new int[maxDoc];
                Arrays.fill(ords, -1);
                for (int r = 0; r < maxDoc; r++) {
                    String v = raw[r];
                    if (v != null) {
                        ords[r] = lookup.get(v);
                    }
                }
                keywordTerms.put(name, terms);
                keywordOrds.put(name, ords);
            }
            // Build sorted term dictionaries + per-doc ord arrays for List<Utf8> columns.
            for (Map.Entry<String, String[][]> entry : keywordArrayValues.entrySet()) {
                String name = entry.getKey();
                String[][] rows = entry.getValue();
                TreeSet<String> unique = new TreeSet<>();
                for (String[] row : rows) {
                    if (row == null) continue;
                    for (String v : row) {
                        if (v != null) unique.add(v);
                    }
                }
                BytesRef[] terms = new BytesRef[unique.size()];
                Map<String, Integer> lookup = new HashMap<>();
                int idx = 0;
                for (String t : unique) {
                    terms[idx] = new BytesRef(t);
                    lookup.put(t, idx);
                    idx++;
                }
                int[][] rowOrds = new int[maxDoc][];
                for (int r = 0; r < maxDoc; r++) {
                    String[] row = rows[r];
                    if (row == null) {
                        rowOrds[r] = null;
                        continue;
                    }
                    // Ords are stored in sorted order without duplicates so
                    // SortedSetDocValues.nextOrd walks strictly ascending.
                    TreeSet<Integer> unique2 = new TreeSet<>();
                    for (String v : row) {
                        if (v != null) unique2.add(lookup.get(v));
                    }
                    int[] ordArr = new int[unique2.size()];
                    int j = 0;
                    for (int o : unique2) {
                        ordArr[j++] = o;
                    }
                    rowOrds[r] = ordArr;
                }
                keywordArrayTerms.put(name, terms);
                keywordArrayOrds.put(name, rowOrds);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        long[] keyColumn = numericColumns.get(intField);
        if (keyColumn != null) {
            System.arraycopy(keyColumn, 0, values, 0, maxDoc);
        }
        this.numDocs = liveCount;
        this.liveDocs = liveCount == maxDoc ? null : live;

        List<FieldInfo> infos = new java.util.ArrayList<>();
        int number = 10;
        for (String column : numericColumns.keySet()) {
            infos.add(
                new FieldInfo(
                    column,
                    number++,
                    false,
                    true,
                    false,
                    IndexOptions.NONE,
                    DocValuesType.NUMERIC,
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
                )
            );
        }
        for (String column : booleanColumns.keySet()) {
            infos.add(
                new FieldInfo(
                    column,
                    number++,
                    false,
                    true,
                    false,
                    IndexOptions.NONE,
                    DocValuesType.NUMERIC,
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
                )
            );
        }
        for (String column : keywordOrds.keySet()) {
            infos.add(
                new FieldInfo(
                    column,
                    number++,
                    false,
                    true,
                    false,
                    IndexOptions.NONE,
                    DocValuesType.SORTED_SET,
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
                )
            );
        }
        for (String column : keywordArrayOrds.keySet()) {
            infos.add(
                new FieldInfo(
                    column,
                    number++,
                    false,
                    true,
                    false,
                    IndexOptions.NONE,
                    DocValuesType.SORTED_SET,
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
                )
            );
        }
        this.fieldInfos = new FieldInfos(infos.toArray(new FieldInfo[0]));

        ByteBuffersDirectory bridgeDir = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(bridgeDir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
        }
        this.cacheLifetimeBridge = DirectoryReader.open(bridgeDir);
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) {
        final long[] column = numericColumns.get(field) != null ? numericColumns.get(field) : booleanColumns.get(field);
        if (column == null) {
            return null;
        }
        return new NumericDocValues() {
            private int doc = -1;

            @Override
            public long longValue() {
                return column[doc];
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                return liveDocs == null || liveDocs.get(target);
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                doc = target >= column.length ? NO_MORE_DOCS : target;
                return doc;
            }

            @Override
            public long cost() {
                return column.length;
            }
        };
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) {
        NumericDocValues numeric = getNumericDocValues(field);
        return numeric == null ? null : DocValues.singleton(numeric);
    }

    @Override
    public FieldInfos getFieldInfos() {
        return fieldInfos;
    }

    @Override
    public Bits getLiveDocs() {
        return liveDocs;
    }

    @Override
    public int numDocs() {
        return numDocs;
    }

    @Override
    public int maxDoc() {
        return maxDoc;
    }

    @Override
    public LeafMetaData getMetaData() {
        return new LeafMetaData(Version.LATEST.major, Version.LATEST, null, false);
    }

    @Override
    public Terms terms(String field) {
        return null;
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) {
        return null;
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) {
        int[] ords = keywordOrds.get(field);
        BytesRef[] terms = keywordTerms.get(field);
        if (ords == null || terms == null) {
            return null;
        }
        return keywordSortedDocValues(ords, terms);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) {
        // multi-valued (List<Utf8>) has priority over the single-valued path.
        int[][] arrayOrds = keywordArrayOrds.get(field);
        BytesRef[] arrayTerms = keywordArrayTerms.get(field);
        if (arrayOrds != null && arrayTerms != null) {
            return keywordArraySortedSetDocValues(arrayOrds, arrayTerms);
        }
        SortedDocValues single = getSortedDocValues(field);
        return single == null ? null : DocValues.singleton(single);
    }

    private SortedSetDocValues keywordArraySortedSetDocValues(int[][] rowOrds, BytesRef[] terms) {
        return new SortedSetDocValues() {
            private int doc = -1;
            private int[] currentRow;
            private int cursor;

            @Override
            public long nextOrd() {
                // Caller iterates docValueCount() times; no sentinel needed.
                return currentRow[cursor++];
            }

            @Override
            public int docValueCount() {
                return currentRow == null ? 0 : currentRow.length;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return terms[(int) ord];
            }

            @Override
            public long getValueCount() {
                return terms.length;
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                cursor = 0;
                if (liveDocs != null && !liveDocs.get(target)) {
                    currentRow = null;
                    return false;
                }
                currentRow = rowOrds[target];
                return currentRow != null && currentRow.length > 0;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                for (int i = target; i < rowOrds.length; i++) {
                    if ((liveDocs == null || liveDocs.get(i)) && rowOrds[i] != null && rowOrds[i].length > 0) {
                        doc = i;
                        currentRow = rowOrds[i];
                        cursor = 0;
                        return i;
                    }
                }
                doc = NO_MORE_DOCS;
                currentRow = null;
                return doc;
            }

            @Override
            public long cost() {
                return rowOrds.length;
            }
        };
    }

    private SortedDocValues keywordSortedDocValues(int[] ords, BytesRef[] terms) {
        return new SortedDocValues() {
            private int doc = -1;

            @Override
            public int ordValue() {
                return ords[doc];
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                return terms[ord];
            }

            @Override
            public int getValueCount() {
                return terms.length;
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                if (liveDocs != null && !liveDocs.get(target)) {
                    return false;
                }
                return ords[target] >= 0;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                for (int i = target; i < ords.length; i++) {
                    if ((liveDocs == null || liveDocs.get(i)) && ords[i] >= 0) {
                        doc = i;
                        return i;
                    }
                }
                doc = NO_MORE_DOCS;
                return doc;
            }

            @Override
            public long cost() {
                return ords.length;
            }
        };
    }

    @Override
    public NumericDocValues getNormValues(String field) {
        return null;
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) {
        return null;
    }

    @Override
    public PointValues getPointValues(String field) {
        return null;
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) {
        return null;
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) {
        return null;
    }

    @Override
    public void searchNearestVectors(String field, float[] target, KnnCollector collector, AcceptDocs acceptDocs) {}

    @Override
    public void searchNearestVectors(String field, byte[] target, KnnCollector collector, AcceptDocs acceptDocs) {}

    @Override
    public TermVectors termVectors() {
        return TermVectors.EMPTY;
    }

    @Override
    public StoredFields storedFields() {
        return new StoredFields() {
            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                materialiseStoredFields(docID, visitor);
            }
        };
    }

    /**
     * Materialise stored fields for a single doc. Extracted from the {@code
     * storedFields()} anonymous class so the sequential wrapper (see
     * {@link LanceSequentialLeafReader}) can call the same routine when
     * {@code FetchPhase} switches to the sequential stored-fields path.
     */
    void materialiseStoredFields(int docID, StoredFieldVisitor visitor) throws IOException {
        FieldInfo idInfo = storedOnly("_id", 1);
        if (visitor.needsField(idInfo) == StoredFieldVisitor.Status.YES) {
            org.apache.lucene.util.BytesRef encoded = org.opensearch.index.mapper.Uid.encodeId(Long.toString(values[docID]));
            byte[] bytes = new byte[encoded.length];
            System.arraycopy(encoded.bytes, encoded.offset, bytes, 0, encoded.length);
            visitor.binaryField(idInfo, bytes);
        }
        FieldInfo sourceInfo = storedOnly("_source", 2);
        if (visitor.needsField(sourceInfo) == StoredFieldVisitor.Status.YES) {
            // Build _source through XContentBuilder so string values get the
            // JSON escaping RFC 8259 requires (control characters U+0000
            // through U+001F, quotes, backslashes). Hand-rolled string
            // concatenation only escaped \" and \\, which meant a body
            // containing a newline emitted invalid JSON and every client
            // that parsed the response strictly (jackson, python json,
            // Dashboards) rejected it.
            try (org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                builder.startObject();
                for (Map.Entry<String, long[]> entry : numericColumns.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue()[docID]);
                }
                for (Map.Entry<String, long[]> entry : booleanColumns.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue()[docID] == 1);
                }
                for (Map.Entry<String, String[]> entry : textColumns.entrySet()) {
                    String value = entry.getValue()[docID];
                    if (value != null) {
                        builder.field(entry.getKey(), value);
                    }
                }
                for (Map.Entry<String, String[][]> entry : keywordArrayValues.entrySet()) {
                    String[] arr = entry.getValue()[docID];
                    if (arr != null) {
                        builder.field(entry.getKey(), arr);
                    }
                }
                for (Map.Entry<String, byte[][]> entry : binaryColumns.entrySet()) {
                    byte[] bytes = entry.getValue()[docID];
                    if (bytes != null) {
                        builder.field(entry.getKey(), java.util.Base64.getEncoder().encodeToString(bytes));
                    }
                }
                builder.endObject();
                byte[] json = org.opensearch.core.common.bytes.BytesReference.toBytes(
                    org.opensearch.core.common.bytes.BytesReference.bytes(builder)
                );
                visitor.binaryField(sourceInfo, json);
            }
        }
    }

    Dataset dataset() {
        return dataset;
    }

    int fragmentId() {
        return fragmentId;
    }

    /**
     * Recursively unwrap {@link FilterLeafReader} layers and return the
     * underlying {@code LanceFragmentLeafReader}, or {@code null} if the
     * leaf is not backed by Lance. Callers that need Lance-specific state
     * (fragment id, dataset handle) use this to strip any wrappers that
     * OpenSearch or Lucene may have applied to the reader.
     */
    static LanceFragmentLeafReader unwrap(LeafReader reader) {
        LeafReader current = reader;
        while (current instanceof FilterLeafReader filter) {
            current = filter.getDelegate();
        }
        return current instanceof LanceFragmentLeafReader lance ? lance : null;
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

    // Reads an Arrow scalar vector value as a long. Date/Timestamp vectors are
    // normalized to epoch milliseconds so the DateFieldMapper reads them through
    // the same numeric doc value path as integers.
    private static long readAsLong(FieldVector v, int i) {
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        if (v instanceof DateDayVector d) {
            return d.get(i) * 86_400_000L;
        }
        if (v instanceof DateMilliVector d) {
            return d.get(i);
        }
        if (v instanceof TimeStampSecVector t) {
            return t.get(i) * 1000L;
        }
        if (v instanceof TimeStampSecTZVector t) {
            return t.get(i) * 1000L;
        }
        if (v instanceof TimeStampMilliVector t) {
            return t.get(i);
        }
        if (v instanceof TimeStampMilliTZVector t) {
            return t.get(i);
        }
        if (v instanceof TimeStampMicroVector t) {
            return t.get(i) / 1000L;
        }
        if (v instanceof TimeStampMicroTZVector t) {
            return t.get(i) / 1000L;
        }
        if (v instanceof TimeStampNanoVector t) {
            return t.get(i) / 1_000_000L;
        }
        if (v instanceof TimeStampNanoTZVector t) {
            return t.get(i) / 1_000_000L;
        }
        throw new IllegalStateException("unsupported vector type: " + v.getClass().getName());
    }

    @Override
    public void checkIntegrity() {}

    @Override
    protected void doClose() throws IOException {
        cacheLifetimeBridge.close();
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return cacheLifetimeBridge.leaves().get(0).reader().getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return cacheLifetimeBridge.getReaderCacheHelper();
    }
}

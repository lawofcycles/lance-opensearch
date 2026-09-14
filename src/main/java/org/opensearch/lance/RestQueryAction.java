/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.Text;
import org.lance.Dataset;
import org.lance.ipc.FullTextQuery;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * POST /_lance/{name}/_query
 *
 * Rewrites DSL shaped queries to Lance native queries, the pattern the RFC's
 * reader route describes ("text and k-NN queries rewrite to Lance FTS and
 * vector index queries").
 *
 * Supported bodies:
 *   {"match": {"field": "body", "query": "hello"}, "limit": 20}
 *   {"knn": {"field": "embedding", "vector": [..], "k": 5}}
 *   optional: "columns": ["id", "title"], "filter": "id > 10"
 */
public class RestQueryAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_lance/{name}/_query"));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String name = request.param("name");
        Map<String, Object> body = XContentHelper.convertToMap(request.content(), false, request.getMediaType()).v2();

        return channel -> {
            Dataset dataset = LanceRegistry.get(name);
            if (dataset == null) {
                try (XContentBuilder b = channel.newBuilder()) {
                    b.startObject().field("error", "table not attached: " + name).endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, b));
                }
                return;
            }

            int limit = body.containsKey("limit") ? ((Number) body.get("limit")).intValue() : 10;
            ScanOptions.Builder options = new ScanOptions.Builder().limit(limit);

            Object columns = body.get("columns");
            if (columns instanceof List<?> cols) {
                List<String> names = new ArrayList<>();
                for (Object c : cols) {
                    names.add(c.toString());
                }
                options.columns(names);
            }
            if (body.get("filter") instanceof String f) {
                options.filter(f);
            }
            if (body.get("fragments") instanceof List<?> frags) {
                List<Integer> ids = new ArrayList<>();
                for (Object fr : frags) {
                    ids.add(((Number) fr).intValue());
                }
                options.fragmentIds(ids);
                options.withRowAddress(true);
            }

            String rewrittenTo;
            if (body.get("match") instanceof Map<?, ?> match) {
                String field = (String) match.get("field");
                String query = (String) match.get("query");
                options.fullTextQuery(FullTextQuery.match(query, field));
                rewrittenTo = "lance_fts_match(" + field + ")";
            } else if (body.get("knn") instanceof Map<?, ?> knn) {
                String field = (String) knn.get("field");
                int k = ((Number) knn.get("k")).intValue();
                List<Number> vectorList = (List<Number>) knn.get("vector");
                float[] key = new float[vectorList.size()];
                for (int i = 0; i < key.length; i++) {
                    key[i] = vectorList.get(i).floatValue();
                }
                options.nearest(new Query.Builder().setColumn(field).setKey(key).setK(k).build());
                rewrittenTo = "lance_vector_nearest(" + field + ", k=" + k + ")";
            } else {
                rewrittenTo = "lance_scan";
            }

            try (XContentBuilder b = channel.newBuilder()) {
                b.startObject();
                b.field("rewritten_to", rewrittenTo);
                b.startArray("hits");
                long total = 0;
                try (LanceScanner scanner = dataset.newScan(options.build()); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        for (int row = 0; row < root.getRowCount(); row++) {
                            b.startObject();
                            for (FieldVector vector : root.getFieldVectors()) {
                                b.field(vector.getName(), toJsonValue(vector.getObject(row)));
                            }
                            b.endObject();
                            total++;
                        }
                    }
                }
                b.endArray();
                b.field("total", total);
                b.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, b));
            }
        };
    }

    private static Object toJsonValue(Object arrowValue) {
        if (arrowValue instanceof Text text) {
            return text.toString();
        }
        if (arrowValue instanceof List<?> list) {
            List<Object> converted = new ArrayList<>(list.size());
            for (Object element : list) {
                converted.add(toJsonValue(element));
            }
            return converted;
        }
        return arrowValue;
    }
}

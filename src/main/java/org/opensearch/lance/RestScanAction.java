/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.Text;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * {@code GET /_lance/{name}/_scan}
 *
 * <p>match_all style scan returning rows as {@code _source} documents.
 * Supports {@code limit}, {@code columns}, and {@code filter} query parameters.
 */
public class RestScanAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_scan";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_lance/{name}/_scan"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String name = request.param("name");
        int limit = request.paramAsInt("limit", 10);
        String columnsParam = request.param("columns");
        String filter = request.param("filter");

        return channel -> {
            Dataset dataset = LanceRegistry.get(name);
            if (dataset == null) {
                try (XContentBuilder b = channel.newBuilder()) {
                    b.startObject().field("error", "table not attached: " + name).endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, b));
                }
                return;
            }

            ScanOptions.Builder options = new ScanOptions.Builder().limit(limit);
            if (columnsParam != null) {
                options.columns(Arrays.asList(columnsParam.split(",")));
            }
            if (filter != null) {
                options.filter(filter);
            }

            try (XContentBuilder b = channel.newBuilder()) {
                b.startObject();
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

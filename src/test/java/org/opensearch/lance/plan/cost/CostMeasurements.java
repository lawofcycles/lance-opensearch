/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The measurement rows of {@code cost/measurements.csv} as the cost
 * model's inputs, shared by the model and coefficient tests.
 */
final class CostMeasurements {

    private CostMeasurements() {}

    /** One CSV row: the descriptive columns plus the model inputs it maps to. */
    record Row(Map<String, String> columns, CostInputs inputs, AggregateProfile shape) {

        String round() {
            return columns.get("round");
        }

        String table() {
            return columns.get("table");
        }

        String cluster() {
            return columns.get("cluster");
        }

        String shapeName() {
            return columns.get("shape");
        }

        boolean pushed() {
            return "pushed".equals(columns.get("path"));
        }

        double latencyMillis() {
            return Double.parseDouble(columns.get("latency_ms"));
        }

        boolean excluded() {
            return !"0".equals(columns.get("excluded"));
        }

        int slices() {
            return Integer.parseInt(columns.get("slices"));
        }

        /** What the model predicts for this row's path. */
        double predictedMillis() {
            return pushed() ? CostModel.pushedAggregateMillis(inputs, shape) : CostModel.luceneAggregateMillis(inputs, shape);
        }

        String pairKey() {
            return table() + " / " + cluster() + " / " + shapeName();
        }
    }

    static List<Row> load() throws IOException {
        List<Row> rows = new ArrayList<>();
        for (Map<String, String> columns : readCsv("/cost/measurements.csv")) {
            rows.add(new Row(columns, inputsOf(columns), shapeOf(columns)));
        }
        return rows;
    }

    static List<Map<String, String>> readCsv(String resource) throws IOException {
        List<Map<String, String>> rows = new ArrayList<>();
        try (InputStream in = CostMeasurements.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("missing test resource " + resource);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String header = reader.readLine();
            List<String> names = splitCsv(header);
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                List<String> values = splitCsv(line);
                Map<String, String> row = new LinkedHashMap<>();
                for (int i = 0; i < names.size(); i++) {
                    row.put(names.get(i), i < values.size() ? values.get(i) : "");
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /** One CSV line into fields: commas separate, double quotes enclose a field that contains commas or quotes. */
    static List<String> splitCsv(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    field.append(c);
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == ',') {
                fields.add(field.toString());
                field.setLength(0);
            } else {
                field.append(c);
            }
        }
        fields.add(field.toString());
        return fields;
    }

    private static CostInputs inputsOf(Map<String, String> c) {
        return new CostInputs(
            Integer.parseInt(c.get("nodes")),
            "object_store".equals(c.get("storage")) ? StorageKind.OBJECT_STORE : StorageKind.LOCAL,
            Integer.parseInt(c.get("cpus")),
            Integer.parseInt(c.get("parallelism")),
            Integer.parseInt(c.get("slices"))
        );
    }

    private static AggregateProfile shapeOf(Map<String, String> c) {
        return new AggregateProfile(
            Double.parseDouble(c.get("rows")),
            Double.parseDouble(c.get("groups")),
            Double.parseDouble(c.get("merged_groups")),
            true,
            Integer.parseInt(c.get("columns_read")),
            Double.parseDouble(c.get("bytes_per_row")),
            Integer.parseInt(c.get("scan_passes")),
            Integer.parseInt(c.get("string_keys")),
            Integer.parseInt(c.get("numeric_keys")),
            Integer.parseInt(c.get("date_keys")),
            Integer.parseInt(c.get("range_keys")),
            Integer.parseInt(c.get("filter_keys")),
            Integer.parseInt(c.get("composite_date_keys")),
            "1".equals(c.get("composite")),
            Integer.parseInt(c.get("simple_metrics")),
            "1".equals(c.get("extended_stats")),
            "1".equals(c.get("percentiles")),
            "1".equals(c.get("cardinality")),
            Double.parseDouble(c.get("distinct_values")),
            Double.parseDouble(c.get("filter_selectivity"))
        );
    }

    /** Reads the whole text of a test resource. */
    static String readText(String resource) throws IOException {
        try (InputStream in = CostMeasurements.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("missing test resource " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}

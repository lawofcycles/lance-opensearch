/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.lance.execute.AggregateSpecResolver.Column;
import org.opensearch.lance.execute.AggregateSpecResolver.Composite;
import org.opensearch.lance.execute.AggregateSpecResolver.KeyKind;
import org.opensearch.lance.execute.AggregateSpecResolver.Level;
import org.opensearch.lance.execute.AggregateSpecResolver.Metric;
import org.opensearch.lance.execute.AggregateSpecResolver.Source;
import org.opensearch.lance.execute.AggregateSpecResolver.TopKSpec;
import org.opensearch.lance.execute.LanceAggregateResults.MetricSlot;
import org.opensearch.lance.execute.LanceAggregateResults.PushedShape;
import org.opensearch.lance.query.ScanAdmission;

/**
 * What {@link AggregateSpecResolver#resolve} produced for one pushed
 * aggregate: the pushed aggregate's {@link PushedShape} (group key
 * count and metric slots, checked against the resolved tree; null for
 * a test instance) and its encoded main scan, the bucket levels of a nested tree ({@code levels}, empty for
 * a composite or a metrics only plan) or the composite shape
 * ({@code composite}, null otherwise), the top level metrics of a
 * plan without groupings, every metric of the plan by slot, the
 * percentiles bin count the bin scans are laid out with, and the
 * top-k selection of the single level {@code terms} shape (null when
 * every group is kept). The key accessors ({@link #keyCount},
 * {@link #keyKind}, {@link #dateInterval}) answer for whichever of
 * the two shapes is set, so the scan runner and the result assembler
 * read the key columns the same way. Immutable; it holds no scan or
 * result state.
 */
record ResolvedAggregate(PushedShape shape, ByteBuffer substrait, List<Level> levels, Composite composite, List<Metric> topMetrics, List<
    Metric> allMetrics, int percentilesBins, TopKSpec topK, long estimatedGroups) {

    /** As the full constructor with one estimated group, the metrics only and composite shapes' bound. */
    ResolvedAggregate(
        PushedShape shape,
        ByteBuffer substrait,
        List<Level> levels,
        Composite composite,
        List<Metric> topMetrics,
        List<Metric> allMetrics,
        int percentilesBins,
        TopKSpec topK
    ) {
        this(shape, substrait, levels, composite, topMetrics, allMetrics, percentilesBins, topK, 1L);
    }

    /**
     * Heap bytes per row of the scan's projection: the key columns and
     * the measure columns as their Arrow types size them, for the
     * admission gate's estimate of the batches in flight.
     */
    long projectedRowBytes() {
        long bytes = 0L;
        if (composite != null) {
            for (Source source : composite.sources()) {
                bytes += columnBytes(source.column());
            }
        } else {
            for (Level level : levels) {
                bytes += columnBytes(level.column());
            }
        }
        for (Metric metric : allMetrics) {
            bytes += columnBytes(metric.column());
        }
        // The count column every main scan row carries.
        return bytes + Long.BYTES;
    }

    private static long columnBytes(Column column) {
        if (column == null) {
            return Long.BYTES;
        }
        return ScanAdmission.columnWidthBytes(new Field(column.name(), FieldType.nullable(column.type()), null));
    }

    int keyCount() {
        return composite != null ? composite.sources().size() : levels.size();
    }

    /**
     * The encoded main scan, as a read only view. Package private
     * for the test sources' byte equivalence assertions between
     * two plans of the same request.
     */
    ByteBuffer substraitPlan() {
        return substrait.asReadOnlyBuffer();
    }

    /**
     * The parsed {@code after} value of every composite source, in
     * source order; null when the plan has no composite, all null
     * entries when the request carried no {@code after}. Package
     * private for the test sources' assertions on the composite
     * paging state, which the wire form does not carry (the
     * executor applies {@code after} to the sorted group rows).
     */
    List<Comparable<?>> compositeAfterValues() {
        if (composite == null) {
            return null;
        }
        List<Comparable<?>> values = new ArrayList<>(composite.sources().size());
        for (Source source : composite.sources()) {
            values.add(source.after());
        }
        return values;
    }

    long dateInterval(int key) {
        return composite != null ? composite.sources().get(key).dateInterval() : levels.get(key).dateInterval();
    }

    KeyKind keyKind(int key) {
        return composite != null ? composite.sources().get(key).keyKind() : levels.get(key).keyKind();
    }

    KeyKind[] keyKinds() {
        KeyKind[] kinds = new KeyKind[keyCount()];
        for (int key = 0; key < kinds.length; key++) {
            kinds[key] = keyKind(key);
        }
        return kinds;
    }

    /**
     * Whether the resolved levels and metrics line up with the pushed
     * aggregate's shape: same key count, same metric count, and per
     * slot the same kind and aggregation name. A mismatch means the
     * translator and this resolution disagree about the request's
     * shape; refusing keeps the request on the aggregators instead of
     * reading the scan's columns under wrong names.
     */
    boolean matchesSpecs() {
        if (shape == null) {
            return true;
        }
        if (shape.groupCount() != keyCount()) {
            return false;
        }
        List<MetricSlot> slots = shape.metrics();
        if (slots.size() != allMetrics.size()) {
            return false;
        }
        for (int slot = 0; slot < allMetrics.size(); slot++) {
            MetricSlot expected = slots.get(slot);
            Metric metric = allMetrics.get(slot);
            if (!expected.kind().name().equals(metric.kind().name()) || !expected.name().equals(metric.name())) {
                return false;
            }
        }
        return true;
    }
}

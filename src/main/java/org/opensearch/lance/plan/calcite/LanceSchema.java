/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

/**
 * Calcite schema over a fixed set of {@link LanceTable}s, one per Lance
 * backed index. The schema is a plain container: the caller builds the
 * table map (production wiring walks the cluster state for Lance backed
 * indexes; tests pass fixtures) and the planner resolves index names
 * against it.
 */
public final class LanceSchema extends AbstractSchema {

    private final Map<String, Table> tables;

    /**
     * @param tables index name to table; copied, so later mutations of the
     *     argument do not reach the schema
     */
    public LanceSchema(Map<String, LanceTable> tables) {
        this.tables = Map.copyOf(tables);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }
}

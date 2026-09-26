/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.opensearch.lance.StorageOptions;

/**
 * Table enumeration and location resolution against a catalog's
 * {@link LanceNamespace} handle. Implementations disagree on how the
 * root namespace is addressed and where tables live, so the walk here
 * tries the root shapes in turn and descends the namespace tree
 * depth-first, bounded by the registration's
 * {@code max_namespace_depth} config.
 */
final class LanceCatalogEnumerator {

    private static final Logger LOG = LogManager.getLogger(LanceCatalogEnumerator.class);

    private LanceCatalogEnumerator() {}

    /** One table the catalog names: its identifier path and its plain name (the last segment). */
    record CatalogTable(List<String> id, String name) {
    }

    /**
     * What one enumeration found: the tables, and the first failure of a
     * subnamespace listing during the walk, or {@code null} when every
     * namespace the walk reached listed its children. A catalog that
     * refuses a subnamespace (a Glue database or Iceberg namespace the
     * credentials do not cover) hides the tables below it, so the
     * failure is carried out for the poll to report instead of the
     * tables silently going missing.
     */
    record Enumeration(List<CatalogTable> tables, Exception firstNamespacesFailure) {
    }

    /** Failures the walk records on the way: the first table listing refusal and the first subnamespace listing refusal. */
    private static final class WalkFailures {
        Exception firstTables;
        Exception firstNamespaces;
    }

    /**
     * Config key naming the warehouse (Iceberg REST) or catalog
     * (Polaris) the table walk starts from. Its dot-separated segments
     * become the leading levels of every namespace and table id the
     * poll sends, because those clients address everything under a
     * warehouse and reject listings without one. Read by the plugin,
     * passed through to {@code initialize} like any other config key.
     */
    static final String WAREHOUSE_CONFIG_KEY = "warehouse";

    /**
     * Config key bounding how many namespace levels below the walk's
     * root the poll descends when the root itself does not answer a
     * table listing. The default reaches Glue databases, single-level
     * Iceberg namespaces under a warehouse, and Unity's fixed
     * catalog.schema shape.
     */
    static final String MAX_NAMESPACE_DEPTH_CONFIG_KEY = "max_namespace_depth";

    static final int DEFAULT_MAX_NAMESPACE_DEPTH = 2;

    /**
     * Enumerate the catalog's tables through the {@link LanceNamespace}
     * interface. Implementations disagree on how the root namespace is
     * addressed: DirectoryNamespace accepts a request without an id,
     * RestNamespace requires an explicit (empty) id for the root, Glue
     * rejects a root table listing outright because tables live inside
     * databases, Iceberg REST and Polaris root every id at the
     * registration's {@code warehouse} config and hold tables in
     * multi-level namespaces below it, and Unity holds tables at the
     * fixed two-level {@code catalog.schema}. The chain below tries the
     * root shapes in turn and finally walks the namespace tree
     * depth-first, bounded by {@code max_namespace_depth}.
     */
    static Enumeration enumerateTables(LanceNamespace handle, LanceNamespaceMetadata.Entry entry) throws Exception {
        List<String> root = walkRoot(entry);
        int maxDepth = maxNamespaceDepth(entry);
        Exception rootListingFailure = null;
        if (root.isEmpty()) {
            try {
                return new Enumeration(tablesAt(handle.listTables(new ListTablesRequest()), root), null);
            } catch (Exception noIdFailure) {
                rootListingFailure = noIdFailure;
            }
        }
        try {
            return new Enumeration(tablesAt(handle.listTables(new ListTablesRequest().id(root)), root), null);
        } catch (Exception explicitIdFailure) {
            if (rootListingFailure == null) {
                rootListingFailure = explicitIdFailure;
            }
        }
        List<CatalogTable> tables = new ArrayList<>();
        Set<List<String>> visited = new HashSet<>();
        visited.add(root);
        WalkFailures failures = new WalkFailures();
        try {
            walkNamespaces(handle, root, maxDepth, visited, tables, failures);
        } catch (Exception walkFailure) {
            // Neither root shape works and the walk cannot start; report
            // the root failure, which names the catalog's own error
            // rather than the fallback's.
            throw rootListingFailure;
        }
        if (tables.isEmpty() && failures.firstTables != null) {
            // Every namespace the walk reached refused its table listing.
            // An empty catalog answers empty listings instead, so this is
            // a real failure (revoked table permissions, wrong warehouse)
            // and the registration should show as unavailable rather than
            // silently surfacing nothing.
            throw failures.firstTables;
        }
        return new Enumeration(tables, failures.firstNamespaces);
    }

    /**
     * Depth-first walk over the namespaces below {@code parent},
     * collecting the tables of every namespace that answers a table
     * listing. A child that refuses its table listing is not a failure
     * on its own: Unity's first level (the catalog) and Iceberg's
     * warehouse level hold no tables and reject the request shape, and
     * the tables live one level further down. The first such refusal is
     * recorded so the caller can tell an empty catalog from one that
     * refused everything. A child that cannot list its own namespaces
     * is treated as a leaf, and the first such refusal is recorded too:
     * the tables below it stay hidden, which the poll reports.
     */
    private static void walkNamespaces(
        LanceNamespace handle,
        List<String> parent,
        int remainingDepth,
        Set<List<String>> visited,
        List<CatalogTable> tables,
        WalkFailures failures
    ) throws Exception {
        if (remainingDepth <= 0) {
            return;
        }
        ListNamespacesResponse children = handle.listNamespaces(new ListNamespacesRequest().id(parent));
        if (children.getNamespaces() == null) {
            return;
        }
        for (String child : children.getNamespaces()) {
            List<String> childId = childId(parent, child);
            if (visited.add(childId) == false) {
                // Glue answers every listNamespaces with the full database
                // list regardless of the parent id; the visited set keeps
                // that from looping.
                continue;
            }
            try {
                tables.addAll(tablesAt(handle.listTables(new ListTablesRequest().id(childId)), childId));
            } catch (Exception tablesFailure) {
                if (failures.firstTables == null) {
                    failures.firstTables = tablesFailure;
                }
                LOG.debug("table listing at {} failed; descending: {}", childId, tablesFailure.getMessage());
            }
            if (remainingDepth > 1) {
                try {
                    walkNamespaces(handle, childId, remainingDepth - 1, visited, tables, failures);
                } catch (Exception childWalkFailure) {
                    if (failures.firstNamespaces == null) {
                        failures.firstNamespaces = new IllegalStateException(
                            "namespace listing below " + childId + " failed: " + childWalkFailure.getMessage(),
                            childWalkFailure
                        );
                    }
                    LOG.warn(
                        "namespace listing below {} failed; treating it as a leaf, its tables are not surfaced: {}",
                        childId,
                        childWalkFailure.getMessage()
                    );
                }
            }
        }
    }

    /**
     * Build a child's full id from the parent id and the name the
     * catalog listed. Iceberg REST and Polaris return dot-joined full
     * paths rooted at the warehouse ({@code wh.ns1}); Unity and Glue
     * return the bare child name.
     */
    private static List<String> childId(List<String> parent, String child) {
        List<String> segments = List.of(child.split("\\."));
        if (segments.size() > parent.size() && segments.subList(0, parent.size()).equals(parent)) {
            return segments;
        }
        List<String> id = new ArrayList<>(parent.size() + 1);
        id.addAll(parent);
        id.add(child);
        return List.copyOf(id);
    }

    private static List<String> walkRoot(LanceNamespaceMetadata.Entry entry) {
        String warehouse = entry.config().get(WAREHOUSE_CONFIG_KEY);
        if (warehouse == null || warehouse.isEmpty()) {
            return List.of();
        }
        return List.of(warehouse.split("\\."));
    }

    private static int maxNamespaceDepth(LanceNamespaceMetadata.Entry entry) {
        String raw = entry.config().get(MAX_NAMESPACE_DEPTH_CONFIG_KEY);
        if (raw == null || raw.isEmpty()) {
            return DEFAULT_MAX_NAMESPACE_DEPTH;
        }
        int depth;
        try {
            depth = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[" + MAX_NAMESPACE_DEPTH_CONFIG_KEY + "] must be a positive integer, got [" + raw + "]");
        }
        if (depth < 1) {
            throw new IllegalArgumentException("[" + MAX_NAMESPACE_DEPTH_CONFIG_KEY + "] must be a positive integer, got [" + raw + "]");
        }
        return depth;
    }

    private static List<CatalogTable> tablesAt(ListTablesResponse response, List<String> namespaceId) {
        List<CatalogTable> tables = new ArrayList<>();
        if (response.getTables() != null) {
            for (String table : response.getTables()) {
                List<String> id = new ArrayList<>(namespaceId.size() + 1);
                id.addAll(namespaceId);
                id.add(table);
                tables.add(new CatalogTable(List.copyOf(id), table));
            }
        }
        return tables;
    }

    /**
     * The single place a table location is read out of a
     * {@code DescribeTableResponse}, whichever implementation produced
     * it. Every catalog implementation maps its own location field into
     * the response's {@code location} before returning: directory and
     * REST set it natively, Glue reads its storage descriptor, Iceberg
     * REST copies the table metadata's location, Polaris the generic
     * table's base location, and Unity the table's storage location. A
     * trailing slash (Glue storage descriptors sometimes carry one) is
     * stripped so the value matches the path shape
     * {@code index.lance.table} persists.
     */
    static String tableLocation(DescribeTableResponse response) {
        if (response == null) {
            return null;
        }
        String location = response.getLocation();
        if (location == null || location.isEmpty()) {
            return null;
        }
        return location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
    }

    /**
     * Overlay the registration's storage options on whatever the
     * catalog returned with the table (Glue passes its
     * {@code storage.*} config through, REST catalogs may vend
     * credentials). The registration's values win so an operator can
     * override what the catalog hands out.
     */
    static StorageOptions mergeStorageOptions(Map<String, String> fromCatalog, StorageOptions fromEntry) {
        if (fromCatalog == null || fromCatalog.isEmpty()) {
            return fromEntry;
        }
        Map<String, String> merged = new LinkedHashMap<>(fromCatalog);
        merged.putAll(fromEntry.asMap());
        return StorageOptions.of(merged);
    }
}

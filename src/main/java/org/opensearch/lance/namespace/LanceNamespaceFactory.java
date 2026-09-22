/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.arrow.memory.BufferAllocator;
import org.lance.namespace.DirectoryNamespace;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.RestNamespace;
import org.lance.namespace.glue.GlueNamespace;

/**
 * Builds the runtime {@link LanceNamespace} handle for a cluster-state
 * {@link LanceNamespaceMetadata.Entry}. The switch over the entry's
 * type is the single place a new catalog implementation plugs into;
 * everything downstream of {@link #create} works against the
 * interface.
 *
 * <p>The {@code initialize} properties are the entry's config as-is —
 * unknown keys belong to the implementation, and secret-bearing values
 * arrive unredacted — plus, for the directory type, the {@code root}
 * property carrying the registered path.
 */
public final class LanceNamespaceFactory {

    private LanceNamespaceFactory() {}

    /**
     * Instantiation seam: maps a namespace type to a fresh,
     * uninitialised implementation. Tests swap this out to hand the
     * service a stub and observe the properties {@link #create}
     * passes to {@code initialize}.
     */
    private static volatile Function<String, LanceNamespace> instantiator = LanceNamespaceFactory::newImplementation;

    /** Instantiate and initialise the implementation for {@code entry}. */
    public static LanceNamespace create(LanceNamespaceMetadata.Entry entry, BufferAllocator allocator) {
        LanceNamespace namespace = instantiator.apply(entry.type());
        Map<String, String> properties = new HashMap<>(entry.config());
        if (LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(entry.type())) {
            properties.put("root", entry.rootUri());
        }
        namespace.initialize(properties, allocator);
        return namespace;
    }

    private static LanceNamespace newImplementation(String type) {
        return switch (type) {
            case LanceNamespaceMetadata.Entry.TYPE_DIRECTORY -> new DirectoryNamespace();
            case LanceNamespaceMetadata.Entry.TYPE_REST -> new RestNamespace();
            case LanceNamespaceMetadata.Entry.TYPE_GLUE -> new GlueNamespace();
            default -> throw new IllegalArgumentException(
                "unknown namespace type [" + type + "]; accepted values are " + LanceNamespaceMetadata.Entry.ACCEPTED_TYPES
            );
        };
    }

    static void setInstantiatorForTests(Function<String, LanceNamespace> replacement) {
        instantiator = replacement;
    }

    static void resetInstantiatorForTests() {
        instantiator = LanceNamespaceFactory::newImplementation;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.indices.IndicesService;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Whether a reader wrapper (the security plugin's DLS / FLS wrapper, or
 * any other plugin's {@code IndexModule.setReaderWrapper}) is installed
 * on this node, read for the explain endpoint's prediction of the
 * {@code SECURITY_WRAPPER} refinement.
 *
 * <p>The wrapper lives on the data node's {@link IndexService}, which
 * the coordinating node does not necessarily hold. The probe reads the
 * explained index's own service when this node has it, and otherwise
 * any index service of this node: a plugin that installs a wrapper does
 * so through {@code onIndexModule} for every index it covers, and the
 * same plugins run on every node, so a wrapper on one local index is
 * the best static evidence that the data nodes wrap the explained index
 * too. A node holding no index at all answers false. The data node
 * decides at execution time; the prediction is what this node can see.
 *
 * <p>{@code IndexService.getReaderWrapper()} is package private in
 * OpenSearch core, so the probe reaches it through the same reflective
 * accessor the fragment executor uses to apply the wrapper; the plugin
 * security policy already grants {@code suppressAccessChecks}.
 */
final class ReaderWrapperProbe {

    private static final Method GET_READER_WRAPPER = resolveAccessor();

    private ReaderWrapperProbe() {}

    @SuppressForbidden(reason = "IndexService#getReaderWrapper() is package-private in core; "
        + "reflection is the only way to see the wrapper the executor will apply")
    private static Method resolveAccessor() {
        try {
            Method method = IndexService.class.getDeclaredMethod("getReaderWrapper");
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("IndexService#getReaderWrapper is not available on this OpenSearch build", e);
        }
    }

    /**
     * Whether a reader wrapper is installed on {@code index}'s service
     * when this node holds it, else on any index service of this node.
     */
    static boolean installed(IndicesService indicesService, Index index) {
        IndexService own = indicesService.indexService(index);
        if (own != null) {
            return hasWrapper(own);
        }
        for (IndexService service : indicesService) {
            if (hasWrapper(service)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasWrapper(IndexService indexService) {
        try {
            return GET_READER_WRAPPER.invoke(indexService) != null;
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("cannot access IndexService#getReaderWrapper via reflection", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("failed to obtain IndexService reader wrapper", cause);
        }
    }
}

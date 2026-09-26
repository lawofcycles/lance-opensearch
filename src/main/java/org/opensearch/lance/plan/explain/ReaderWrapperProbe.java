/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Whether a reader wrapper (the security plugin's DLS / FLS wrapper, or
 * any other plugin's {@code IndexModule.setReaderWrapper}) is installed
 * on this node, read for the explain endpoint's prediction of the
 * {@code SECURITY_WRAPPER} refinement and for the coordinator's result
 * cache, which must not cache an answer the wrapper shaped for one user.
 *
 * <p>The wrapper lives on the data node's {@link IndexService}, which
 * the coordinating node does not necessarily hold. {@link #installed}
 * reads the explained index's own service when this node has it, and
 * otherwise any index service of this node: a plugin that installs a
 * wrapper does so through {@code onIndexModule} for every index it
 * covers, and the same plugins run on every node, so a wrapper on one
 * local index is the best static evidence that the data nodes wrap the
 * explained index too. A node holding no index at all answers false.
 * The data node decides at execution time; the prediction is what this
 * node can see. {@link #installedOn} answers for one index exactly: the
 * node's own service when it has one, else a request scoped temporary
 * service built from the index metadata, which runs the plugins'
 * {@code onIndexModule} hooks the way the fragment executor's does.
 *
 * <p>{@code IndexService.getReaderWrapper()} is package private in
 * OpenSearch core, so the probe reaches it through the same reflective
 * accessor the fragment executor uses to apply the wrapper; the plugin
 * security policy already grants {@code suppressAccessChecks}.
 */
public final class ReaderWrapperProbe {

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
    public static boolean installed(IndicesService indicesService, Index index) {
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

    /**
     * Whether a reader wrapper is installed on the index of
     * {@code metadata}: read off this node's own service when it holds
     * one, else off a temporary service built from the metadata for the
     * duration of the call, which the plugins' {@code onIndexModule}
     * hooks run against as they do for the executor's. When the cluster
     * state applier registers the node's own service while the temporary
     * one is being built, the registered one answers; if it is gone
     * again by then, the answer is {@code true}, the side that never
     * lets a wrapped answer through.
     */
    public static boolean installedOn(IndicesService indicesService, IndexMetadata metadata) throws IOException {
        IndexService own = indicesService.indexService(metadata.getIndex());
        if (own != null) {
            return hasWrapper(own);
        }
        try {
            return indicesService.withTempIndexService(metadata, ReaderWrapperProbe::hasWrapper);
        } catch (ResourceAlreadyExistsException raced) {
            IndexService registered = indicesService.indexService(metadata.getIndex());
            return registered == null || hasWrapper(registered);
        }
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

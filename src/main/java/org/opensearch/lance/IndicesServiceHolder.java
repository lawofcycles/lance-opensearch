/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;

import org.opensearch.common.SetOnce;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.indices.IndicesService;

/**
 * The node's {@link IndicesService}, for the parts of the plugin that are
 * built before Guice runs and need it afterwards.
 *
 * <p>{@code Plugin.createComponents} does not receive the
 * {@link IndicesService}, and the engine factory the plugin hands out in
 * {@code getEngineFactory} is built from the plugin's own fields. The
 * plugin returns one holder from {@code createComponents}, which binds
 * it in Guice, and names {@link Binder} in {@code getGuiceServiceClasses};
 * Guice constructs the binder with the holder and the node's
 * {@link IndicesService} while the node is assembled, before the node
 * starts and so before any index service, shard or engine exists. From
 * then on {@link #get} answers the service.
 */
public final class IndicesServiceHolder {

    private final SetOnce<IndicesService> indicesService = new SetOnce<>();

    /** The node's {@link IndicesService}, or {@code null} before Guice bound it (a test harness without a node). */
    public IndicesService get() {
        return indicesService.get();
    }

    void set(IndicesService service) {
        indicesService.set(service);
    }

    /**
     * Lifecycle component Guice constructs with the node's
     * {@link IndicesService}; its only work is handing the service to
     * the holder. It starts, stops and closes nothing.
     */
    public static final class Binder extends AbstractLifecycleComponent {

        @Inject
        public Binder(IndicesServiceHolder holder, IndicesService indicesService) {
            holder.set(indicesService);
        }

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}

        @Override
        protected void doClose() throws IOException {}
    }
}

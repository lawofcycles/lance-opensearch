/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.get.GetIndexAction;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.tasks.Task;

/**
 * ActionFilter that withholds the credential entries of
 * {@code index.plugins.lance.storage_options.*} from {@code GET /<index>} (the
 * get index API, {@code indices:admin/get}). The stock
 * {@code TransportGetIndexAction} applies OpenSearch's settings filter
 * to the response's {@code defaults} block only and writes each index's
 * settings as they are in the cluster state, so the patterns the plugin
 * registers through {@code getSettingsFilter()}, which cover
 * {@code GET /<index>/_settings} and the cluster state API, do not reach
 * this one view. The filter runs those same patterns
 * ({@link StorageOptions#SENSITIVE_INDEX_SETTING_PATTERNS}) over the
 * settings of every Lance backed index in the response and hands the
 * caller a response with them replaced. The settings of an index
 * without {@code index.plugins.lance.table} pass through untouched, as does a
 * response that carries no settings.
 */
public final class LanceGetIndexActionFilter implements ActionFilter {

    private final SettingsFilter settingsFilter = new SettingsFilter(StorageOptions.SENSITIVE_INDEX_SETTING_PATTERNS);

    /**
     * After the security plugin's filter ({@code Integer.MIN_VALUE}),
     * which authorises the request this filter only reshapes the answer
     * of, and after {@code LanceClearCacheActionFilter}
     * ({@code Integer.MIN_VALUE + 100}); the two Lance filters act on
     * different actions, the distinct value only keeps their order
     * explicit.
     */
    @Override
    public int order() {
        return Integer.MIN_VALUE + 101;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (!GetIndexAction.NAME.equals(action)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        chain.proceed(task, action, request, ActionListener.map(listener, response -> {
            if (response instanceof GetIndexResponse getIndex) {
                @SuppressWarnings("unchecked")
                Response filtered = (Response) withFilteredSettings(getIndex);
                return filtered;
            }
            return response;
        }));
    }

    /**
     * {@code response} with the credential entries removed from the
     * settings of its Lance backed indexes; {@code response} itself when
     * it lists none.
     */
    GetIndexResponse withFilteredSettings(GetIndexResponse response) {
        Map<String, Settings> settings = response.settings();
        Map<String, Settings> filtered = null;
        for (Map.Entry<String, Settings> entry : settings.entrySet()) {
            String table = LanceEngineFactory.tableOf(entry.getValue());
            if (table == null || table.isEmpty()) {
                continue;
            }
            if (filtered == null) {
                filtered = new HashMap<>(settings);
            }
            filtered.put(entry.getKey(), settingsFilter.filter(entry.getValue()));
        }
        if (filtered == null) {
            return response;
        }
        return new GetIndexResponse(
            response.indices(),
            response.mappings(),
            response.aliases(),
            filtered,
            response.defaultSettings(),
            response.dataStreams(),
            response.contexts()
        );
    }
}

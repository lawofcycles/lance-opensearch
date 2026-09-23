/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.lance.plan.execute.MergeReducer;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.transport.client.node.NodeClient;

/**
 * The {@code inner_hits} expansion of a collapsed page, the step
 * {@code ExpandSearchPhase} runs on the shard path after the fetch
 * phase: one {@link MultiSearchRequest} through the node client with,
 * per collapsed hit and per {@code inner_hits} block, the request's
 * query wrapped in a {@code bool} that pins the hit's collapse value
 * (a {@code match} on the collapse field, or {@code must_not exists}
 * for the group of the missing value) under the block's own
 * {@code from}, {@code size}, {@code sort}, {@code _source},
 * {@code docvalue_fields}, {@code fields}, {@code stored_fields},
 * {@code explain}, {@code track_scores}, {@code version},
 * {@code seq_no_primary_term} and nested {@code collapse}, with the
 * request's {@code post_filter}. Each group search is an ordinary search
 * on the same indices: it passes {@link LanceDispatchActionFilter} again
 * and runs on the fragment path (it carries no {@code collapse} unless
 * the block nests one, and a nested collapse expands in turn, so the
 * recursion ends with the body). {@code max_concurrent_group_searches}
 * bounds the multi search's concurrency as on the shard path. The
 * results attach to each hit as its {@code inner_hits}; a failed group
 * search fails the request.
 */
final class CollapseExpansion {

    private final NodeClient client;
    private final String localNodeId;
    private final SearchRequest searchRequest;
    private final CollapseBuilder collapse;
    private final CancellableTask task;
    private final int defaultConcurrency;

    /**
     * @param defaultConcurrency how many group searches run at once when
     *     the body sets no {@code max_concurrent_group_searches}: the size
     *     of the {@code lance_coordinator} pool, so the group searches,
     *     each of which enters that pool through the dispatch filter, do
     *     not fill its queue by themselves (the multi search's own
     *     default is sized for the search pool)
     */
    CollapseExpansion(
        NodeClient client,
        String localNodeId,
        SearchRequest searchRequest,
        CollapseBuilder collapse,
        CancellableTask task,
        int defaultConcurrency
    ) {
        this.client = client;
        this.localNodeId = localNodeId;
        this.searchRequest = searchRequest;
        this.collapse = collapse;
        this.task = task;
        this.defaultConcurrency = Math.max(1, defaultConcurrency);
    }

    /**
     * Attach the inner hits of every collapsed group of {@code response}
     * and complete {@code listener} with it; a response without hits
     * completes at once.
     */
    void expand(SearchResponse response, ActionListener<SearchResponse> listener) {
        SearchHit[] hits = response.getHits().getHits();
        List<InnerHitBuilder> innerHitBuilders = collapse.getInnerHits();
        if (hits.length == 0 || innerHitBuilders == null || innerHitBuilders.isEmpty()) {
            listener.onResponse(response);
            return;
        }
        MultiSearchRequest multiRequest = new MultiSearchRequest();
        multiRequest.maxConcurrentSearchRequests(
            collapse.getMaxConcurrentGroupRequests() > 0 ? collapse.getMaxConcurrentGroupRequests() : defaultConcurrency
        );
        SearchSourceBuilder source = searchRequest.source();
        for (SearchHit hit : hits) {
            BoolQueryBuilder groupQuery = new BoolQueryBuilder();
            Object collapseValue = MergeReducer.collapseValueOf(hit, collapse.getField());
            if (collapseValue != null) {
                groupQuery.filter(QueryBuilders.matchQuery(collapse.getField(), collapseValue));
            } else {
                groupQuery.mustNot(QueryBuilders.existsQuery(collapse.getField()));
            }
            QueryBuilder originalQuery = source == null ? null : source.query();
            if (originalQuery != null) {
                groupQuery.must(originalQuery);
            }
            for (InnerHitBuilder innerHitBuilder : innerHitBuilders) {
                SearchSourceBuilder groupSource = groupSource(innerHitBuilder).query(groupQuery)
                    .postFilter(source == null ? null : source.postFilter());
                SearchRequest groupRequest = new SearchRequest(searchRequest);
                groupRequest.source(groupSource);
                multiRequest.add(groupRequest);
            }
        }
        if (task != null) {
            // The group searches are children of the coordinator task,
            // so a cancelled request stops them too.
            multiRequest.setParentTask(localNodeId, task.getId());
        }
        client.multiSearch(multiRequest, ActionListener.wrap(multiResponse -> {
            Iterator<MultiSearchResponse.Item> items = multiResponse.iterator();
            for (SearchHit hit : hits) {
                for (InnerHitBuilder innerHitBuilder : innerHitBuilders) {
                    MultiSearchResponse.Item item = items.next();
                    if (item.isFailure()) {
                        listener.onFailure(item.getFailure());
                        return;
                    }
                    SearchHits innerHits = item.getResponse().getHits();
                    if (hit.getInnerHits() == null) {
                        hit.setInnerHits(new HashMap<>(innerHitBuilders.size()));
                    }
                    hit.getInnerHits().put(innerHitBuilder.getName(), innerHits);
                }
            }
            listener.onResponse(response);
        }, listener::onFailure));
    }

    /**
     * The body of one group search from an {@code inner_hits} block, the
     * elements {@code ExpandSearchPhase.buildExpandSearchSourceBuilder}
     * copies over; the caller sets the query and the post filter.
     */
    private static SearchSourceBuilder groupSource(InnerHitBuilder options) {
        SearchSourceBuilder groupSource = new SearchSourceBuilder();
        groupSource.from(options.getFrom());
        groupSource.size(options.getSize());
        if (options.getSorts() != null) {
            options.getSorts().forEach(groupSource::sort);
        }
        if (options.getFetchSourceContext() != null) {
            if (options.getFetchSourceContext().includes().length == 0 && options.getFetchSourceContext().excludes().length == 0) {
                groupSource.fetchSource(options.getFetchSourceContext().fetchSource());
            } else {
                groupSource.fetchSource(options.getFetchSourceContext().includes(), options.getFetchSourceContext().excludes());
            }
        }
        if (options.getFetchFields() != null) {
            options.getFetchFields().forEach(field -> groupSource.fetchField(field.field, field.format));
        }
        if (options.getDocValueFields() != null) {
            options.getDocValueFields().forEach(field -> groupSource.docValueField(field.field, field.format));
        }
        if (options.getStoredFieldsContext() != null && options.getStoredFieldsContext().fieldNames() != null) {
            options.getStoredFieldsContext().fieldNames().forEach(groupSource::storedField);
        }
        if (options.getScriptFields() != null) {
            for (SearchSourceBuilder.ScriptField field : options.getScriptFields()) {
                groupSource.scriptField(field.fieldName(), field.script());
            }
        }
        if (options.getHighlightBuilder() != null) {
            groupSource.highlighter(options.getHighlightBuilder());
        }
        groupSource.explain(options.isExplain());
        groupSource.trackScores(options.isTrackScores());
        groupSource.version(options.isVersion());
        groupSource.seqNoAndPrimaryTerm(options.isSeqNoAndPrimaryTerm());
        if (options.getInnerCollapseBuilder() != null) {
            groupSource.collapse(options.getInnerCollapseBuilder());
        }
        return groupSource;
    }
}

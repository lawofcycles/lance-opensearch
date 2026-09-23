/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner;
import org.opensearch.lance.plan.rel.ShardPathReason;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Response for {@link LanceExplainAction}: what the coordinator would
 * do with the search body against the index.
 *
 * <p>{@code route} says which path answers the request: {@code fragment}
 * when the coordinator fans the request out to the data nodes, or
 * {@code shard_path} when the body holds an element only OpenSearch's
 * regular shard search serves, in which case {@code reasons} lists the
 * {@link ShardPathReason}s. {@code logical} is the tree the translator
 * built and {@code physical} the tree the planner chose, each as
 * {@code RelOptUtil.toString} renders them, one node per line; on the
 * fragment route the physical tree is the coordinator's
 * {@code MergeExec(FanOutExec(per node plan))}, on the shard path the
 * {@code ShardPathFallbackExec} root. The text is for humans and will
 * change as the planner grows traits and costs.
 *
 * <p>On the fragment route, {@code fragment_plan} is the
 * {@link FragmentPlan} the coordinator ships with every per node
 * request (the same parts the data node logs under {@code lance.plan}),
 * {@code unplanned} names the request element the translator refused
 * when one kept the envelope or the query on the Lucene side (absent
 * when everything translated), and {@code refinements_possible} lists
 * the node local downgrades that could still move a pushed operation to
 * Lucene, predicted from the mapping and the plan; it is a prediction,
 * the data node decides, and the counts it decided with are under
 * {@code plan.refinements} in {@code GET /_lance/stats}.
 *
 * <p>The stream fields are read and written unconditionally, so the
 * wire format is not rolling upgrade safe; the plugin has no mixed
 * version story yet, as the backwards-compatibility policy on
 * {@link org.opensearch.lance.namespace.LanceNamespaceMetadata} spells
 * out.
 */
public final class LanceExplainResponse extends ActionResponse implements ToXContentObject {

    /** Which path answers the request. */
    public enum Route {
        /** The coordinator fans the request out to the data nodes. */
        FRAGMENT,
        /** OpenSearch's regular shard search answers the request. */
        SHARD_PATH;

        /** The JSON value: the constant name in lower case. */
        public String jsonName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final String index;
    private final Route route;
    private final List<ShardPathReason> reasons;
    private final String logical;
    private final String physical;
    private final FragmentPlan fragmentPlan;
    private final String unplanned;
    private final List<FragmentPlanRefiner.Reason> refinementsPossible;

    /**
     * A fragment route answer.
     *
     * @param unplanned the element the translator refused, or null
     * @param refinementsPossible the predicted node local downgrades, empty when none apply
     */
    public static LanceExplainResponse fragment(
        String index,
        String logical,
        String physical,
        FragmentPlan fragmentPlan,
        String unplanned,
        List<FragmentPlanRefiner.Reason> refinementsPossible
    ) {
        return new LanceExplainResponse(
            index,
            Route.FRAGMENT,
            List.of(),
            logical,
            physical,
            Objects.requireNonNull(fragmentPlan, "fragmentPlan"),
            unplanned,
            refinementsPossible
        );
    }

    /** A shard path answer: {@code reasons} must not be empty. */
    public static LanceExplainResponse shardPath(String index, List<ShardPathReason> reasons, String logical, String physical) {
        if (reasons.isEmpty()) {
            throw new IllegalArgumentException("a shard path route needs at least one reason");
        }
        return new LanceExplainResponse(index, Route.SHARD_PATH, reasons, logical, physical, null, null, List.of());
    }

    private LanceExplainResponse(
        String index,
        Route route,
        List<ShardPathReason> reasons,
        String logical,
        String physical,
        FragmentPlan fragmentPlan,
        String unplanned,
        List<FragmentPlanRefiner.Reason> refinementsPossible
    ) {
        this.index = Objects.requireNonNull(index, "index");
        this.route = Objects.requireNonNull(route, "route");
        this.reasons = List.copyOf(reasons);
        this.logical = Objects.requireNonNull(logical, "logical");
        this.physical = Objects.requireNonNull(physical, "physical");
        this.fragmentPlan = fragmentPlan;
        this.unplanned = unplanned;
        this.refinementsPossible = List.copyOf(refinementsPossible);
    }

    public LanceExplainResponse(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.route = in.readEnum(Route.class);
        this.reasons = in.readList(input -> input.readEnum(ShardPathReason.class));
        this.logical = in.readString();
        this.physical = in.readString();
        this.fragmentPlan = in.readOptionalWriteable(FragmentPlan::new);
        this.unplanned = in.readOptionalString();
        this.refinementsPossible = in.readList(input -> input.readEnum(FragmentPlanRefiner.Reason.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeEnum(route);
        out.writeCollection(reasons, StreamOutput::writeEnum);
        out.writeString(logical);
        out.writeString(physical);
        out.writeOptionalWriteable(fragmentPlan);
        out.writeOptionalString(unplanned);
        out.writeCollection(refinementsPossible, StreamOutput::writeEnum);
    }

    public String index() {
        return index;
    }

    public Route route() {
        return route;
    }

    /** The shard path reasons; empty on the fragment route. */
    public List<ShardPathReason> reasons() {
        return reasons;
    }

    public String logical() {
        return logical;
    }

    public String physical() {
        return physical;
    }

    /** The plan the coordinator ships; null on the shard path route. */
    public FragmentPlan fragmentPlan() {
        return fragmentPlan;
    }

    /** The element the translator refused, or null. */
    public String unplanned() {
        return unplanned;
    }

    /** The predicted node local downgrades; empty when none apply or on the shard path route. */
    public List<FragmentPlanRefiner.Reason> refinementsPossible() {
        return refinementsPossible;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("route", route.jsonName());
        if (route == Route.SHARD_PATH) {
            builder.startArray("reasons");
            for (ShardPathReason reason : reasons) {
                builder.value(reason.name());
            }
            builder.endArray();
        }
        builder.field("logical", logical);
        builder.field("physical", physical);
        if (fragmentPlan != null) {
            builder.field("fragment_plan");
            fragmentPlan.toXContent(builder, params);
        }
        if (unplanned != null) {
            builder.field("unplanned", unplanned);
        }
        if (route == Route.FRAGMENT) {
            builder.startArray("refinements_possible");
            for (FragmentPlanRefiner.Reason reason : refinementsPossible) {
                builder.value(reason.statsKey());
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceExplainResponse other)) {
            return false;
        }
        return index.equals(other.index)
            && route == other.route
            && reasons.equals(other.reasons)
            && logical.equals(other.logical)
            && physical.equals(other.physical)
            && Objects.equals(fragmentPlan, other.fragmentPlan)
            && Objects.equals(unplanned, other.unplanned)
            && refinementsPossible.equals(other.refinementsPossible);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, route, reasons, logical, physical, fragmentPlan, unplanned, refinementsPossible);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.apache.calcite.plan.RelTraitSet;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.WireVersion;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner;
import org.opensearch.lance.plan.traits.Accuracy;
import org.opensearch.lance.plan.traits.PlanRequirement;
import org.opensearch.lance.plan.traits.TieStability;
import org.opensearch.lance.plan.traits.TraitEnforcement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Response for {@link LanceExplainAction}: what the coordinator would
 * do with the search body against the index.
 *
 * <p>{@code route} says what happens to the request: {@code fragment}
 * when the coordinator fans the request out to the data nodes, or
 * {@code unsupported} when the body carries an element no plan answers
 * ({@code suggest}, {@code highlight}), in which case {@code unplanned}
 * carries the message the search endpoint refuses the same body with
 * (400) and nothing else is planned or rendered. On the fragment route
 * {@code logical} is the tree the translator built and {@code physical}
 * the tree the planner chose, each one node per line as
 * {@code PlanText} renders them, every physical operator carrying its
 * {@link Accuracy}, its {@link TieStability} and its cost; the physical
 * tree is the coordinator's {@code MergeExec(FanOutExec(per node plan))}.
 * The text is for humans.
 *
 * <p>On the fragment route, {@code fragment_plan} is the
 * {@link FragmentPlan} the coordinator ships with every per node
 * request (the same parts the data node logs under {@code lance.plan}),
 * {@code unplanned} names the request element the translator refused
 * when one kept the envelope or the query on the Lucene side (absent
 * when everything translated), and {@code refinements_possible} lists
 * the node local downgrades that could still move a pushed operation to
 * Lucene, predicted from the mapping and the plan, in
 * {@link FragmentPlanRefiner.Reason} order; it is a prediction, the
 * data node decides, and the counts it decided with are under
 * {@code plan.refinements} in {@code GET /_lance/stats}. When no plan
 * of the request declares the traits it demands ({@link #planFailed}),
 * the route is still {@code fragment} but nothing ships:
 * {@code fragment_plan} is absent, {@code unplanned} carries the
 * {@code plan_failed} message the search endpoint answers 400 with, and
 * {@code physical} shows the cheapest plan the demand refused.
 *
 * <p>{@code traits} describes the trait side of the plan: what the
 * request demanded ({@code requested}), what the plan root declares
 * ({@code declared}) and whether the enforcer, the second Volcano pass
 * with the demand on the root, fired ({@code enforcer}, {@code none}
 * when the cheapest plan already met the demand or there was none).
 *
 * <p>The stream opens with {@link #WIRE_VERSION} (see
 * {@link WireVersion}). Version 1 carried the retired shard path route
 * with a reasons list, and plan texts and traits that were always
 * present; version 2 dropped the list and made those optional for the
 * unsupported route. A reader of this version decodes a version 1
 * stream by mapping a shard path answer to an unsupported one whose
 * message is {@link #SHARD_PATH_RETIRED}, and reads a fragment answer
 * field by field. The response travels from the node that planned the
 * explain body to the node that received the REST call when they
 * differ.
 */
public final class LanceExplainResponse extends ActionResponse implements ToXContentObject {

    /**
     * The wire format's version, the first field written and the first
     * read. Bumped when a field is added; 2 dropped the reasons list of
     * the retired fallback route and made the plan texts and the traits
     * optional for the unsupported route, the last change to the base
     * fields the format allows.
     */
    public static final int WIRE_VERSION = 2;

    /** The {@code unplanned} message of a shard path answer read from a version 1 stream. */
    public static final String SHARD_PATH_RETIRED =
        "the node that planned this request routed it to OpenSearch's shard search, a route this plugin version no longer has";

    /** What happens to the request. */
    public enum Route {
        /** The coordinator fans the request out to the data nodes. */
        FRAGMENT,
        /** No plan answers the body; the search endpoint refuses it with 400. */
        UNSUPPORTED;

        /** The JSON value: the constant name in lower case. */
        public String jsonName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * The trait side of the plan. {@code requested} is what the request
     * demanded of the plan root: the {@link Accuracy} ({@code APPROXIMATE},
     * the trait def's default, when nothing demanded one) and the
     * {@link TieStability} (rendered {@code NONE} when no cursor
     * demanded one). {@code declared} is what the plan root declares.
     * {@code enforcer} is {@link TraitEnforcement#describe()}.
     *
     * @param requestedAccuracy the accuracy demanded, the default when none
     * @param requestedTieStability the tie stability demanded, null when none
     * @param declaredAccuracy the plan root's accuracy
     * @param declaredTieStability the plan root's tie stability
     * @param enforcer {@code none}, or what the enforcer did
     */
    public record Traits(Accuracy requestedAccuracy, TieStability requestedTieStability, Accuracy declaredAccuracy,
        TieStability declaredTieStability, String enforcer) implements Writeable {

        /** The JSON value of a tie stability nobody demanded. */
        public static final String NO_TIE_STABILITY_DEMAND = "NONE";

        public Traits {
            Objects.requireNonNull(requestedAccuracy, "requestedAccuracy");
            Objects.requireNonNull(declaredAccuracy, "declaredAccuracy");
            Objects.requireNonNull(declaredTieStability, "declaredTieStability");
            Objects.requireNonNull(enforcer, "enforcer");
        }

        /** The traits of a plan whose root declares {@code root}, planned under {@code enforcement}. */
        public static Traits of(TraitEnforcement enforcement, RelTraitSet root) {
            PlanRequirement requirement = enforcement.requirement();
            return new Traits(
                requirement.accuracy(),
                requirement.tieStabilityReason() == null ? null : requirement.tieStability(),
                PlanRequirement.declaredAccuracy(root),
                PlanRequirement.declaredTieStability(root),
                enforcement.describe()
            );
        }

        public static Traits read(StreamInput in) throws IOException {
            Accuracy requestedAccuracy = in.readEnum(Accuracy.class);
            TieStability requestedTieStability = in.readBoolean() ? in.readEnum(TieStability.class) : null;
            return new Traits(
                requestedAccuracy,
                requestedTieStability,
                in.readEnum(Accuracy.class),
                in.readEnum(TieStability.class),
                in.readString()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(requestedAccuracy);
            out.writeBoolean(requestedTieStability != null);
            if (requestedTieStability != null) {
                out.writeEnum(requestedTieStability);
            }
            out.writeEnum(declaredAccuracy);
            out.writeEnum(declaredTieStability);
            out.writeString(enforcer);
        }

        /** The JSON value of {@code requested.tie_stability}. */
        public String requestedTieStabilityName() {
            return requestedTieStability == null ? NO_TIE_STABILITY_DEMAND : requestedTieStability.name();
        }

        XContentBuilder toXContent(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.startObject("requested");
            builder.field("accuracy", requestedAccuracy.name());
            builder.field("tie_stability", requestedTieStabilityName());
            builder.endObject();
            builder.startObject("declared");
            builder.field("accuracy", declaredAccuracy.name());
            builder.field("tie_stability", declaredTieStability.name());
            builder.endObject();
            builder.field("enforcer", enforcer);
            return builder.endObject();
        }
    }

    private final String index;
    private final Route route;
    private final String logical;
    private final String physical;
    private final FragmentPlan fragmentPlan;
    private final String unplanned;
    private final List<FragmentPlanRefiner.Reason> refinementsPossible;
    private final Traits traits;

    /**
     * A fragment route answer.
     *
     * @param unplanned the element the translator refused, or null
     * @param refinementsPossible the predicted node local downgrades, empty when none apply
     * @param traits the trait side of the plan
     */
    public static LanceExplainResponse fragment(
        String index,
        String logical,
        String physical,
        FragmentPlan fragmentPlan,
        String unplanned,
        List<FragmentPlanRefiner.Reason> refinementsPossible,
        Traits traits
    ) {
        return new LanceExplainResponse(
            index,
            Route.FRAGMENT,
            Objects.requireNonNull(logical, "logical"),
            Objects.requireNonNull(physical, "physical"),
            Objects.requireNonNull(fragmentPlan, "fragmentPlan"),
            unplanned,
            refinementsPossible,
            Objects.requireNonNull(traits, "traits")
        );
    }

    /**
     * A fragment route answer for a request no plan meets: nothing
     * ships, so there is no fragment plan and nothing to refine.
     *
     * @param physical the cheapest plan the demand refused, under the coordinator layer
     * @param planFailed the {@code plan_failed} message the search endpoint answers with
     * @param traits the demand, the cheapest plan's traits and the enforcer's refusal
     */
    public static LanceExplainResponse planFailed(String index, String logical, String physical, String planFailed, Traits traits) {
        return new LanceExplainResponse(
            index,
            Route.FRAGMENT,
            Objects.requireNonNull(logical, "logical"),
            Objects.requireNonNull(physical, "physical"),
            null,
            Objects.requireNonNull(planFailed, "planFailed"),
            List.of(),
            Objects.requireNonNull(traits, "traits")
        );
    }

    /**
     * An unsupported route answer: nothing was planned, and
     * {@code unplanned} carries the message the search endpoint refuses
     * the body with. No trees, no fragment plan, no traits.
     */
    public static LanceExplainResponse unsupported(String index, String unplanned) {
        return new LanceExplainResponse(
            index,
            Route.UNSUPPORTED,
            null,
            null,
            null,
            Objects.requireNonNull(unplanned, "unplanned"),
            List.of(),
            null
        );
    }

    private LanceExplainResponse(
        String index,
        Route route,
        String logical,
        String physical,
        FragmentPlan fragmentPlan,
        String unplanned,
        List<FragmentPlanRefiner.Reason> refinementsPossible,
        Traits traits
    ) {
        this.index = Objects.requireNonNull(index, "index");
        this.route = Objects.requireNonNull(route, "route");
        this.logical = logical;
        this.physical = physical;
        this.fragmentPlan = fragmentPlan;
        this.unplanned = unplanned;
        this.refinementsPossible = inReasonOrder(refinementsPossible);
        this.traits = traits;
    }

    /** {@code reasons} sorted in {@link FragmentPlanRefiner.Reason} order, so the array reads the same for every caller. */
    private static List<FragmentPlanRefiner.Reason> inReasonOrder(List<FragmentPlanRefiner.Reason> reasons) {
        List<FragmentPlanRefiner.Reason> sorted = new ArrayList<>(reasons);
        sorted.sort(Comparator.naturalOrder());
        return List.copyOf(sorted);
    }

    public LanceExplainResponse(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceExplainResponse", WIRE_VERSION, 2);
        this.index = in.readString();
        if (reader.marker() == 1) {
            // Version 1 laid the response out for the retired shard path
            // route: a reasons list after the route, plan texts and
            // traits that were always present. A shard path answer
            // becomes an unsupported answer, which is what the search
            // endpoint does with such a body today; the reasons are
            // constants of a type that no longer exists, so they are
            // read past and the message names the route instead.
            int routeOrdinal = in.readVInt();
            int reasonCount = in.readVInt();
            for (int i = 0; i < reasonCount; i++) {
                in.readVInt();
            }
            String readLogical = in.readString();
            String readPhysical = in.readString();
            FragmentPlan readPlan = in.readOptionalWriteable(FragmentPlan::new);
            String readUnplanned = in.readOptionalString();
            List<FragmentPlanRefiner.Reason> readRefinements = in.readList(input -> input.readEnum(FragmentPlanRefiner.Reason.class));
            Traits readTraits = Traits.read(in);
            if (routeOrdinal == 0) {
                this.route = Route.FRAGMENT;
                this.logical = readLogical;
                this.physical = readPhysical;
                this.fragmentPlan = readPlan;
                this.unplanned = readUnplanned;
                this.refinementsPossible = inReasonOrder(readRefinements);
                this.traits = readTraits;
            } else {
                this.route = Route.UNSUPPORTED;
                this.logical = null;
                this.physical = null;
                this.fragmentPlan = null;
                this.unplanned = SHARD_PATH_RETIRED;
                this.refinementsPossible = List.of();
                this.traits = null;
            }
        } else {
            this.route = in.readEnum(Route.class);
            this.logical = in.readOptionalString();
            this.physical = in.readOptionalString();
            this.fragmentPlan = in.readOptionalWriteable(FragmentPlan::new);
            this.unplanned = in.readOptionalString();
            this.refinementsPossible = inReasonOrder(in.readList(input -> input.readEnum(FragmentPlanRefiner.Reason.class)));
            this.traits = in.readBoolean() ? Traits.read(in) : null;
        }
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        WireVersion.write(out, WIRE_VERSION);
        out.writeString(index);
        out.writeEnum(route);
        out.writeOptionalString(logical);
        out.writeOptionalString(physical);
        out.writeOptionalWriteable(fragmentPlan);
        out.writeOptionalString(unplanned);
        out.writeCollection(refinementsPossible, StreamOutput::writeEnum);
        out.writeBoolean(traits != null);
        if (traits != null) {
            traits.writeTo(out);
        }
    }

    public String index() {
        return index;
    }

    public Route route() {
        return route;
    }

    /** The logical tree the translator built; null on the unsupported route. */
    public String logical() {
        return logical;
    }

    /** The physical tree the planner chose; null on the unsupported route. */
    public String physical() {
        return physical;
    }

    /** The plan the coordinator ships; null on the unsupported route and when the plan failed. */
    public FragmentPlan fragmentPlan() {
        return fragmentPlan;
    }

    /**
     * The element the translator refused, the {@code plan_failed}
     * message, or the refusal message on the unsupported route; null
     * when everything translated.
     */
    public String unplanned() {
        return unplanned;
    }

    /** The predicted node local downgrades in reason order; empty when none applies or on the unsupported route. */
    public List<FragmentPlanRefiner.Reason> refinementsPossible() {
        return refinementsPossible;
    }

    /** The trait side of the plan; null on the unsupported route, where nothing was planned. */
    public Traits traits() {
        return traits;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("route", route.jsonName());
        if (logical != null) {
            builder.field("logical", logical);
        }
        if (physical != null) {
            builder.field("physical", physical);
        }
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
        if (traits != null) {
            builder.field("traits");
            traits.toXContent(builder);
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
            && Objects.equals(logical, other.logical)
            && Objects.equals(physical, other.physical)
            && Objects.equals(fragmentPlan, other.fragmentPlan)
            && Objects.equals(unplanned, other.unplanned)
            && refinementsPossible.equals(other.refinementsPossible)
            && Objects.equals(traits, other.traits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, route, logical, physical, fragmentPlan, unplanned, refinementsPossible, traits);
    }
}

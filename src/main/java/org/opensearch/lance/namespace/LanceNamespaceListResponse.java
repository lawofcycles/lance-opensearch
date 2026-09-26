/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for {@link LanceNamespaceListAction}. Carries one of three
 * shapes, mirrored by {@link #toXContent}:
 * <ul>
 *   <li>{@code {"namespaces": [{...}, ...]}} when the request had no
 *       identifier: one object per registration with its name, type,
 *       path (directory only), redacted config, and an availability
 *       status ({@code unavailable} entries carry the initialise or
 *       poll error, {@code partial} entries the subnamespace listing
 *       failure of the last poll).</li>
 *   <li>{@code {"name": "...", "tables": [...]}} when the identifier
 *       names a registration.</li>
 *   <li>{@code {"registered": false, "name": "..."}} with status 404
 *       when it does not, so "unknown namespace" and "registered but
 *       empty" stay distinguishable.</li>
 * </ul>
 */
public final class LanceNamespaceListResponse extends ActionResponse implements StatusToXContentObject {

    /**
     * One registration in the root listing. {@code config} arrives
     * already redacted — the transport response never carries raw
     * secret values. {@code error} is {@code null} while the catalog
     * handle is healthy; {@code partial} is {@code null} unless the
     * last poll listed the catalog but could not descend into one of
     * its subnamespaces, and then carries that failure.
     */
    public static final class NamespaceInfo implements Writeable {

        private final String name;
        private final String type;
        private final String path;
        private final Map<String, String> config;
        private final String error;
        private final String partial;

        public NamespaceInfo(String name, String type, String path, Map<String, String> config, String error) {
            this(name, type, path, config, error, null);
        }

        public NamespaceInfo(String name, String type, String path, Map<String, String> config, String error, String partial) {
            this.name = Objects.requireNonNull(name, "name");
            this.type = Objects.requireNonNull(type, "type");
            this.path = path;
            this.config = Map.copyOf(config);
            this.error = error;
            this.partial = partial;
        }

        public NamespaceInfo(StreamInput in) throws IOException {
            this.name = in.readString();
            this.type = in.readString();
            this.path = in.readOptionalString();
            this.config = in.readMap(StreamInput::readString, StreamInput::readString);
            this.error = in.readOptionalString();
            this.partial = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(type);
            out.writeOptionalString(path);
            out.writeMap(config, StreamOutput::writeString, StreamOutput::writeString);
            out.writeOptionalString(error);
            out.writeOptionalString(partial);
        }

        public String name() {
            return name;
        }

        public String type() {
            return type;
        }

        public String path() {
            return path;
        }

        public Map<String, String> config() {
            return config;
        }

        public String error() {
            return error;
        }

        /** The subnamespace listing failure of the last poll, or {@code null} when the walk reached every namespace. */
        public String partial() {
            return partial;
        }

        XContentBuilder toXContent(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("name", name);
            builder.field("type", type);
            if (path != null) {
                builder.field("path", path);
            }
            builder.field("config", config);
            if (error != null) {
                builder.field("status", "unavailable");
                builder.field("error", error);
            } else if (partial != null) {
                builder.field("status", "partial");
                builder.field("error", partial);
            } else {
                builder.field("status", "available");
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof NamespaceInfo other)) return false;
            return name.equals(other.name)
                && type.equals(other.type)
                && Objects.equals(path, other.path)
                && config.equals(other.config)
                && Objects.equals(error, other.error)
                && Objects.equals(partial, other.partial);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, path, config, error, partial);
        }
    }

    private final List<NamespaceInfo> namespaces;
    private final String name;
    private final boolean registered;
    private final List<String> tables;

    public static LanceNamespaceListResponse namespaces(List<NamespaceInfo> namespaces) {
        return new LanceNamespaceListResponse(List.copyOf(namespaces), null, false, null);
    }

    public static LanceNamespaceListResponse tables(String name, List<String> tables) {
        return new LanceNamespaceListResponse(null, name, true, List.copyOf(tables));
    }

    public static LanceNamespaceListResponse unregistered(String name) {
        return new LanceNamespaceListResponse(null, name, false, null);
    }

    private LanceNamespaceListResponse(List<NamespaceInfo> namespaces, String name, boolean registered, List<String> tables) {
        this.namespaces = namespaces;
        this.name = name;
        this.registered = registered;
        this.tables = tables;
    }

    public LanceNamespaceListResponse(StreamInput in) throws IOException {
        super(in);
        this.namespaces = in.readBoolean() ? in.readList(NamespaceInfo::new) : null;
        this.name = in.readOptionalString();
        this.registered = in.readBoolean();
        this.tables = in.readOptionalStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(namespaces != null);
        if (namespaces != null) {
            out.writeList(namespaces);
        }
        out.writeOptionalString(name);
        out.writeBoolean(registered);
        out.writeOptionalStringCollection(tables);
    }

    /** Registered namespaces, or {@code null} when the request carried an identifier. */
    public List<NamespaceInfo> namespaces() {
        return namespaces;
    }

    /** The identifier the request asked about, or {@code null} for a root listing. */
    public String name() {
        return name;
    }

    /** True when {@link #name()} names a registered namespace. */
    public boolean registered() {
        return registered;
    }

    /** Sorted table names under {@link #name()}, or {@code null} when not registered. */
    public List<String> tables() {
        return tables;
    }

    @Override
    public RestStatus status() {
        return name != null && !registered ? RestStatus.NOT_FOUND : RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (name == null) {
            builder.startArray("namespaces");
            for (NamespaceInfo info : namespaces) {
                info.toXContent(builder);
            }
            builder.endArray();
        } else if (!registered) {
            builder.field("registered", false).field("name", name);
        } else {
            builder.field("name", name).field("tables", tables);
        }
        return builder.endObject();
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.ack.AckedRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.WireVersion;

/**
 * Transport request the plugin routes to the cluster manager to add
 * or remove an entry in {@link LanceNamespaceMetadata}. Wraps the
 * two mutations in a single request class so a shared
 * {@link org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction}
 * can serve them without duplicating the routing plumbing. Opens with
 * {@link #WIRE_VERSION} (see {@link WireVersion}).
 *
 * <p>The two REST body shapes that lead here are declared as parsers on
 * this class. {@link #REGISTER_PARSER} reads the body of
 * {@code POST /_plugins/_lance/namespace} ({@code path}, {@code type},
 * {@code name}, {@code config}, {@code storage_options}, {@code overrides})
 * into a {@link Register}; {@link #IDENTIFIER_PARSER} reads the body of
 * {@code DELETE /_plugins/_lance/namespace} and of
 * {@code POST /_plugins/_lance/namespace/tables} ({@code path},
 * {@code name}) into an {@link Identifier}. A field outside the declared
 * set is an {@link org.opensearch.core.xcontent.XContentParseException}
 * naming it, which the REST layer answers with 400.
 */
public final class LanceNamespaceUpdateRequest extends ClusterManagerNodeRequest<LanceNamespaceUpdateRequest> implements AckedRequest {

    /** The wire format's version, the first field the request writes after its base class. */
    public static final int WIRE_VERSION = 1;

    /** The name both parsers report in their messages, the same as the REST handler's. */
    public static final String PARSER_NAME = "lance_namespace";

    private static final ParseField PATH = new ParseField("path");
    private static final ParseField NAME = new ParseField("name");

    /** The body of {@code POST /_plugins/_lance/namespace}. */
    public static final ObjectParser<Register, Void> REGISTER_PARSER = new ObjectParser<>(PARSER_NAME, Register::new);

    /** The body of {@code DELETE /_plugins/_lance/namespace} and {@code POST /_plugins/_lance/namespace/tables}. */
    public static final ObjectParser<Identifier, Void> IDENTIFIER_PARSER = new ObjectParser<>(PARSER_NAME, Identifier::new);

    static {
        REGISTER_PARSER.declareString(Register::path, PATH);
        REGISTER_PARSER.declareString(Register::type, new ParseField("type"));
        REGISTER_PARSER.declareString(Register::name, NAME);
        REGISTER_PARSER.declareObject(Register::config, (p, c) -> p.map(), new ParseField("config"));
        REGISTER_PARSER.declareObject(Register::storageOptions, (p, c) -> p.map(), new ParseField("storage_options"));
        REGISTER_PARSER.declareObject(Register::overrides, (p, c) -> p.map(), new ParseField("overrides"));

        IDENTIFIER_PARSER.declareString(Identifier::path, PATH);
        IDENTIFIER_PARSER.declareString(Identifier::name, NAME);
    }

    /**
     * The fields of a register body as {@link #REGISTER_PARSER} reads
     * them. {@link #build} fills the defaults ({@code type} is
     * {@code directory} when absent, a directory registration is named
     * after its path), parses the object valued fields and throws
     * {@link IllegalArgumentException} with the message the operator sees
     * as a 400 for every rule the body breaks: an empty string field, a
     * type outside {@link LanceNamespaceMetadata.Entry#ACCEPTED_TYPES},
     * a missing {@code path} for a directory registration, a {@code path}
     * on a catalog registration, a missing {@code name} for a catalog
     * registration, and the config keys each catalog type needs
     * ({@code uri} for rest, {@code endpoint} and {@code warehouse} for
     * iceberg and polaris, {@code endpoint} and {@code catalog} for unity).
     */
    public static final class Register {

        private String path;
        private String type;
        private String name;
        private Map<String, Object> config;
        private Map<String, Object> storageOptions;
        private Map<String, Object> overrides;

        public Register() {}

        private void path(String path) {
            this.path = path;
        }

        private void type(String type) {
            this.type = type;
        }

        private void name(String name) {
            this.name = name;
        }

        private void config(Map<String, Object> config) {
            this.config = config;
        }

        private void storageOptions(Map<String, Object> storageOptions) {
            this.storageOptions = storageOptions;
        }

        private void overrides(Map<String, Object> overrides) {
            this.overrides = overrides;
        }

        public LanceNamespaceUpdateRequest build() {
            String registeredPath = nonEmpty(path, "path");
            String registeredType = nonEmpty(type, "type");
            if (registeredType == null) {
                registeredType = LanceNamespaceMetadata.Entry.TYPE_DIRECTORY;
            } else if (!LanceNamespaceMetadata.Entry.ACCEPTED_TYPES.contains(registeredType)) {
                throw new IllegalArgumentException(
                    "unknown namespace type [" + registeredType + "]; accepted values are " + LanceNamespaceMetadata.Entry.ACCEPTED_TYPES
                );
            }
            String registeredName = nonEmpty(name, "name");
            Map<String, String> parsedConfig = parseConfig(config);
            StorageOptions parsedStorageOptions = StorageOptions.parseFromRequestField(storageOptions, "[" + PARSER_NAME + "]");
            String overridesJson = LanceOverrides.parseAttachClauses(overrides, null).toJson();
            if (LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(registeredType)) {
                if (registeredPath == null) {
                    throw new IllegalArgumentException("[path] is required");
                }
                if (registeredName == null) {
                    registeredName = registeredPath;
                }
            } else {
                if (registeredPath != null) {
                    throw new IllegalArgumentException(
                        "[path] is only accepted for type [directory]; a [" + registeredType + "] namespace is rooted by its config"
                    );
                }
                if (registeredName == null) {
                    throw new IllegalArgumentException("[name] is required for type [" + registeredType + "]");
                }
                if (LanceNamespaceMetadata.Entry.TYPE_REST.equals(registeredType) && isBlank(parsedConfig.get("uri"))) {
                    throw new IllegalArgumentException("[config.uri] is required for type [rest]");
                }
                boolean icebergProtocol = LanceNamespaceMetadata.Entry.TYPE_ICEBERG.equals(registeredType)
                    || LanceNamespaceMetadata.Entry.TYPE_POLARIS.equals(registeredType);
                if (icebergProtocol || LanceNamespaceMetadata.Entry.TYPE_UNITY.equals(registeredType)) {
                    if (isBlank(parsedConfig.get("endpoint"))) {
                        throw new IllegalArgumentException("[config.endpoint] is required for type [" + registeredType + "]");
                    }
                }
                if (icebergProtocol && isBlank(parsedConfig.get("warehouse"))) {
                    // Without a warehouse the client rejects every listing
                    // (the warehouse / catalog is the first level of each
                    // table id), so the registration could never surface
                    // a table. Refuse it up front with the reason.
                    throw new IllegalArgumentException(
                        "[config.warehouse] is required for type ["
                            + registeredType
                            + "]; it names the warehouse (catalog) whose namespaces are polled"
                    );
                }
                if (LanceNamespaceMetadata.Entry.TYPE_UNITY.equals(registeredType) && isBlank(parsedConfig.get("catalog"))) {
                    throw new IllegalArgumentException("[config.catalog] is required for type [unity]");
                }
            }
            return register(registeredName, registeredType, registeredPath, parsedStorageOptions, parsedConfig, overridesJson);
        }
    }

    /**
     * The fields of a body that only identifies a registration, as
     * {@link #IDENTIFIER_PARSER} reads them. {@link #resolve} returns
     * {@code name} when given and {@code path} otherwise, and throws
     * {@link IllegalArgumentException} when neither is given or either is
     * empty.
     */
    public static final class Identifier {

        private String path;
        private String name;

        public Identifier() {}

        private void path(String path) {
            this.path = path;
        }

        private void name(String name) {
            this.name = name;
        }

        /** The registration identifier: {@code name} when given, {@code path} otherwise. */
        public String resolve() {
            String resolvedPath = nonEmpty(path, "path");
            String resolvedName = nonEmpty(name, "name");
            if (resolvedName != null) {
                return resolvedName;
            }
            if (resolvedPath != null) {
                return resolvedPath;
            }
            throw new IllegalArgumentException("[name] (or [path] for a directory registration) is required");
        }
    }

    /** {@code value} when it is {@code null} or non empty; an empty string is refused naming the field. */
    private static String nonEmpty(String value, String field) {
        if (value != null && value.isEmpty()) {
            throw new IllegalArgumentException("[" + field + "] must not be empty");
        }
        return value;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isEmpty();
    }

    /**
     * Parse the {@code config} object into a string-to-string map.
     * Keys the plugin does not know pass through to the catalog
     * implementation untouched; only the shape and the bounds of
     * {@link StorageOptions#MAX_ENTRIES}, {@link StorageOptions#MAX_KEY_BYTES}
     * and {@link StorageOptions#MAX_VALUE_BYTES} are validated here.
     */
    static Map<String, String> parseConfig(Object raw) {
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Map<?, ?> rawMap)) {
            throw new IllegalArgumentException("[config] must be a JSON object of string values");
        }
        StorageOptions.checkEntryCount(rawMap.size(), "[" + PARSER_NAME + "]", "config");
        Map<String, String> parsed = new LinkedHashMap<>(rawMap.size());
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            if (!(entry.getKey() instanceof String key) || key.isEmpty()) {
                throw new IllegalArgumentException("[config] keys must be non-empty strings");
            }
            if (!(entry.getValue() instanceof String value)) {
                throw new IllegalArgumentException(
                    "[config." + key + "] must be a string (nested objects / arrays / numbers / booleans are not accepted)"
                );
            }
            StorageOptions.checkEntryBytes(key, value, "[" + PARSER_NAME + "]", "config");
            parsed.put(key, value);
        }
        return parsed;
    }

    /** Which of the two supported mutations the manager should perform. */
    public enum Operation {
        REGISTER,
        UNREGISTER
    }

    private final Operation operation;
    private final String name;
    private final String type;
    private final String rootUri;
    private final StorageOptions storageOptions;
    private final Map<String, String> config;
    /** Canonical JSON of the per-column mapping overrides, empty when none were declared. */
    private final String overridesJson;

    /**
     * Register a directory namespace at {@code rootUri}, named after
     * the root, with the given storage options and mapping overrides.
     */
    public static LanceNamespaceUpdateRequest register(String rootUri, StorageOptions storageOptions, String overridesJson) {
        return register(rootUri, LanceNamespaceMetadata.Entry.TYPE_DIRECTORY, rootUri, storageOptions, Map.of(), overridesJson);
    }

    /**
     * Register a namespace of any type. {@code rootUri} is the
     * directory root and null for catalog types whose root lives in
     * {@code config}; {@code config} carries the implementation's
     * initialize properties as-is, secrets included.
     */
    public static LanceNamespaceUpdateRequest register(
        String name,
        String type,
        String rootUri,
        StorageOptions storageOptions,
        Map<String, String> config,
        String overridesJson
    ) {
        return applyLongTimeout(
            new LanceNamespaceUpdateRequest(Operation.REGISTER, name, type, rootUri, storageOptions, config, overridesJson)
        );
    }

    /**
     * Remove the entry whose name is {@code identifier}, or, for
     * directory entries, whose root path is {@code identifier}.
     */
    public static LanceNamespaceUpdateRequest unregister(String identifier) {
        return applyLongTimeout(
            new LanceNamespaceUpdateRequest(Operation.UNREGISTER, identifier, null, null, StorageOptions.empty(), Map.of(), "")
        );
    }

    private static LanceNamespaceUpdateRequest applyLongTimeout(LanceNamespaceUpdateRequest request) {
        // Widen the master-node timeout to match the ack timeout so
        // the caller does not get a 30s ProcessClusterEventTimeoutException
        // when the manager is queued behind namespace-poll CreateIndex
        // updates. 90s aligns with the plugin's own await window.
        request.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(90));
        return request;
    }

    private LanceNamespaceUpdateRequest(
        Operation operation,
        String name,
        String type,
        String rootUri,
        StorageOptions storageOptions,
        Map<String, String> config,
        String overridesJson
    ) {
        this.operation = operation;
        this.name = name;
        this.type = type;
        this.rootUri = rootUri;
        this.storageOptions = storageOptions;
        this.config = config;
        this.overridesJson = overridesJson == null ? "" : overridesJson;
    }

    public LanceNamespaceUpdateRequest(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceNamespaceUpdateRequest", WIRE_VERSION);
        this.operation = Operation.values()[in.readVInt()];
        this.name = in.readString();
        this.type = in.readOptionalString();
        this.rootUri = in.readOptionalString();
        this.storageOptions = StorageOptions.readFromStream(in);
        this.config = in.readMap(StreamInput::readString, StreamInput::readString);
        this.overridesJson = in.readString();
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        out.writeVInt(operation.ordinal());
        out.writeString(name);
        out.writeOptionalString(type);
        out.writeOptionalString(rootUri);
        storageOptions.writeTo(out);
        out.writeMap(config, StreamOutput::writeString, StreamOutput::writeString);
        out.writeString(overridesJson);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException ex = null;
        if (name == null || name.isEmpty()) {
            ex = new ActionRequestValidationException();
            ex.addValidationError("name must not be empty");
        }
        if (operation == Operation.REGISTER) {
            if (type == null || !LanceNamespaceMetadata.Entry.ACCEPTED_TYPES.contains(type)) {
                if (ex == null) {
                    ex = new ActionRequestValidationException();
                }
                ex.addValidationError(
                    "unknown namespace type [" + type + "]; accepted values are " + LanceNamespaceMetadata.Entry.ACCEPTED_TYPES
                );
            }
            if (LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(type) && (rootUri == null || rootUri.isEmpty())) {
                if (ex == null) {
                    ex = new ActionRequestValidationException();
                }
                ex.addValidationError("a directory namespace requires a path");
            }
        }
        return ex;
    }

    public Operation operation() {
        return operation;
    }

    /** Registration name for register; the identifier (name or directory root) for unregister. */
    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public String rootUri() {
        return rootUri;
    }

    public StorageOptions storageOptions() {
        return storageOptions;
    }

    public Map<String, String> config() {
        return config;
    }

    /** Canonical overrides JSON, empty when the register call declared none. */
    public String overridesJson() {
        return overridesJson;
    }

    /** The metadata entry a register request describes. */
    public LanceNamespaceMetadata.Entry toEntry() {
        return new LanceNamespaceMetadata.Entry(name, type, rootUri, storageOptions, config, overridesJson);
    }

    @Override
    public TimeValue ackTimeout() {
        // Give followers up to 90 seconds to acknowledge the state
        // change. Namespace polls on the manager occasionally batch
        // several CreateIndex updates ahead of a register /
        // unregister, so the ack has to wait behind them. The
        // plugin's own await loops align to the same window.
        return TimeValue.timeValueSeconds(90);
    }
}

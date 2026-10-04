/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.common.network.InetAddresses;

/**
 * Node-level allowlist for the servers a catalog registration makes the
 * cluster connect to: the {@code uri} of a {@code rest} registration and
 * the {@code endpoint} of a {@code glue}, {@code iceberg}, {@code polaris}
 * or {@code unity} one.
 *
 * <p>Configured through the {@code plugins.lance.allowed_catalog_endpoints}
 * setting, a list of URI prefixes. With the list empty every endpoint is
 * accepted except one whose host is a loopback, link local or wildcard
 * address ({@code 127.0.0.0/8}, {@code ::1}, {@code localhost},
 * {@code 169.254.0.0/16}, {@code fe80::/10}, {@code 0.0.0.0}, {@code ::}),
 * so a registration cannot point the cluster manager at a node's own
 * services or at an instance metadata service. With the list set, an
 * endpoint is accepted when it matches one of the prefixes, and only
 * then; a loopback or link local host named by a prefix is accepted.
 *
 * <p>A prefix is an absolute URI with a scheme and a host, for example
 * {@code https://catalog.example.com/} or {@code https://*.internal/}.
 * The scheme and the host compare case insensitively. The host of a
 * prefix may start with {@code *.}, which stands for exactly one label
 * ({@code https://*.internal/} covers {@code https://a.internal/} and not
 * {@code https://a.b.internal/} or {@code https://internal/}). A prefix
 * without a port covers every port; one with a port covers that port
 * only, the port the scheme implies when the endpoint names none. The
 * endpoint's path, with {@code .} and {@code ..} segments resolved, has
 * to start with the prefix's path. The host is compared as written, no
 * name is resolved, so a hostname that resolves to a refused address is
 * not caught by the default rule and is for the allowlist to exclude.
 *
 * <p>The prefixes are parsed once, when the setting is read; a prefix
 * that is not an absolute URI with a host fails construction so the
 * operator sees the typo at node start rather than as a refused
 * registration later.
 */
public final class AllowedCatalogEndpoints {

    public static final String SETTING_KEY = "plugins.lance.allowed_catalog_endpoints";

    /**
     * The config keys whose value names the server a catalog client
     * connects to. Checked on every registration type except
     * {@code directory}, whatever keys the type requires.
     */
    static final List<String> ENDPOINT_KEYS = List.of("uri", "endpoint");

    private static final String WILDCARD_LABEL = "*.";
    private static final String WILDCARD_STAND_IN = "wildcard-stand-in.";

    private final List<Endpoint> prefixes;

    public AllowedCatalogEndpoints(List<String> configuredPrefixes) {
        List<String> source = configuredPrefixes == null ? List.of() : configuredPrefixes;
        List<Endpoint> parsed = new ArrayList<>(source.size());
        for (String prefix : source) {
            if (prefix == null || prefix.isEmpty()) {
                continue;
            }
            Endpoint endpoint = Endpoint.parse(prefix, true);
            if (endpoint == null) {
                throw new IllegalArgumentException(
                    SETTING_KEY
                        + " entry ["
                        + prefix
                        + "] is not an absolute URI with a scheme and a host, such as https://catalog.example.com/"
                );
            }
            parsed.add(endpoint);
        }
        this.prefixes = List.copyOf(parsed);
    }

    public boolean isEmpty() {
        return prefixes.isEmpty();
    }

    /**
     * The canonical forms of the configured prefixes, each ending in {@code /}.
     */
    public List<String> configuredPrefixes() {
        List<String> forms = new ArrayList<>(prefixes.size());
        for (Endpoint prefix : prefixes) {
            forms.add(prefix.display());
        }
        return List.copyOf(forms);
    }

    /**
     * Checks every endpoint key of a registration's config. Returns null
     * when the registration may be polled, or the message naming the
     * first endpoint that may not. A {@code directory} registration has
     * no catalog server and is never refused here.
     */
    public String refusal(LanceNamespaceMetadata.Entry entry) {
        if (LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(entry.type())) {
            return null;
        }
        Map<String, String> config = entry.config();
        for (String key : ENDPOINT_KEYS) {
            String value = config.get(key);
            if (value == null || value.isEmpty()) {
                continue;
            }
            String reason = refusal(key, value);
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }

    /**
     * Returns null when {@code value}, the endpoint under {@code config.<key>},
     * may be contacted, or the message saying why it may not. The message
     * repeats the endpoint without its user info and query, so a
     * credential written into the URI does not reach a response or a log.
     */
    String refusal(String key, String value) {
        Endpoint candidate = Endpoint.parse(value, false);
        if (candidate == null) {
            return "[lance_namespace] [config." + key + "] is not an absolute URI with a scheme and a host";
        }
        if (prefixes.isEmpty()) {
            if (candidate.isLocalOrLinkLocal()) {
                return "[lance_namespace] catalog endpoint ["
                    + candidate.display()
                    + "] (config."
                    + key
                    + ") is a link local or loopback address; it is refused unless "
                    + SETTING_KEY
                    + " names it";
            }
            return null;
        }
        for (Endpoint prefix : prefixes) {
            if (prefix.covers(candidate)) {
                return null;
            }
        }
        return "[lance_namespace] catalog endpoint [" + candidate.display() + "] (config." + key + ") is not under " + SETTING_KEY;
    }

    /**
     * One parsed endpoint or prefix: lower cased scheme and host, the
     * effective port ({@code -1} when none is written and the scheme has
     * no default), and the path with its segments resolved and a trailing
     * {@code /}.
     */
    private record Endpoint(String scheme, String host, boolean wildcardHost, int port, boolean explicitPort, String path) {

        /**
         * Parses a prefix ({@code allowWildcard}) or an endpoint. Returns
         * null for anything that is not an absolute URI with a host, or
         * whose path climbs above its root.
         */
        static Endpoint parse(String value, boolean allowWildcard) {
            String text = value;
            boolean wildcard = false;
            int schemeEnd = text.indexOf("://");
            if (allowWildcard && schemeEnd > 0 && text.startsWith(WILDCARD_LABEL, schemeEnd + 3)) {
                // java.net.URI refuses '*' in a host, so the wildcard label
                // is parsed under a stand-in and taken off the host below.
                wildcard = true;
                text = text.substring(0, schemeEnd + 3) + WILDCARD_STAND_IN + text.substring(schemeEnd + 3 + WILDCARD_LABEL.length());
            }
            URI uri;
            try {
                uri = new URI(text);
            } catch (URISyntaxException e) {
                return null;
            }
            if (uri.getScheme() == null || uri.getHost() == null || uri.getHost().isEmpty()) {
                return null;
            }
            String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
            String host = uri.getHost().toLowerCase(Locale.ROOT);
            if (wildcard) {
                host = host.substring(WILDCARD_STAND_IN.length());
                if (host.isEmpty()) {
                    return null;
                }
            }
            boolean explicitPort = uri.getPort() != -1;
            int port = explicitPort ? uri.getPort() : defaultPort(scheme);
            String path = resolveSegments(uri.getPath() == null ? "" : uri.getPath());
            if (path == null) {
                return null;
            }
            return new Endpoint(scheme, host, wildcard, port, explicitPort, path);
        }

        private static int defaultPort(String scheme) {
            return switch (scheme) {
                case "http" -> 80;
                case "https" -> 443;
                default -> -1;
            };
        }

        /**
         * Resolves {@code .} and {@code ..} in a decoded path and returns
         * it with a leading and a trailing {@code /}; null when a
         * {@code ..} climbs above the first segment.
         */
        private static String resolveSegments(String decodedPath) {
            String relative = decodedPath.startsWith("/") ? decodedPath.substring(1) : decodedPath;
            List<String> segments = new ArrayList<>();
            for (String segment : relative.split("/", -1)) {
                if (segment.isEmpty() || segment.equals(".")) {
                    continue;
                }
                if (segment.equals("..")) {
                    if (segments.isEmpty()) {
                        return null;
                    }
                    segments.remove(segments.size() - 1);
                    continue;
                }
                segments.add(segment);
            }
            StringBuilder path = new StringBuilder();
            for (String segment : segments) {
                path.append('/').append(segment);
            }
            path.append('/');
            return path.toString();
        }

        /** Whether this prefix covers {@code candidate}. */
        boolean covers(Endpoint candidate) {
            if (!scheme.equals(candidate.scheme)) {
                return false;
            }
            if (wildcardHost) {
                String suffix = "." + host;
                if (!candidate.host.endsWith(suffix)) {
                    return false;
                }
                String label = candidate.host.substring(0, candidate.host.length() - suffix.length());
                if (label.isEmpty() || label.indexOf('.') >= 0) {
                    return false;
                }
            } else if (!host.equals(candidate.host)) {
                return false;
            }
            if (explicitPort && port != candidate.port) {
                return false;
            }
            return candidate.path.startsWith(path);
        }

        /**
         * Whether the host is written as a loopback, link local or
         * wildcard address, or as {@code localhost}. Hostnames are not
         * resolved.
         */
        boolean isLocalOrLinkLocal() {
            if (host.equals("localhost")) {
                return true;
            }
            String literal = host;
            if (literal.startsWith("[") && literal.endsWith("]")) {
                literal = literal.substring(1, literal.length() - 1);
            }
            int zone = literal.indexOf('%');
            if (zone >= 0) {
                literal = literal.substring(0, zone);
            }
            if (!InetAddresses.isInetAddress(literal)) {
                return false;
            }
            InetAddress address = InetAddresses.forString(literal);
            return address.isLoopbackAddress() || address.isLinkLocalAddress() || address.isAnyLocalAddress();
        }

        /** The endpoint without user info, query or fragment, for messages. */
        String display() {
            StringBuilder form = new StringBuilder().append(scheme).append("://");
            if (wildcardHost) {
                form.append(WILDCARD_LABEL);
            }
            form.append(host);
            if (explicitPort) {
                form.append(':').append(port);
            }
            return form.append(path).toString();
        }
    }
}

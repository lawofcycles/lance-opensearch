/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Node-level allowlist for the paths / URIs {@code attach} and
 * {@code namespace} accept as Lance table roots.
 *
 * <p>Configured through the {@code plugins.lance.allowed_table_roots} setting, which
 * takes a list of prefixes. A candidate is accepted when its canonical
 * form starts with the canonical form of any configured root. An empty
 * list disables the check so operators can upgrade an existing deployment
 * without immediately having to enumerate every table location; the
 * recommendation in the README is to set the list explicitly in production.
 *
 * <p>Both sides go through the same canonicalisation before the prefix
 * compare, so the comparison sees the location the storage layer will
 * open rather than the string as it arrived on the REST payload.
 * <ul>
 * <li>A URI with a scheme ({@code s3://}, {@code gs://}, {@code az://},
 * {@code file://}, ...) is parsed with {@link URI}; the scheme and the
 * authority (the bucket) are lower cased, the path is percent decoded and
 * its {@code .} and {@code ..} segments are resolved. A path whose
 * {@code ..} climbs above the bucket, and a string {@link URI} cannot
 * parse, are refused.</li>
 * <li>A local path is made absolute and its {@code .} and {@code ..}
 * segments are resolved; the deepest ancestor that exists on this node is
 * then replaced by its real path so a symlink under a root cannot point
 * outside it. A path that does not exist yet is not refused for that
 * reason alone (an attach may name a table that is being written).</li>
 * </ul>
 * Every canonical form ends with {@code /} so {@code /data/lance} does not
 * match {@code /data/lance-old}.
 *
 * <p>The roots are canonicalised once, when the setting is read; a root
 * that cannot be canonicalised fails construction so the operator sees
 * the typo at node start rather than as a refused attach later.
 */
public final class AllowedTableRoots {

    private static final Logger LOG = LogManager.getLogger(AllowedTableRoots.class);

    private static final Pattern SCHEME_PREFIX = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]*://.*", Pattern.DOTALL);

    private final List<String> canonicalRoots;

    public AllowedTableRoots(List<String> configuredRoots) {
        List<String> source = configuredRoots == null ? List.of() : configuredRoots;
        List<String> canonical = new ArrayList<>(source.size());
        for (String root : source) {
            if (root == null || root.isEmpty()) {
                continue;
            }
            String form = canonicalise(root);
            if (form == null) {
                throw new IllegalArgumentException("plugins.lance.allowed_table_roots entry [" + root + "] is not a valid path or URI");
            }
            canonical.add(form);
        }
        this.canonicalRoots = List.copyOf(canonical);
    }

    public boolean isEmpty() {
        return canonicalRoots.isEmpty();
    }

    /**
     * Returns true when the candidate's canonical form is inside one of
     * the configured roots, or when the allowlist is empty. Null, empty and
     * unparseable candidates are refused when the list is non empty.
     */
    public boolean allows(String candidate) {
        if (canonicalRoots.isEmpty()) {
            return true;
        }
        if (candidate == null || candidate.isEmpty()) {
            return false;
        }
        String form = canonicalise(candidate);
        if (form == null) {
            LOG.debug("table [{}] has no canonical form; refused by plugins.lance.allowed_table_roots", candidate);
            return false;
        }
        for (String root : canonicalRoots) {
            if (form.startsWith(root)) {
                return true;
            }
        }
        LOG.debug("table [{}] canonicalises to [{}], outside plugins.lance.allowed_table_roots {}", candidate, form, canonicalRoots);
        return false;
    }

    /**
     * The canonical forms of the configured roots, each ending in {@code /}.
     */
    public List<String> configuredRoots() {
        return canonicalRoots;
    }

    /**
     * Returns the canonical form of a root or candidate, ending in
     * {@code /}, or null when the value cannot be canonicalised.
     */
    static String canonicalise(String value) {
        return SCHEME_PREFIX.matcher(value).matches() ? canonicaliseUri(value) : canonicaliseLocalPath(value);
    }

    private static String canonicaliseUri(String value) {
        URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException e) {
            return null;
        }
        if (uri.getScheme() == null || uri.getRawPath() == null) {
            return null;
        }
        String path = resolveSegments(uri.getPath());
        if (path == null) {
            return null;
        }
        String authority = uri.getAuthority();
        StringBuilder form = new StringBuilder();
        form.append(uri.getScheme().toLowerCase(Locale.ROOT)).append("://");
        if (authority != null) {
            form.append(authority.toLowerCase(Locale.ROOT));
        }
        form.append(path);
        return form.toString();
    }

    /**
     * Resolves {@code .} and {@code ..} in an already percent decoded URI
     * path. Returns the path with a leading and a trailing {@code /}, or
     * null when a {@code ..} climbs above the first segment. Interior
     * empty segments are kept, because object stores treat {@code a//b}
     * and {@code a/b} as different keys.
     */
    private static String resolveSegments(String decodedPath) {
        String relative = decodedPath.startsWith("/") ? decodedPath.substring(1) : decodedPath;
        List<String> segments = new ArrayList<>();
        for (String segment : relative.split("/", -1)) {
            if (segment.equals(".")) {
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
        int last = segments.size() - 1;
        while (last >= 0 && segments.get(last).isEmpty()) {
            last--;
        }
        StringBuilder path = new StringBuilder();
        for (int i = 0; i <= last; i++) {
            path.append('/').append(segments.get(i));
        }
        path.append('/');
        return path.toString();
    }

    private static String canonicaliseLocalPath(String value) {
        Path absolute;
        try {
            absolute = Path.of(value).toAbsolutePath().normalize();
        } catch (InvalidPathException | SecurityException e) {
            return null;
        }
        Path real = resolveExistingAncestor(absolute);
        String form = real.toString();
        return form.endsWith("/") ? form : form + "/";
    }

    /**
     * Replaces the deepest existing ancestor of {@code absolute} by its
     * real path (symlinks resolved) and re-appends the remaining names
     * unchanged. A path that exists in full is returned as its real path.
     */
    private static Path resolveExistingAncestor(Path absolute) {
        Deque<Path> tail = new ArrayDeque<>();
        Path probe = absolute;
        while (probe != null) {
            try {
                Path real = probe.toRealPath();
                for (Path name : tail) {
                    real = real.resolve(name);
                }
                return real;
            } catch (IOException | SecurityException e) {
                Path name = probe.getFileName();
                if (name == null) {
                    break;
                }
                tail.addFirst(name);
                probe = probe.getParent();
            }
        }
        return absolute;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.lance.ReadOptions;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;

/**
 * Immutable wrapper around a Lance object-store storage options map, the
 * same shape Lance's Rust {@code object_store} accepts via
 * {@link ReadOptions.Builder#setStorageOptions(Map)}. The plugin surface
 * uses the Lance-native key names (e.g. {@code aws_access_key_id},
 * {@code aws_region}, {@code aws_endpoint}, {@code allow_http}) so what
 * the caller writes on {@code POST /_plugins/_lance/attach} is what Lance's
 * object store sees; there is no OpenSearch-side translation.
 *
 * <p>The value type carries only {@link String} keys and values. Nested
 * objects, arrays, and non-string primitives are rejected at parse time
 * so bad payloads surface as 400 instead of reaching Lance with a
 * corrupted map.
 */
public final class StorageOptions {

    /** Index-settings prefix under which entries are persisted. */
    public static final String INDEX_SETTING_PREFIX = "index.plugins.lance.storage_options.";

    /**
     * Index-settings prefix the first preview releases persisted the
     * entries under. Still read, through the fallback of the group setting
     * registered in {@code LancePlugin}, so an index created under it keeps
     * its options; never written.
     */
    public static final String DEPRECATED_INDEX_SETTING_PREFIX = "index.lance.storage_options.";

    /**
     * The most entries a {@code storage_options} map or a namespace
     * {@code config} map accepts. Both maps are persisted in the cluster
     * state (index settings for an attach, the {@code lance.namespaces}
     * custom for a namespace) and travel to every node with every state
     * update, so a request cannot be allowed to make them arbitrarily
     * large; the bound is a guard against a pasted file or a secret
     * manager answering a blob, not a privilege boundary. 64 is well
     * above the dozen or so keys the object stores and catalog clients
     * read.
     */
    public static final int MAX_ENTRIES = 64;

    /** The longest key, in UTF 8 bytes, a {@code storage_options} or namespace {@code config} map accepts. */
    public static final int MAX_KEY_BYTES = 256;

    /**
     * The longest value, in UTF 8 bytes, a {@code storage_options} or
     * namespace {@code config} map accepts. 4 KiB holds every documented
     * AWS session token, GCS service account entry, Azure SAS token and
     * REST catalog header.
     */
    public static final int MAX_VALUE_BYTES = 4096;

    /**
     * The words that mark a storage option or namespace config key as a
     * credential, matched case-insensitively as substrings of the key
     * name. {@code authorization} covers the {@code header.Authorization}
     * property the REST catalog client reads its bearer credential from;
     * {@code credential} covers the Iceberg REST client's
     * {@code credential} property (an OAuth client id and secret pair),
     * which none of the other substrings match. {@link #isSensitiveKey}
     * and {@link #SENSITIVE_INDEX_SETTING_PATTERNS} are both derived from
     * this one list so the redaction in logs and listings and the filter
     * on the settings APIs cannot drift apart.
     */
    public static final List<String> SENSITIVE_KEY_WORDS = List.of("secret", "password", "token", "key", "authorization", "credential");

    /**
     * Glob patterns for OpenSearch's {@code SettingsFilter} that remove
     * the credential entries of {@link #INDEX_SETTING_PREFIX} and of
     * {@link #DEPRECATED_INDEX_SETTING_PREFIX} from the settings and
     * cluster state APIs. Three patterns per prefix and word of
     * {@link #SENSITIVE_KEY_WORDS}: lower case ({@code aws_secret_access_key}),
     * upper case ({@code AWS_SECRET_ACCESS_KEY}) and capitalised
     * ({@code header.Authorization}). The filter matches
     * case-sensitively, so a key that spells the word in another mix of
     * cases ({@code aws_SeCrEt_access_key}) is not filtered; Lance's
     * object store does not recognise such a key either.
     */
    public static final List<String> SENSITIVE_INDEX_SETTING_PATTERNS = sensitiveIndexSettingPatterns();

    private static List<String> sensitiveIndexSettingPatterns() {
        List<String> patterns = new ArrayList<>(SENSITIVE_KEY_WORDS.size() * 6);
        for (String prefix : List.of(INDEX_SETTING_PREFIX, DEPRECATED_INDEX_SETTING_PREFIX)) {
            for (String word : SENSITIVE_KEY_WORDS) {
                patterns.add(prefix + "*" + word + "*");
                patterns.add(prefix + "*" + word.toUpperCase(Locale.ROOT) + "*");
                patterns.add(prefix + "*" + word.substring(0, 1).toUpperCase(Locale.ROOT) + word.substring(1) + "*");
            }
        }
        return Collections.unmodifiableList(patterns);
    }

    /**
     * True for keys whose value must never appear in logs, listings,
     * {@code toString} or the settings APIs: any key whose name contains
     * one of {@link #SENSITIVE_KEY_WORDS}, case-insensitively.
     */
    public static boolean isSensitiveKey(String key) {
        String lower = key.toLowerCase(Locale.ROOT);
        for (String word : SENSITIVE_KEY_WORDS) {
            if (lower.contains(word)) {
                return true;
            }
        }
        return false;
    }

    /** What {@link #redactCredentials(String)} puts in place of a credential shaped value. */
    public static final String REDACTED = "***";

    /**
     * An AWS access key id: the {@code AKIA} (long lived) or {@code ASIA}
     * (temporary) prefix and 16 upper case alphanumerics, not embedded
     * in a longer run of the same alphabet. S3 echoes it in the
     * {@code InvalidAccessKeyId} error body and it also appears in
     * {@code X-Amz-Credential} of a presigned URL and in the
     * {@code Credential=} part of a SigV4 {@code Authorization} header.
     */
    private static final Pattern ACCESS_KEY_ID = Pattern.compile("(?<![0-9A-Z])(?:AKIA|ASIA)[0-9A-Z]{16}(?![0-9A-Z])");

    /**
     * The {@code <AWSAccessKeyId>} element of an S3 error body (and the
     * {@code <AccessKeyId>} element STS answers with), whatever its
     * value; MinIO and other S3 compatible stores echo key ids that
     * are not in the AWS format.
     */
    private static final Pattern XML_ACCESS_KEY_ID = Pattern.compile("(<(?:AWS)?AccessKeyId>)[^<]*(</(?:AWS)?AccessKeyId>)");

    /**
     * The {@code <StringToSign>}, {@code <SignatureProvided>},
     * {@code <CanonicalRequest>}, {@code <StringToSignBytes>} and
     * {@code <CanonicalRequestBytes>} elements of the S3
     * {@code SignatureDoesNotMatch} error body, whatever their content.
     * {@code SignatureProvided} is the HMAC the client derived from
     * the secret access key, {@code StringToSign} and
     * {@code CanonicalRequest} are the inputs it was computed over,
     * which carry the signed headers, the session token and the key
     * id's credential scope, and the two {@code Bytes} elements are
     * the same inputs as a space separated hex dump, from which the
     * text decodes byte for byte. {@code StringToSign} and
     * {@code CanonicalRequest} are newline separated and span lines,
     * so the match runs in DOTALL mode and stops at the element's own
     * closing tag. An opening tag with no closing tag anywhere after
     * it (a body cut short by a message length limit, or a body that
     * moves on to another element) is redacted from the opening tag
     * to the next {@code <} or to the end of the message, so a
     * truncated body does not leak the part it kept. Group 3 is
     * absent in that case and the replacement adds nothing after the
     * placeholder. The element name in prose, with no angle brackets,
     * is not a match.
     */
    private static final Pattern XML_SIGNING_ELEMENTS = Pattern.compile(
        "(<(StringToSign|SignatureProvided|CanonicalRequest|StringToSignBytes|CanonicalRequestBytes)>)(?:.*?(</\\2>)|[^<]*)",
        Pattern.DOTALL
    );

    /**
     * Any element whose name ends in {@code Bytes}, the shape S3 gives
     * the hex dump of a signing input. The two such elements S3 sends
     * today are named in {@link #XML_SIGNING_ELEMENTS}; this family
     * match covers a hex element a later S3 body adds next to them, so
     * its content does not pass through until the name is listed. The
     * closing tag and truncation handling are those of
     * {@link #XML_SIGNING_ELEMENTS}.
     */
    private static final Pattern XML_HEX_DUMP_ELEMENTS = Pattern.compile("(<([A-Za-z]+Bytes)>)(?:.*?(</\\2>)|[^<]*)", Pattern.DOTALL);

    /**
     * A {@code Bearer} or {@code Basic} authorization value, the shapes
     * a catalog client puts in an {@code Authorization} header.
     */
    private static final Pattern AUTHORIZATION_VALUE = Pattern.compile("(?i)\\b((?:Bearer|Basic)\\s+)[A-Za-z0-9\\-._~+/]+=*");

    /** The {@code sig} parameter of an Azure SAS URL. */
    private static final Pattern SAS_SIGNATURE = Pattern.compile("([?&]sig=)[^&\\s]+");

    /**
     * A {@code key=value}, {@code key: value} or {@code "key": "value"}
     * pair. Group 1 is the key, group 2 the separator, group 3 the
     * value (quoted or up to the next delimiter). Whether the pair is
     * redacted is decided per match by {@link #hasSensitiveSegment} on
     * the key; the value alone says nothing.
     */
    private static final Pattern KEY_VALUE = Pattern.compile("([A-Za-z0-9_.\\-]+)(\"?\\s*[=:]\\s*)(\"[^\"]*\"|'[^']*'|[^\\s,;&)}\\]\"']+)");

    /**
     * Replace the credential shaped parts of {@code message} with
     * {@link #REDACTED}, for a message that is about to leave the node
     * in a client response or a log line. Covers the value of any
     * {@code key=value} / {@code key: value} / {@code "key": "value"}
     * pair whose key names a credential (see {@link #hasSensitiveSegment}),
     * an AWS access key id wherever it appears, the
     * {@code <AWSAccessKeyId>} element of an S3 error body, the
     * {@code <StringToSign>}, {@code <SignatureProvided>} and
     * {@code <CanonicalRequest>} elements of an S3
     * {@code SignatureDoesNotMatch} body together with their hex dumps
     * ({@code <StringToSignBytes>}, {@code <CanonicalRequestBytes>} and
     * any other element whose name ends in {@code Bytes}), a
     * {@code Bearer} or {@code Basic} authorization value, and the
     * {@code sig} parameter of an Azure SAS URL. Region, endpoint,
     * bucket and table path are left as they are. Returns
     * {@code message} itself (not a copy) when nothing matches, so a
     * caller can compare by identity; {@code null} in gives
     * {@code null} out.
     *
     * <p>The patterns follow what S3, GCS and Azure put in their error
     * bodies today and what a storage option or catalog config looks
     * like when a message quotes one. A new shape needs a new pattern;
     * the exception messages themselves are not changed, only what is
     * reported, so the unredacted text stays available for debugging
     * through the exception's own chain.
     */
    public static String redactCredentials(String message) {
        if (message == null || message.isEmpty()) {
            return message;
        }
        String out = message;
        out = XML_SIGNING_ELEMENTS.matcher(out).replaceAll("$1" + REDACTED + "$3");
        out = XML_HEX_DUMP_ELEMENTS.matcher(out).replaceAll("$1" + REDACTED + "$3");
        out = XML_ACCESS_KEY_ID.matcher(out).replaceAll("$1" + REDACTED + "$2");
        out = ACCESS_KEY_ID.matcher(out).replaceAll(REDACTED);
        out = AUTHORIZATION_VALUE.matcher(out).replaceAll("$1" + REDACTED);
        out = SAS_SIGNATURE.matcher(out).replaceAll("$1" + REDACTED);
        out = redactKeyValuePairs(out);
        return out.equals(message) ? message : out;
    }

    private static String redactKeyValuePairs(String text) {
        Matcher matcher = KEY_VALUE.matcher(text);
        StringBuilder out = null;
        int last = 0;
        while (matcher.find()) {
            if (!hasSensitiveSegment(matcher.group(1))) {
                continue;
            }
            if (out == null) {
                out = new StringBuilder(text.length());
            }
            String value = matcher.group(3);
            char quote = value.charAt(0);
            String replacement = (quote == '"' || quote == '\'') && value.length() >= 2 && value.charAt(value.length() - 1) == quote
                ? quote + REDACTED + quote
                : REDACTED;
            out.append(text, last, matcher.start(3)).append(replacement);
            last = matcher.end(3);
        }
        if (out == null) {
            return text;
        }
        out.append(text, last, text.length());
        return out.toString();
    }

    /**
     * Whether a key quoted in free text names a credential: one of its
     * segments, split on {@code _}, {@code .}, {@code -} and camel case
     * humps, is one of {@link #SENSITIVE_KEY_WORDS} or its plural
     * ({@code aws_access_key_id}, {@code AWS_SECRET_ACCESS_KEY},
     * {@code header.Authorization}, {@code sessionToken},
     * {@code x-amz-security-token}, {@code credentials}). Stricter than
     * {@link #isSensitiveKey}, which matches the words as substrings:
     * in free text a substring match would take the value after
     * {@code tokenizer:} or {@code keyword:} for a credential, while a
     * storage option key is only ever compared whole.
     */
    static boolean hasSensitiveSegment(String key) {
        int start = 0;
        for (int i = 0; i <= key.length(); i++) {
            boolean end = i == key.length();
            char c = end ? 0 : key.charAt(i);
            boolean delimiter = !end && (c == '_' || c == '.' || c == '-');
            boolean hump = !end && i > start && Character.isUpperCase(c) && Character.isLowerCase(key.charAt(i - 1));
            if (!(end || delimiter || hump)) {
                continue;
            }
            if (i > start) {
                String segment = key.substring(start, i).toLowerCase(Locale.ROOT);
                for (String word : SENSITIVE_KEY_WORDS) {
                    if (segment.equals(word) || segment.equals(word + "s")) {
                        return true;
                    }
                }
            }
            start = delimiter ? i + 1 : i;
        }
        return false;
    }

    /** Bound on the cause chain walk of {@link #redactCredentials(Exception)}, in case a chain is cyclic. */
    private static final int MAX_CAUSE_DEPTH = 10;

    /**
     * The exception to hand to a client response or a log line for
     * {@code failure}: {@code failure} itself when no message in its
     * cause chain changes under {@link #redactCredentials(String)},
     * otherwise a copy whose messages are redacted. The copy keeps
     * what the REST layer and the log need from the original: the
     * HTTP status {@code ExceptionsHelper.status} assigns it, the
     * {@link IllegalArgumentException} class when the original (after
     * unwrapping a transport wrapper) is one, so a Lance invalid input
     * stays a 400 of the same type, the stack frames, and one cause per
     * cause of the original, each naming the original's class in its
     * redacted message. Everything else becomes an
     * {@link OpenSearchStatusException} of the same status. The
     * original is not modified.
     */
    public static Exception redactCredentials(Exception failure) {
        if (failure == null || !needsRedaction(failure)) {
            return failure;
        }
        return redactedCopy(failure);
    }

    /** {@link #redactCredentials(Exception)} for a {@link RuntimeException}; the copy is always one. */
    public static RuntimeException redactCredentials(RuntimeException failure) {
        if (failure == null || !needsRedaction(failure)) {
            return failure;
        }
        return redactedCopy(failure);
    }

    private static boolean needsRedaction(Throwable failure) {
        Throwable current = failure;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            String message = current.getMessage();
            if (message != null && redactCredentials(message) != message) {
                return true;
            }
            Throwable cause = current.getCause();
            current = cause == current ? null : cause;
        }
        return false;
    }

    private static RuntimeException redactedCopy(Exception failure) {
        RestStatus status = ExceptionsHelper.status(failure);
        Throwable unwrapped = ExceptionsHelper.unwrapCause(failure);
        String message = redactCredentials(unwrapped.getMessage() == null ? unwrapped.toString() : unwrapped.getMessage());
        Throwable cause = unwrapped.getCause() == unwrapped ? null : redactedCause(unwrapped.getCause(), 1);
        RuntimeException copy = unwrapped instanceof IllegalArgumentException
            ? new IllegalArgumentException(message, cause)
            : new OpenSearchStatusException(message, status, cause);
        copy.setStackTrace(unwrapped.getStackTrace());
        return copy;
    }

    private static Throwable redactedCause(Throwable original, int depth) {
        if (original == null || depth >= MAX_CAUSE_DEPTH) {
            return null;
        }
        Throwable next = original.getCause() == original ? null : original.getCause();
        return new RedactedCause(original, redactedCause(next, depth + 1));
    }

    /**
     * One link of the cause chain of a redacted copy: the original's
     * class name and redacted message, and the original's frames.
     */
    private static final class RedactedCause extends RuntimeException {
        RedactedCause(Throwable original, Throwable cause) {
            super(
                original.getMessage() == null
                    ? original.getClass().getName()
                    : original.getClass().getName() + ": " + redactCredentials(original.getMessage()),
                cause
            );
            setStackTrace(original.getStackTrace());
        }
    }

    private static final StorageOptions EMPTY = new StorageOptions(Collections.emptyMap());

    private final Map<String, String> options;

    private StorageOptions(Map<String, String> options) {
        // TreeMap so equals / hashCode / toString / persisted settings do
        // not depend on the caller's iteration order, and callers can
        // compare two StorageOptions without worrying about ordering.
        this.options = Collections.unmodifiableMap(new TreeMap<>(options));
    }

    public static StorageOptions empty() {
        return EMPTY;
    }

    /**
     * Wrap a caller-supplied map. Values are copied so later mutation of
     * the input does not leak into the returned {@code StorageOptions}.
     * Throws {@link IllegalArgumentException} on null keys or values.
     */
    public static StorageOptions of(Map<String, String> options) {
        if (options == null || options.isEmpty()) {
            return EMPTY;
        }
        Map<String, String> copy = new LinkedHashMap<>(options.size());
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("storage_options keys must be non-empty strings");
            }
            if (value == null) {
                throw new IllegalArgumentException("storage_options value for [" + key + "] must not be null");
            }
            copy.put(key, value);
        }
        return new StorageOptions(copy);
    }

    /**
     * Parse the {@code storage_options} field from a request body map.
     * Accepts a JSON object whose values are strings; rejects anything
     * else, and a map outside the bounds of {@link #MAX_ENTRIES},
     * {@link #MAX_KEY_BYTES} and {@link #MAX_VALUE_BYTES}. Returns
     * {@link #empty()} when the caller omits the field.
     */
    public static StorageOptions parseFromRequestField(Object rawValue, String errorPrefix) {
        if (rawValue == null) {
            return EMPTY;
        }
        if (!(rawValue instanceof Map<?, ?> rawMap)) {
            throw new IllegalArgumentException(errorPrefix + " [storage_options] must be a JSON object");
        }
        checkEntryCount(rawMap.size(), errorPrefix, "storage_options");
        Map<String, String> parsed = new LinkedHashMap<>(rawMap.size());
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            if (!(key instanceof String stringKey) || stringKey.isEmpty()) {
                throw new IllegalArgumentException(errorPrefix + " [storage_options] keys must be non-empty strings");
            }
            if (!(value instanceof String stringValue)) {
                throw new IllegalArgumentException(
                    errorPrefix
                        + " [storage_options."
                        + stringKey
                        + "] must be a string (nested objects / arrays / numbers / booleans are not accepted)"
                );
            }
            checkEntryBytes(stringKey, stringValue, errorPrefix, "storage_options");
            parsed.put(stringKey, stringValue);
        }
        return of(parsed);
    }

    /**
     * Refuse a {@code storage_options} or namespace {@code config} map of
     * more than {@link #MAX_ENTRIES} entries with an
     * {@link IllegalArgumentException} the REST layer answers with 400.
     * {@code errorPrefix} is the handler's tag ({@code [lance_attach]}),
     * {@code field} the name of the map in the request body.
     */
    public static void checkEntryCount(int count, String errorPrefix, String field) {
        if (count > MAX_ENTRIES) {
            throw new IllegalArgumentException(errorPrefix + " " + field + " has [" + count + "] entries, the limit is " + MAX_ENTRIES);
        }
    }

    /**
     * Refuse one entry of a {@code storage_options} or namespace
     * {@code config} map whose key is longer than {@link #MAX_KEY_BYTES}
     * or whose value is longer than {@link #MAX_VALUE_BYTES}, both in
     * UTF 8 bytes, with an {@link IllegalArgumentException} the REST
     * layer answers with 400. The message names the key and the length;
     * it never quotes the value, which may be a credential.
     */
    public static void checkEntryBytes(String key, String value, String errorPrefix, String field) {
        int keyBytes = key.getBytes(StandardCharsets.UTF_8).length;
        if (keyBytes > MAX_KEY_BYTES) {
            throw new IllegalArgumentException(
                errorPrefix + " " + field + " key [" + key + "] is [" + keyBytes + "] bytes, the limit is " + MAX_KEY_BYTES
            );
        }
        int valueBytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (valueBytes > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException(
                errorPrefix + " " + field + " value for [" + key + "] is [" + valueBytes + "] bytes, the limit is " + MAX_VALUE_BYTES
            );
        }
    }

    /**
     * Read the {@code index.plugins.lance.storage_options.*} group from
     * index settings back into a {@link StorageOptions}; an index created
     * under the deprecated {@code index.lance.storage_options.*} prefix is
     * read through the group setting's fallback. Returns {@link #empty()}
     * when the group is missing.
     */
    public static StorageOptions fromIndexSettings(Settings settings) {
        Settings group = LancePlugin.STORAGE_OPTIONS_SETTING.get(settings);
        Map<String, String> map = new LinkedHashMap<>();
        for (String key : group.keySet()) {
            map.put(key, group.get(key));
        }
        return of(map);
    }

    /**
     * Write the options into an OpenSearch settings builder under the
     * canonical {@link #INDEX_SETTING_PREFIX}. No-op when empty.
     */
    public void writeToSettings(Settings.Builder builder) {
        for (Map.Entry<String, String> entry : options.entrySet()) {
            builder.put(INDEX_SETTING_PREFIX + entry.getKey(), entry.getValue());
        }
    }

    public static StorageOptions readFromStream(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            return EMPTY;
        }
        Map<String, String> map = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            String value = in.readString();
            map.put(key, value);
        }
        return of(map);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(options.size());
        for (Map.Entry<String, String> entry : options.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
    }

    /** Returns an unmodifiable view of the underlying map. */
    public Map<String, String> asMap() {
        return options;
    }

    public boolean isEmpty() {
        return options.isEmpty();
    }

    /**
     * Build a {@link ReadOptions} instance carrying these options.
     * Returns {@code null} when {@link #isEmpty()} so callers can pass
     * the {@link org.lance.OpenDatasetBuilder} the null and skip the
     * {@code readOptions} configuration entirely — Lance uses its own
     * defaults in that case.
     */
    public ReadOptions toReadOptionsOrNull() {
        if (options.isEmpty()) {
            return null;
        }
        return new ReadOptions.Builder().setStorageOptions(options).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StorageOptions other)) return false;
        return options.equals(other.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(options);
    }

    /**
     * Human-readable representation with credential-like values redacted.
     * Anything whose key {@link #isSensitiveKey} classifies as sensitive
     * is shown as {@code ***REDACTED***}. Used for exception messages
     * and log lines that might otherwise leak credentials to
     * shard-failed responses.
     */
    @Override
    public String toString() {
        if (options.isEmpty()) {
            return "StorageOptions{}";
        }
        StringBuilder sb = new StringBuilder("StorageOptions{");
        boolean first = true;
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(entry.getKey()).append("=").append(isSensitiveKey(entry.getKey()) ? "***REDACTED***" : entry.getValue());
        }
        sb.append("}");
        return sb.toString();
    }
}

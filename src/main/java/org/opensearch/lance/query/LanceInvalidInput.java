/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.opensearch.lance.StorageOptions;

/**
 * Recognises the exception Lance raises for a request the caller got
 * wrong, so the failure can be answered as a client error.
 *
 * <p>Lance's JNI layer maps its {@code Error::InvalidInput} (a phrase
 * query against an inverted index built without positions, an unknown
 * tokenizer, a malformed SQL predicate) to
 * {@link IllegalArgumentException}, which OpenSearch's
 * {@code ExceptionsHelper.status} answers with HTTP 400 and renders as
 * {@code illegal_argument_exception}. The Lucene contract of
 * {@code Weight} and {@code ScorerSupplier} allows only
 * {@link java.io.IOException}, so the Lance scans of the query classes
 * keep the original exception as the cause of an {@code IOException},
 * and a plain {@code IOException} is a 500 at the REST layer. The
 * fragment executor calls {@link #unwrap} on the failure it is about to
 * report so the client sees Lance's message with the status the
 * criterion below assigns.
 *
 * <p>{@link #isInvalidInput} classifies an exception caught directly; a
 * scan failure arrives wrapped, so {@link #find} additionally walks the
 * cause chain. Both accept only an {@code IllegalArgumentException}
 * that Lance produced.
 *
 * <p>The criterion rests on two conventions of the Lance Java SDK the
 * plugin has no contract over, {@link #INVALID_INPUT_PREFIX} and
 * {@link #LANCE_PACKAGE}. {@code LanceInvalidInputTests} raises real
 * invalid input through the bundled SDK and checks both against it, so
 * an SDK upgrade that changes either wording or package fails that
 * test instead of silently answering 500 for a client mistake.
 * {@code docs/design/lance-error-mapping.md} records the rule.
 *
 * <p>A second criterion covers the object store refusing the
 * credentials a caller handed in with an attach or a namespace
 * registration. Lance raises that as {@code Error::IO} (a 500) with the
 * store's error body in the message, so {@link #credentialRejection}
 * recognises the store's own error code in the body
 * ({@link #S3_CREDENTIAL_ERROR_CODES}, {@link #GCS_CREDENTIAL_ERROR_REASONS},
 * {@link #AZURE_CREDENTIAL_ERROR_CODES}) and reports an
 * {@link IllegalArgumentException} whose message quotes the redacted
 * body. A third covers a bucket or container the URI names that the
 * store does not have ({@link #S3_MISSING_BUCKET_ERROR_CODES},
 * {@link #AZURE_MISSING_CONTAINER_ERROR_CODES}): Lance lists the
 * table's directory to find the latest manifest and the listing fails
 * with the store's body as {@code Error::IO}, so {@link #missingBucket}
 * reports it as a 400 that names the table. {@link #openFailure} is
 * what the attach action and the namespace service call on the failure
 * they are about to report.
 */
public final class LanceInvalidInput {

    /**
     * Display prefix of Lance's {@code Error::InvalidInput}
     * ({@code "Invalid user input: {source}, {location}"} in
     * lance-core's error type); the JNI layer forwards the display form
     * as the exception message. Depends on the Lance SDK's wording: on
     * an SDK upgrade, run {@code LanceInvalidInputTests}.
     */
    public static final String INVALID_INPUT_PREFIX = "Invalid user input";

    /**
     * Package of the Lance Java SDK; the native methods of its classes
     * are the frames the JNI exception is thrown from. Depends on the
     * SDK's package name: on an SDK upgrade, run
     * {@code LanceInvalidInputTests}.
     */
    public static final String LANCE_PACKAGE = "org.lance.";

    /**
     * The {@code <Code>} values of an S3 error body that say the
     * request's credentials were refused: the access key id is unknown,
     * the signature does not match the secret, the key has no
     * permission on the bucket, or the session token has expired or is
     * malformed. Depends on the codes S3 (and the S3 compatible stores)
     * return today; {@code LanceInvalidInputTests} pins the list.
     */
    public static final String[] S3_CREDENTIAL_ERROR_CODES = {
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "AccessDenied",
        "ExpiredToken",
        "InvalidToken" };

    /**
     * The {@code "reason"} values of a GCS JSON error body that say the
     * request's credentials were refused: the token is invalid or
     * expired ({@code authError}), or the account has no permission on
     * the bucket ({@code forbidden}, {@code insufficientPermissions}).
     * {@code LanceInvalidInputTests} pins the list.
     */
    public static final String[] GCS_CREDENTIAL_ERROR_REASONS = { "authError", "forbidden", "insufficientPermissions" };

    /**
     * The {@code <Code>} values of an Azure Blob Storage error body that
     * say the request's credentials were refused: the shared key or SAS
     * signature does not verify, or the identity has no permission on
     * the container. {@code LanceInvalidInputTests} pins the list.
     */
    public static final String[] AZURE_CREDENTIAL_ERROR_CODES = {
        "AuthenticationFailed",
        "AuthorizationFailure",
        "AuthorizationPermissionMismatch",
        "InvalidAuthenticationInfo" };

    /**
     * What a message reporting a rejected credential opens with; the
     * table or namespace follows in brackets, then the redacted store
     * error.
     */
    public static final String CREDENTIALS_REJECTED_PREFIX = "object store rejected the credentials of [";

    /**
     * The {@code <Code>} value of an S3 error body that says the bucket
     * the URI names does not exist. S3 compares bucket names exactly,
     * so a URI whose bucket differs in case from the real one gets this
     * code; the allowlist of {@code plugins.lance.allowed_table_roots}
     * compares bucket names case insensitively and the open uses the
     * URI as written. MinIO and the other S3 compatible stores answer
     * the same code. {@code LanceInvalidInputTests} pins the list.
     */
    public static final String[] S3_MISSING_BUCKET_ERROR_CODES = { "NoSuchBucket" };

    /**
     * The {@code <Code>} value of an Azure Blob Storage error body that
     * says the container the URI names does not exist.
     * {@code LanceInvalidInputTests} pins the list.
     */
    public static final String[] AZURE_MISSING_CONTAINER_ERROR_CODES = { "ContainerNotFound" };

    /**
     * What a message reporting a bucket or container the store does not
     * have opens with; the table or namespace follows in brackets, then
     * the redacted store error.
     */
    public static final String COULD_NOT_OPEN_PREFIX = "could not open [";

    /**
     * Matches the error code of a refused credential where the object
     * store puts it in its error body, which Lance quotes in its
     * message: the {@code <Code>} element of an S3 or Azure XML body,
     * or the {@code "reason"} member of a GCS JSON body. The code is
     * group 1 for the XML shape and group 2 for the JSON shape.
     */
    private static final Pattern CREDENTIAL_ERROR_CODE = Pattern.compile(
        "<Code>("
            + alternatives(S3_CREDENTIAL_ERROR_CODES, AZURE_CREDENTIAL_ERROR_CODES)
            + ")</Code>|\"reason\"\\s*:\\s*\"("
            + alternatives(GCS_CREDENTIAL_ERROR_REASONS)
            + ")\""
    );

    /**
     * Matches the error code of a bucket or container the store does
     * not have, in the {@code <Code>} element of an S3 or Azure XML
     * body. Lance lists the table's {@code _versions/} directory to
     * find the latest manifest, and a listing against a bucket that
     * does not exist fails with this body as an IO error, not as the
     * not found Lance raises for a missing table in a bucket that
     * exists.
     */
    private static final Pattern MISSING_BUCKET_ERROR_CODE = Pattern.compile(
        "<Code>(" + alternatives(S3_MISSING_BUCKET_ERROR_CODES, AZURE_MISSING_CONTAINER_ERROR_CODES) + ")</Code>"
    );

    /** Bound on the cause chain walk, in case a chain is cyclic. */
    private static final int MAX_DEPTH = 10;

    private LanceInvalidInput() {}

    private static String alternatives(String[]... lists) {
        StringBuilder out = new StringBuilder();
        for (String[] list : lists) {
            for (String code : list) {
                if (out.length() > 0) {
                    out.append('|');
                }
                out.append(Pattern.quote(code));
            }
        }
        return out.toString();
    }

    /**
     * Whether {@code t} is an {@link IllegalArgumentException} Lance
     * raised: its message carries the display form of
     * {@code Error::InvalidInput}, or the frame that threw it is a
     * method of the Lance SDK. An {@code IllegalArgumentException} from
     * Lucene or OpenSearch code inside a scan does not qualify, so a
     * plugin bug is not reported as the client's mistake.
     */
    public static boolean isInvalidInput(Throwable t) {
        if (!(t instanceof IllegalArgumentException)) {
            return false;
        }
        String message = t.getMessage();
        if (message != null && message.startsWith(INVALID_INPUT_PREFIX)) {
            return true;
        }
        StackTraceElement[] frames = t.getStackTrace();
        return frames.length > 0 && frames[0].getClassName().startsWith(LANCE_PACKAGE);
    }

    /**
     * The first exception in the cause chain of {@code t} (starting
     * with {@code t} itself) that {@link #isInvalidInput} accepts, or
     * {@code null} when the chain holds none. {@code null} in gives
     * {@code null} out.
     */
    public static IllegalArgumentException find(Throwable t) {
        Throwable current = t;
        for (int depth = 0; current != null && depth < MAX_DEPTH; depth++) {
            if (isInvalidInput(current)) {
                return (IllegalArgumentException) current;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                return null;
            }
            current = cause;
        }
        return null;
    }

    /**
     * The exception to report for {@code failure}: the Lance
     * {@link IllegalArgumentException} found in its cause chain when
     * there is one, so the status becomes 400 and the reason is Lance's
     * own message, otherwise {@code failure} itself. Either is passed
     * through {@link StorageOptions#redactCredentials(Exception)}, so a
     * message that quotes a credential (an object store error body
     * echoing the access key id) is reported without it whatever the
     * exception class; the redacted copy keeps the class of an
     * {@link IllegalArgumentException} and the status of anything else.
     * {@code failure} must not be null.
     */
    public static Exception unwrap(Exception failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        IllegalArgumentException invalid = find(failure);
        return StorageOptions.redactCredentials(invalid == null ? failure : invalid);
    }

    /**
     * The error code of a refused credential quoted in the message of
     * {@code t} or of an exception in its cause chain (one of
     * {@link #S3_CREDENTIAL_ERROR_CODES}, {@link #GCS_CREDENTIAL_ERROR_REASONS}
     * or {@link #AZURE_CREDENTIAL_ERROR_CODES}, in the body shape of its
     * store), or {@code null} when no message quotes one. The Lance SDK
     * raises every object store failure as {@code Error::IO} with the
     * store's response body in the message and no error type, so the
     * body's own code is the only thing that tells a refused credential
     * from an unreachable endpoint. {@code null} in gives {@code null}
     * out.
     */
    public static String credentialErrorCode(Throwable t) {
        String message = firstMessageMatching(t, CREDENTIAL_ERROR_CODE);
        if (message == null) {
            return null;
        }
        Matcher matcher = CREDENTIAL_ERROR_CODE.matcher(message);
        matcher.find();
        return matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
    }

    /**
     * The error code of a bucket or container the store does not have,
     * quoted in the message of {@code t} or of an exception in its
     * cause chain (one of {@link #S3_MISSING_BUCKET_ERROR_CODES} or
     * {@link #AZURE_MISSING_CONTAINER_ERROR_CODES}), or {@code null}
     * when no message quotes one. {@code null} in gives {@code null}
     * out.
     */
    public static String missingBucketErrorCode(Throwable t) {
        String message = firstMessageMatching(t, MISSING_BUCKET_ERROR_CODE);
        if (message == null) {
            return null;
        }
        Matcher matcher = MISSING_BUCKET_ERROR_CODE.matcher(message);
        matcher.find();
        return matcher.group(1);
    }

    /**
     * The first message in the cause chain of {@code t} that
     * {@code pattern} finds a match in, or {@code null}.
     */
    private static String firstMessageMatching(Throwable t, Pattern pattern) {
        Throwable current = t;
        for (int depth = 0; current != null && depth < MAX_DEPTH; depth++) {
            String message = current.getMessage();
            if (message != null && pattern.matcher(message).find()) {
                return message;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                return null;
            }
            current = cause;
        }
        return null;
    }

    /**
     * The exception to report when {@code failure} says the object
     * store refused the credentials {@code subject} (a table or a
     * namespace) was given: an {@link IllegalArgumentException}, so the
     * status is 400, whose message opens with
     * {@link #CREDENTIALS_REJECTED_PREFIX} and quotes the store's error
     * after {@link StorageOptions#redactCredentials(String)}, and whose
     * cause is the redacted copy of {@code failure} with its frames.
     * {@code null} when {@link #credentialErrorCode} finds no code in
     * the chain. {@code failure} must not be null.
     */
    public static IllegalArgumentException credentialRejection(Exception failure, String subject) {
        Objects.requireNonNull(failure, "failure must not be null");
        String store = firstMessageMatching(failure, CREDENTIAL_ERROR_CODE);
        if (store == null) {
            return null;
        }
        return new IllegalArgumentException(
            CREDENTIALS_REJECTED_PREFIX + subject + "]: " + StorageOptions.redactCredentials(store),
            StorageOptions.redactCredentials(failure)
        );
    }

    /**
     * The exception to report when {@code failure} says the bucket or
     * container the URI of {@code subject} (a table or a namespace)
     * names does not exist: an {@link IllegalArgumentException}, so the
     * status is 400, whose message opens with
     * {@link #COULD_NOT_OPEN_PREFIX}, names the subject, says the bucket
     * is not there and quotes the store's error after
     * {@link StorageOptions#redactCredentials(String)}, and whose cause
     * is the redacted copy of {@code failure} with its frames.
     * {@code null} when {@link #missingBucketErrorCode} finds no code
     * in the chain. {@code failure} must not be null.
     */
    public static IllegalArgumentException missingBucket(Exception failure, String subject) {
        Objects.requireNonNull(failure, "failure must not be null");
        String store = firstMessageMatching(failure, MISSING_BUCKET_ERROR_CODE);
        if (store == null) {
            return null;
        }
        return new IllegalArgumentException(
            COULD_NOT_OPEN_PREFIX
                + subject
                + "]: the object store has no bucket or container of that name (bucket names are compared exactly, "
                + "so check the case): "
                + StorageOptions.redactCredentials(store),
            StorageOptions.redactCredentials(failure)
        );
    }

    /**
     * The exception to report for a failure to open or list
     * {@code subject} (a table or a namespace) at the point where the
     * caller handed its URI and credentials in: {@link #credentialRejection}
     * when the store refused the credentials, {@link #missingBucket}
     * when the store has no bucket or container of the name the URI
     * carries, otherwise the redacted copy
     * {@link StorageOptions#redactCredentials(Exception)} gives, which
     * keeps the status of {@code failure}. A search that fails because
     * a credential expired after the attach is not routed through
     * here; the request path keeps its status. {@code failure} must not
     * be null.
     */
    public static Exception openFailure(Exception failure, String subject) {
        IllegalArgumentException rejected = credentialRejection(failure, subject);
        if (rejected != null) {
            return rejected;
        }
        IllegalArgumentException missing = missingBucket(failure, subject);
        return missing != null ? missing : StorageOptions.redactCredentials(failure);
    }
}

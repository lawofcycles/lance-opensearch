# Lance errors and the HTTP status the client sees

Lance's JNI layer translates its Rust `Error` variants into Java exceptions: `Error::InvalidInput`
(with `DatasetNotFound`, `DatasetAlreadyExists` and `CommitConflict`) becomes
`IllegalArgumentException`, `Error::IO` becomes `IOException`, the rest `RuntimeException`. The
plugin maps those onto HTTP status as follows.

- An `IllegalArgumentException` that `LanceInvalidInput.isInvalidInput` recognises as Lance's answers
  400 `illegal_argument_exception` with Lance's message. The fragment executor reads it back out of
  the `IOException` the Lucene `Weight` contract wrapped it in (`LanceInvalidInput.unwrap`).
- `TaskCancelledException` and `CircuitBreakingException` pass through unchanged.
- Any other exception, including an `IllegalArgumentException` the plugin's own code raised inside a
  scan, answers 500, so a plugin bug is not reported as the client's mistake.

The recognition rests on two SDK conventions the plugin has no contract over, kept as constants on
`LanceInvalidInput`: the message opens with `Invalid user input` (the display form of
`Error::InvalidInput` in lance-core), or the frame that threw is a method of a class under
`org.lance.`. `LanceInvalidInputTests` raises real invalid input through the bundled SDK and checks
both, so an SDK upgrade that changes either fails the test rather than turning 400 into 500.

## Object store credentials the store refuses

An attach or a namespace registration carries the caller's `storage_options`. When the object store
refuses them, Lance raises `Error::IO` with the store's response body in the message and no error
type, so by the rule above the failure would be a 500. `LanceInvalidInput.openFailure`, called by the
attach action on the failure it is about to report and by the namespace service on an initialise or
listing failure, looks for the store's own error code in the body and, when it names a refused
credential, reports an `IllegalArgumentException` (400) instead. The message is
`object store rejected the credentials of [<table or namespace>]: ` followed by Lance's message after
the credential redaction (`StorageOptions.redactCredentials`), so the S3 body's `<AWSAccessKeyId>`
is `***`; the cause is the redacted copy of the original with its frames.

The codes are constants on `LanceInvalidInput`, pinned by `LanceInvalidInputTests`, matched in the
shape each store puts them in.

| Store | Shape in the body | Codes |
| --- | --- | --- |
| S3 and S3 compatible stores | `<Code>X</Code>` | `InvalidAccessKeyId`, `SignatureDoesNotMatch`, `AccessDenied`, `ExpiredToken`, `InvalidToken` |
| GCS | `"reason": "X"` | `authError`, `forbidden`, `insufficientPermissions` |
| Azure Blob Storage | `<Code>X</Code>` | `AuthenticationFailed`, `AuthorizationFailure`, `AuthorizationPermissionMismatch`, `InvalidAuthenticationInfo` |

Any other body (`NoSuchBucket`, a connection refused, a local `Permission denied`) keeps the 500. The
rule applies only where the credentials arrive: a search that fails because a session token expired
after the attach runs on the request path, which does not call `openFailure`, and stays a 500.

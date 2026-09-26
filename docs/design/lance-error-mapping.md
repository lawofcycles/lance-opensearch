# Lance errors and the HTTP status the client sees

Lance's JNI layer translates its Rust `Error` variants into Java exceptions: `Error::InvalidInput`
(with `DatasetNotFound`, `DatasetAlreadyExists` and `CommitConflict`) becomes
`IllegalArgumentException`, `Error::IO` becomes `IOException`, the rest `RuntimeException`. The
plugin maps those onto HTTP status as follows.

- An `IllegalArgumentException` that `LanceInvalidInput.isInvalidInput` recognises as Lance's answers
  400 `illegal_argument_exception` with Lance's message. The fragment executor reads it back out of
  the `IOException` the Lucene `Weight` contract wrapped it in (`LanceInvalidInput.unwrap`), and
  `POST /_lance/build_indexes` reports it as `failed` with the flag that makes the response 400.
- `TaskCancelledException` and `CircuitBreakingException` pass through unchanged.
- Any other exception, including an `IllegalArgumentException` the plugin's own code raised inside a
  scan, answers 500, so a plugin bug is not reported as the client's mistake.

The recognition rests on two SDK conventions the plugin has no contract over, kept as constants on
`LanceInvalidInput`: the message opens with `Invalid user input` (the display form of
`Error::InvalidInput` in lance-core), or the frame that threw is a method of a class under
`org.lance.`. `LanceInvalidInputTests` raises real invalid input through the bundled SDK and checks
both, so an SDK upgrade that changes either fails the test rather than turning 400 into 500.

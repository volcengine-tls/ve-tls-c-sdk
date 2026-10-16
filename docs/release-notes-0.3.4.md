# Release Notes: 0.3.4 (preparation)

Status: preparation only. No Git tag has been created and no release has been
published for this candidate.

## Version identifiers

- SDK package version: `0.3.4`.
- Default TLS service API header version: `0.3.0` (`VE_TLS_C_SDK_API_VERSION`
  remains `"0.3.0"`).

The package version identifies this distribution. The service API value is sent
in `x-tls-apiversion`; it does not identify the C ABI. C compatibility is verified
through exported symbols and exact configuration version/size contracts.

## Included changes

- API-key authentication is available through the public producer constructors,
  including exact-size versioned configuration validation and key capture per
  request attempt. API-key requests disable HTTP debug logging to avoid exposing
  the key.
- Versioned configuration initialization and creation enforce the legacy-prefix
  and exact-size/version boundaries. Version 3 covers memory-retry,
  timeout-binding, delivery-diagnostic, and transient-HTTP policy fields while
  legacy creation leaves that tail disabled.
- Public delivery diagnostics and send-done callbacks expose delivery state,
  retry and retention outcomes, and borrowed callback metadata with documented
  lifetime and threading rules.
- Review fixes preserve the legacy initialization prefix, reject mismatched
  versioned layouts without reading beyond the supplied size, and keep delivery
  diagnostics consistent across retained-close and retry paths.

## Release gate

The static/shared build, test, export, sanitizer, installed-consumer, and
platform checks remain release-gate work for the eventual 0.3.4 tag. Tagging and
publishing are intentionally outside this preparation change.

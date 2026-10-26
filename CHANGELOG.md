# Changelog

All notable changes to `com.babelqueue:babelqueue-artemis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.1.1] - 2026-10-26

### Changed

- The poison DLQ block is built by `babelqueue-core` 1.14.0; no wire change. Requires `babelqueue-core` 1.14.0 (was 1.13.0).

## [1.1.0] - 2026-10-26

MINOR (ADR-0037): poison bodies are dead-lettered as the ADR-0035 record and the version gate follows one
settle rule. The envelope is unchanged (`schema_version` stays `1`).

### Changed (behaviour)

- **Poison body** (not JSON, not an envelope, no valid identity, or an unsupported `meta.schema_version`):
  the DLQ record is now the **ADR-0035 record** (`PoisonRecord`: an object body spliced with a `dead_letter`
  block, `reason: "poison"`, `original_queue` = the consumed queue; any other body under `raw`, base64 with
  `raw_encoding` when it is not valid UTF-8) instead of a forward of the raw body. It is acknowledged only
  after the write succeeded (otherwise `recover()`, as before). Under `unknownUrn(DELETE)` it is now
  acknowledged without a record. With no dead-letter queue it is acknowledged, dropped and **logged**
  (unchanged, now visible).
- **Version-gated delivery** (`bq_schema_version` present, not blank, not `1`): settled on the first delivery,
  never decoded, never retried. The raw body is still forwarded to the DLQ byte for byte (no `dead_letter`
  block) and acknowledged after the write, but now **not** under `unknownUrn(DELETE)`: that acknowledges it
  without a record (**dropped**). With no dead-letter queue it is acknowledged, dropped and logged.
- **`BytesMessage` bodies are read, not dropped.** `JmsMessages.text(...)` treated every non-`TextMessage` as an
  empty body, so a message from an AMQP 1.0 producer (its Data section arrives as a `BytesMessage`) was decoded as
  `""`, and a poison one was recorded as `{"raw": ""}` and acknowledged, losing its bytes. The consumer now reads the
  exact body bytes of a `TextMessage` (UTF-8; empty when unset) or a `BytesMessage` (new `JmsMessages.bodyBytes`)
  and decodes them with a **strict** UTF-8 check: a valid envelope in a `BytesMessage` is routed normally; a body
  that is not valid UTF-8 (never decoded leniently) is poison and its exact bytes go to the ADR-0035 record
  (`raw` base64 with `raw_encoding: "base64"`). A version-gated `BytesMessage` is forwarded to the DLQ as a
  `BytesMessage` with the same bytes. A `TextMessage` with no body is read as empty instead of `null`.
- Requires `babelqueue-core` 1.13.0 (was 1.11.0) for `PoisonRecord`.
- Vendored the cross-SDK conformance suite v1.8.0 (`version_gate_artemis`, `poison_dlq` runners added) and moved
  the CI pin to it.

## [1.0.3] - 2026-10-17

### Fixed

- Unknown top-level keys and unknown `meta` keys (ADR-0031 extension keys, e.g. `extra_top`, `meta.vendor_flag`) are no longer dropped when `ArtemisConsumer` reconciles `attempts` with `JMSXDeliveryCount`: the envelope is now rebuilt with `Envelope.withAttempts`, which carries the extras, instead of the 6-argument constructor. The extras therefore also survive into the dead-letter copy. Regression tests cover both paths.

### Changed

- The `babelqueue-core` dependency is raised from 1.0.0 to 1.11.0. 1.8.0 is the first core release whose `Envelope` carries extras and exposes `withAttempts`; 1.11.0 adds the ADR-0031 decode behaviour (`job` + `urn` warning, closed ASCII blank set) and encode-time extension-key rejection.

## [1.0.2] - 2026-10-11

### Fixed

- `ArtemisConsumer` no longer risks losing a message when a DLQ write fails on the non-gate paths
  (non-conformant envelope, unmapped URN under `dead_letter`, handler failure after max-tries). The
  `JMSException` escaped without `recover()`, leaving the message unacknowledged on a `CLIENT_ACKNOWLEDGE`
  session where a later message's session-wide `acknowledge()` silently acknowledged it too. The message is
  now acknowledged only after the DLQ write succeeded; otherwise the session is `recover()`ed (redelivery) and
  the failure is reported via `onError` - the same protection the §7.7 gate already had.

## [1.0.1] - 2026-10-10

### Fixed

- `ArtemisConsumer` now applies the §7.7 schema-version gate **before** decoding the body: when the
  `bq_schema_version` JMS string property is present and is not the supported version (`1`), the message
  is never decoded and the handler is never called; it is reported via `onError` (with a `null` envelope),
  its raw body is forwarded to `<queue>.dlq` (when configured) and it is acknowledged. A missing or blank
  property (empty or ASCII-whitespace-only) keeps the existing decode path. Cross-SDK parity (GR-5): the
  value is compared exactly, with no trimming.
  If the DLQ write fails, the session is `recover()`ed instead of acknowledging (CLIENT_ACKNOWLEDGE is
  session-wide, so a later message's `acknowledge()` would otherwise swallow the rejected one) and the
  failure goes to `onError`; the message is redelivered and rejected again, never lost.

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Apache ActiveMQ Artemis transport on `babelqueue-core` over **JMS**
  (Jakarta Messaging 3.x), implementing §7 of the broker-bindings contract. Artemis offers
  native ack/scheduled-delivery/delivery-counter/dead-letter-address, so the binding maps onto
  them rather than re-implementing: `ArtemisPublisher` (body = canonical envelope `TextMessage`,
  `JMSType` = URN, `JMSCorrelationID` = `trace_id`, the `bq-` string-property projection — so a
  JMS or AMQP-1.0 consumer routes on `JMSType`; a delay uses native JMS 2.0 `setDeliveryDelay`)
  and `ArtemisConsumer` (`CLIENT_ACKNOWLEDGE` consume — acknowledge after success; a throwing
  handler `recover()`s the session for broker redelivery; **`attempts = max(body,
  JMSXDeliveryCount − 1)`** with the broker's 1-based counter authoritative; terminal failures
  go to `<queue>.dlq` with the additive `dead_letter` block; `fail`/`delete`/`release`/
  `dead_letter` unknown-URN strategies; poison bodies forwarded raw to the DLQ). Java 17, JUnit
  5, JaCoCo ≥90% line coverage; the JMS interfaces are mocked with Mockito (no Artemis, no
  network). The envelope is unchanged (`schema_version: 1`); Apache ActiveMQ Artemis is purely
  additive.

# Changelog

All notable changes to `com.babelqueue:babelqueue-artemis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

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

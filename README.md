# BabelQueue — Apache ActiveMQ Artemis (Java)

`com.babelqueue:babelqueue-artemis` — an Apache ActiveMQ Artemis transport for
[BabelQueue](https://babelqueue.com), built on **JMS** (Jakarta Messaging 3.x) and the
framework-agnostic [`babelqueue-core`](https://github.com/BabelQueue/babelqueue-java).

A canonical-envelope **publisher** and a URN-routed, `CLIENT_ACKNOWLEDGE` **consumer**, so an
Artemis-based Java service speaks the same wire contract (envelope shape, URN identity, trace
propagation) as the .NET, Python, Go and Node SDKs. Implements
[§7 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-activemq-artemis).

Unlike Kafka, Artemis gives the binding **native** primitives — per-message acknowledgement,
scheduled delivery, a delivery counter and a dead-letter address — so this transport maps onto
them instead of re-implementing them (the envelope stays `schema_version: 1`):

- the envelope JSON is the message **body** (`TextMessage`); the contract fields are mirrored
  onto JMS metadata — `JMSType` = URN, `JMSCorrelationID` = `trace_id`, `JMSTimestamp` =
  `created_at` — plus the `bq-` string properties (so a JMS **or** AMQP-1.0 consumer routes on
  `JMSType` without decoding the body);
- consume is `CLIENT_ACKNOWLEDGE`: **acknowledge after success**; a throwing handler leaves the
  message unacknowledged and `recover()`s the session so the broker redelivers it (incrementing
  `JMSXDeliveryCount`);
- **`attempts = max(body, JMSXDeliveryCount − 1)`** — `JMSXDeliveryCount` is the broker's
  1-based authoritative redelivery counter, the body's `attempts` the floor;
- delay uses **native** JMS 2.0 scheduled delivery (`setDeliveryDelay`); terminal failures go to
  an opt-in `<queue>.dlq` carrying the canonical envelope plus the additive `dead_letter` block,
  cross-language alongside Artemis's own dead-letter address.

## Install (Maven)

```xml
<dependency>
  <groupId>com.babelqueue</groupId>
  <artifactId>babelqueue-artemis</artifactId>
  <version>1.0.0</version>
</dependency>
```

It pulls `babelqueue-core` transitively. The JMS API is `provided`-style — bring your Artemis
JMS client (`org.apache.activemq:artemis-jakarta-client`), which supplies both the
`jakarta.jms` API and the broker connection.

## Produce

```java
ConnectionFactory factory = new org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory(
    "tcp://localhost:61616");

try (JMSContext ctx = factory.createContext("user", "pass")) {
    Session session = ctx.createSession(Session.CLIENT_ACKNOWLEDGE);
    MessageProducer producer = session.createProducer(session.createQueue("orders"));

    String id = ArtemisPublisher.create(session, producer)
        .publish("urn:babel:orders:created", Map.of("order_id", 1042));
}
```

`publish(urn, data)` returns the message `meta.id`; overloads add a `traceId` and a relative
`Duration delay` (native `setDeliveryDelay`).

## Consume

```java
Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
MessageConsumer consumer = session.createConsumer(session.createQueue("orders"));
MessageProducer dlqProducer = session.createProducer(null); // anonymous, for <queue>.dlq

ArtemisConsumer worker = ArtemisConsumer.builder(consumer, session)
    .handler("urn:babel:orders:created", (env, message) -> {
        // env.data(), env.traceId(), env.attempts() ...
    })
    .deadLetterQueue(dlqProducer, "orders.dlq")
    .maxTries(3)
    .onError((err, env, message) -> err.printStackTrace())
    .build();

connection.start();
worker.run(() -> true); // receive → process → acknowledge, until you stop it
```

A successful handler `acknowledge()`s the message. A throwing handler leaves it unacknowledged
and `recover()`s the session, so the broker redelivers it and bumps `JMSXDeliveryCount`; once
`maxTries` is reached the envelope goes to `<queue>.dlq` with a `dead_letter` block. The consumer
routes on `JMSType`, so it never decodes a message it cannot handle. Unknown-URN strategy is one
of `fail` / `delete` / `release` / `dead_letter`.

> One message per `poll()` keeps the session-wide `acknowledge()` / `recover()` correct. A JMS
> session is single-threaded — run one `ArtemisConsumer` per thread.

## Contract mapping (§7)

| Envelope | Apache ActiveMQ Artemis (JMS) |
| :--- | :--- |
| body | message body (`TextMessage`, byte-identical across SDKs) |
| `job` (URN) | `JMSType` (consumer routes on this) |
| `trace_id` | `JMSCorrelationID` |
| `meta.id` | `JMSMessageID` (broker-set for JMS; body is authoritative) |
| `meta.schema_version` | property `bq-schema-version` (`"1"`) |
| `meta.lang` | property `bq-source-lang` |
| `meta.created_at` | `JMSTimestamp` (Unix ms) |
| `attempts` | `max(body, JMSXDeliveryCount − 1)` (broker counter is 1-based) |
| reserve / ack | `receive` → process → **`acknowledge()`** (CLIENT_ACKNOWLEDGE) |
| retry / delay | `recover()` redelivery · native `setDeliveryDelay` |
| dead-letter | `<queue>.dlq` + `dead_letter` block (alongside the native DLA) |

The `bq-` property values are strings (integers as decimal, e.g. `"1"`); `bq-app-id` is
`"babelqueue"`. The envelope is unchanged (`schema_version` stays `1`); Artemis is purely
additive.

## Build & test

```bash
mvn verify
```

The JMS interfaces (`Session`, `MessageProducer`, `MessageConsumer`, `Message`) are mocked with
Mockito — no Artemis, no network. JUnit 5, JaCoCo ≥90% line coverage.

## License

MIT

/**
 * Apache ActiveMQ Artemis (JMS) transport for BabelQueue — implements §7 of the
 * broker-bindings contract.
 *
 * <p>Artemis speaks <strong>AMQP 1.0</strong> (not AMQP 0-9-1); Java reaches it via
 * <strong>Jakarta Messaging (JMS)</strong>. The canonical envelope is the message body
 * (a {@code TextMessage}); the contract fields are projected onto native JMS metadata so a
 * JMS or AMQP-1.0 consumer in any language routes/correlates without decoding the body:
 *
 * <ul>
 *   <li>{@code JMSType} = the URN ({@code job}) — route on it;</li>
 *   <li>{@code JMSCorrelationID} = {@code trace_id};</li>
 *   <li>string properties {@code bq-schema-version} / {@code bq-source-lang} /
 *       {@code bq-attempts} (the 0-based mirror, for AMQP-1.0 consumers) / {@code bq-app-id};</li>
 *   <li>{@code attempts} is reconciled to {@code max(body, JMSXDeliveryCount − 1)} —
 *       {@code JMSXDeliveryCount} is the broker-maintained, 1-based authoritative counter.</li>
 * </ul>
 *
 * <p>Consume uses {@code CLIENT_ACKNOWLEDGE}: acknowledge after success; a throwing handler
 * leaves the message unacknowledged and {@code recover}s the session so the broker redelivers
 * it (incrementing {@code JMSXDeliveryCount}); at max-tries it goes to {@code <queue>.dlq} with
 * the additive {@code dead_letter} block. Delay is native — JMS&nbsp;2.0
 * {@code setDeliveryDelay}. The envelope is unchanged ({@code schema_version} stays 1).
 *
 * <p>Full spec: <a href="https://babelqueue.com">babelqueue.com</a>.
 */
package com.babelqueue.artemis;

package com.babelqueue.artemis;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Sends canonical-envelope messages to one Artemis queue with the §7 JMS projection: the body
 * is the envelope JSON ({@code TextMessage}), {@code JMSType} = URN, {@code JMSCorrelationID} =
 * {@code trace_id}, plus the {@code bq-} string properties — so a JMS or AMQP-1.0 consumer can
 * route on {@code JMSType} without decoding the body. The envelope is unchanged
 * ({@code schema_version} stays 1); Artemis is purely additive.
 *
 * <p>A positive delay uses native JMS&nbsp;2.0 scheduled delivery ({@code setDeliveryDelay});
 * {@code bq-delay} is set for observability.
 */
public final class ArtemisPublisher {

    private final Session session;
    private final MessageProducer producer;

    private ArtemisPublisher(Session session, MessageProducer producer) {
        this.session = Objects.requireNonNull(session, "session");
        this.producer = Objects.requireNonNull(producer, "producer");
    }

    /** A publisher over the given session and producer (the producer's destination is the queue). */
    public static ArtemisPublisher create(Session session, MessageProducer producer) {
        return new ArtemisPublisher(session, producer);
    }

    /** Publish {@code (urn, data)} as a canonical envelope; returns the message id ({@code meta.id}). */
    public String publish(String urn, Map<String, Object> data) throws JMSException {
        return publish(urn, data, null, null);
    }

    /** Publish, continuing an existing {@code traceId} (or {@code null} to mint a fresh one). */
    public String publish(String urn, Map<String, Object> data, String traceId) throws JMSException {
        return publish(urn, data, traceId, null);
    }

    /** Publish with an optional relative {@code delay} (native JMS 2.0 {@code setDeliveryDelay}). */
    public String publish(String urn, Map<String, Object> data, String traceId, Duration delay) throws JMSException {
        Envelope envelope = EnvelopeCodec.make(urn, data, queueName(), traceId);
        TextMessage message = JmsMessages.project(session, envelope);

        if (delay != null && !delay.isZero() && !delay.isNegative()) {
            message.setStringProperty(JmsMessages.DELAY, Long.toString(delay.toMillis()));
            producer.setDeliveryDelay(delay.toMillis());
            try {
                producer.send(message);
            } finally {
                producer.setDeliveryDelay(0);
            }
        } else {
            producer.send(message);
        }
        return envelope.meta() != null ? envelope.meta().id() : "";
    }

    private String queueName() {
        try {
            Destination destination = producer.getDestination();
            if (destination instanceof Queue queue) {
                return queue.getQueueName();
            }
        } catch (JMSException e) {
            return "";
        }
        return "";
    }
}

package com.babelqueue.artemis;

import com.babelqueue.Envelope;
import jakarta.jms.Message;

/**
 * Processes one decoded, validated envelope and the raw JMS message it arrived on. Returning
 * normally acknowledges it (the consumer {@code acknowledge}s it); throwing leaves it
 * unacknowledged so the broker redelivers it (incrementing {@code JMSXDeliveryCount}), or
 * routes it to the DLQ once max-tries is reached.
 */
@FunctionalInterface
public interface BabelHandler {
    void handle(Envelope envelope, Message message) throws Exception;
}

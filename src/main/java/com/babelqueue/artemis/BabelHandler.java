package com.babelqueue.artemis;

import com.babelqueue.Envelope;
import jakarta.jms.Message;
import java.util.Map;

/**
 * Processes one decoded, validated envelope and the raw JMS message it arrived on. Returning
 * normally acknowledges it (the consumer {@code acknowledge}s it); throwing leaves it
 * unacknowledged so the broker redelivers it (incrementing {@code JMSXDeliveryCount}), or
 * routes it to the DLQ once max-tries is reached.
 *
 * <p>The raw message carries the out-of-band transport headers (ADR-0028) as JMS string properties.
 * {@link #headersOf} surfaces them as a plain map, ready for the optional OpenTelemetry wrapper so the
 * consumer span becomes a true child of the producer span:
 *
 * <pre>{@code
 * ArtemisConsumer.builder(consumer, session)
 *     .handler(urn, (env, message) -> Tracing
 *         .wrapHandler(tracer, h, () -> BabelHandler.headersOf(message))
 *         .handle(env))
 *     .build();
 * }</pre>
 */
@FunctionalInterface
public interface BabelHandler {
    void handle(Envelope envelope, Message message) throws Exception;

    /**
     * The message's string properties as a {@code Map<String, String>} (blank and non-string values dropped) —
     * the delivered-headers {@code Supplier} source for {@code com.babelqueue.otel.Tracing#wrapHandler}.
     * Contains the contract {@code bq_} properties and any out-of-band header (e.g. {@code traceparent},
     * {@code tracestate}). Empty when {@code message} is {@code null} or unreadable. Needs no OpenTelemetry
     * dependency.
     */
    static Map<String, String> headersOf(Message message) {
        return JmsMessages.toMap(message);
    }
}

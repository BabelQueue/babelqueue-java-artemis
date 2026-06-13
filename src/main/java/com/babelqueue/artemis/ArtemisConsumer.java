package com.babelqueue.artemis;

import com.babelqueue.BabelQueueException;
import com.babelqueue.DeadLetters;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import com.babelqueue.UnknownUrnStrategy;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Receives from an Artemis queue over JMS in {@code CLIENT_ACKNOWLEDGE} mode, decodes and
 * validates each message, routes it to the handler registered for its URN (read from
 * {@code JMSType}), and acknowledges it on success. A throwing handler leaves the message
 * unacknowledged and {@code recover}s the session so the broker redelivers it (incrementing
 * {@code JMSXDeliveryCount}); once max-tries is reached the envelope goes to {@code <queue>.dlq}
 * with a {@code dead_letter} block. {@code attempts} is reconciled to
 * {@code max(body, JMSXDeliveryCount − 1)} — {@code JMSXDeliveryCount} is the broker-maintained,
 * 1-based authoritative counter.
 *
 * <p>Process one message per {@link #poll()} so the per-message {@code acknowledge()} and
 * {@code recover()} stay correct under the session-wide ack model. Not thread-safe — a JMS
 * session is single-threaded; run one per thread.
 */
public final class ArtemisConsumer {

    /** Notified of a non-conformant message, an unmapped URN, or a throwing handler. */
    @FunctionalInterface
    public interface ErrorHandler {
        void onError(Throwable error, Envelope envelope, Message message);
    }

    private final MessageConsumer consumer;
    private final Session session;
    private final Map<String, BabelHandler> handlers;
    private final MessageProducer dlqProducer;
    private final String dlqQueue;
    private final int maxTries;
    private final String unknownUrn;
    private final ErrorHandler onError;
    private final long receiveTimeoutMillis;

    private ArtemisConsumer(Builder builder) {
        this.consumer = builder.consumer;
        this.session = builder.session;
        this.handlers = Map.copyOf(builder.handlers);
        this.dlqProducer = builder.dlqProducer;
        this.dlqQueue = builder.dlqQueue;
        this.maxTries = builder.maxTries;
        this.unknownUrn = builder.unknownUrn;
        this.onError = builder.onError;
        this.receiveTimeoutMillis = builder.receiveTimeoutMillis;
    }

    public static Builder builder(MessageConsumer consumer, Session session) {
        return new Builder(consumer, session);
    }

    /** Receive one message (up to the receive timeout), route + settle it. Returns 1, or 0 on timeout. */
    public int poll() throws JMSException {
        Message message = consumer.receive(receiveTimeoutMillis);
        if (message == null) {
            return 0;
        }
        handle(message);
        return 1;
    }

    /** Poll while {@code shouldContinue} returns true; a transient JMS error is reported and retried. */
    public void run(BooleanSupplier shouldContinue) {
        while (shouldContinue.getAsBoolean()) {
            try {
                poll();
            } catch (JMSException error) {
                report(error, null, null);
            }
        }
    }

    private void handle(Message message) throws JMSException {
        Envelope envelope = reconcile(EnvelopeCodec.decode(JmsMessages.text(message)), message);

        if (!EnvelopeCodec.accepts(envelope)) {
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Artemis."),
                envelope, message);
            deadLetterRaw(message);
            message.acknowledge();
            return;
        }

        String urn = urn(message, envelope);
        BabelHandler handler = handlers.get(urn);
        if (handler == null) {
            onUnknownUrn(message, envelope, urn);
            return;
        }

        try {
            handler.handle(envelope, message);
            message.acknowledge();
        } catch (Exception error) {
            report(error, envelope, message);
            retryOrDeadLetter(message, envelope, error);
        }
    }

    /** {@code attempts = max(body, JMSXDeliveryCount − 1)}: count 1 (or absent) keeps the body's count. */
    private static Envelope reconcile(Envelope envelope, Message message) {
        int count = JmsMessages.deliveryCount(message);
        if (count <= 1) {
            return envelope;
        }
        int delivered = count - 1;
        return delivered > envelope.attempts() ? withAttempts(envelope, delivered) : envelope;
    }

    private void onUnknownUrn(Message message, Envelope envelope, String urn) throws JMSException {
        switch (unknownUrn) {
            case UnknownUrnStrategy.DELETE -> message.acknowledge();
            case UnknownUrnStrategy.DEAD_LETTER -> {
                deadLetter(envelope, message, "unknown_urn", null);
                message.acknowledge();
            }
            case UnknownUrnStrategy.RELEASE -> session.recover();
            // FAIL (default): surface and do NOT acknowledge — the broker redelivers, then DLAs.
            default -> {
                report(new UnknownUrnException(urn), envelope, message);
                throw new UnknownUrnException(urn);
            }
        }
    }

    private void retryOrDeadLetter(Message message, Envelope envelope, Throwable error) throws JMSException {
        if (envelope.attempts() + 1 < maxTries) {
            // Leave it unacknowledged and force redelivery — the broker increments JMSXDeliveryCount.
            session.recover();
        } else if (dlqQueue != null && dlqProducer != null) {
            deadLetter(envelope, message, "failed", error);
            message.acknowledge();
        } else {
            message.acknowledge(); // terminal, no DLQ → degrade to drop
        }
    }

    private void deadLetter(Envelope envelope, Message message, String reason, Throwable error) throws JMSException {
        if (dlqQueue == null || dlqProducer == null) {
            return;
        }
        Envelope annotated = DeadLetters.annotate(
            envelope, reason, originalQueue(envelope), envelope.attempts(),
            error == null ? null : error.getMessage(),
            error == null ? null : error.getClass().getName());
        dlqProducer.send(session.createQueue(dlqQueue), JmsMessages.project(session, annotated));
    }

    /** Forward an undecodable message's raw body to the DLQ (no envelope to annotate). */
    private void deadLetterRaw(Message message) throws JMSException {
        if (dlqQueue == null || dlqProducer == null) {
            return;
        }
        TextMessage forwarded = session.createTextMessage(JmsMessages.text(message));
        dlqProducer.send(session.createQueue(dlqQueue), forwarded);
    }

    private static String urn(Message message, Envelope envelope) {
        String type = JmsMessages.type(message);
        return type != null && !type.isEmpty() ? type : EnvelopeCodec.urn(envelope);
    }

    private static String originalQueue(Envelope envelope) {
        return envelope.meta() != null && envelope.meta().queue() != null ? envelope.meta().queue() : "";
    }

    private static Envelope withAttempts(Envelope e, int attempts) {
        return new Envelope(e.job(), e.traceId(), e.data(), e.meta(), attempts, e.deadLetter());
    }

    private void report(Throwable error, Envelope envelope, Message message) {
        if (onError != null) {
            onError.onError(error, envelope, message);
        }
    }

    /** Fluent builder for {@link ArtemisConsumer}. */
    public static final class Builder {
        private final MessageConsumer consumer;
        private final Session session;
        private final Map<String, BabelHandler> handlers = new HashMap<>();
        private MessageProducer dlqProducer;
        private String dlqQueue;
        private int maxTries = 3;
        private String unknownUrn = UnknownUrnStrategy.FAIL;
        private ErrorHandler onError;
        private long receiveTimeoutMillis = 1000;

        private Builder(MessageConsumer consumer, Session session) {
            this.consumer = Objects.requireNonNull(consumer, "consumer");
            this.session = Objects.requireNonNull(session, "session");
        }

        public Builder handler(String urn, BabelHandler handler) {
            this.handlers.put(urn, handler);
            return this;
        }

        public Builder handlers(Map<String, BabelHandler> handlers) {
            this.handlers.putAll(handlers);
            return this;
        }

        /** Enable dead-lettering: an (anonymous) producer + the DLQ queue name. */
        public Builder deadLetterQueue(MessageProducer producer, String dlqQueue) {
            this.dlqProducer = producer;
            this.dlqQueue = dlqQueue;
            return this;
        }

        /** Attempts before terminal dead-lettering (default 3). */
        public Builder maxTries(int maxTries) {
            this.maxTries = maxTries;
            return this;
        }

        /** Strategy for a URN with no handler: {@link UnknownUrnStrategy} values (default {@code fail}). */
        public Builder unknownUrn(String strategy) {
            this.unknownUrn = strategy;
            return this;
        }

        public Builder onError(ErrorHandler onError) {
            this.onError = onError;
            return this;
        }

        public Builder receiveTimeout(long millis) {
            this.receiveTimeoutMillis = millis;
            return this;
        }

        public ArtemisConsumer build() {
            return new ArtemisConsumer(this);
        }
    }
}

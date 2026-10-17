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
 * <p>Per §7.7 the {@code bq_schema_version} string property is checked <b>before</b> the body is
 * decoded: when it is present, not blank (empty or ASCII-whitespace-only) and not exactly
 * {@link EnvelopeCodec#SCHEMA_VERSION} (compared as sent, no trimming), the message is never decoded and
 * the handler is never called — {@code onError} is notified (with a {@code null} envelope), the raw body
 * is forwarded to the DLQ (when configured) and the message is acknowledged — but only after the DLQ write succeeded. If it fails, the session is
 * {@code recover()}ed (the message is redelivered and rejected again, never lost, never handled — a later
 * message's session-wide {@code acknowledge()} cannot swallow it) and the failure goes to {@code onError}.
 * An absent or blank property
 * changes nothing, and the post-decode {@link EnvelopeCodec#accepts} check still applies.
 *
 * <p>The same rule holds for every other DLQ write (a non-conformant envelope, an unmapped URN under
 * {@code dead_letter}, a handler that exhausted max-tries): the message is acknowledged only after the DLQ write
 * succeeded; otherwise the session is {@code recover()}ed and the failure goes to {@code onError}.
 *
 * <p>Process one message per {@link #poll()} so the per-message {@code acknowledge()} and
 * {@code recover()} stay correct under the session-wide ack model. Not thread-safe — a JMS
 * session is single-threaded; run one per thread.
 */
public final class ArtemisConsumer {

    /**
     * Notified of a non-conformant message, an unmapped URN, a throwing handler, or a message rejected by the
     * §7.7 schema-version gate. {@code envelope} is {@code null} when the body was not decoded.
     */
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
        String declaredVersion = message.getStringProperty(JmsMessages.SCHEMA_VERSION);
        if (declaredVersion != null && !isBlankSchemaVersion(declaredVersion)
            && !declaredVersion.equals(Integer.toString(EnvelopeCodec.SCHEMA_VERSION))) {
            // §7.7: reject WITHOUT decoding — never best-effort parse an unknown schema.
            BabelQueueException rejected = new BabelQueueException("Rejected a BabelQueue message with unsupported "
                + JmsMessages.SCHEMA_VERSION + " property '" + declaredVersion + "' (supported: "
                + EnvelopeCodec.SCHEMA_VERSION + "); body not decoded.");
            // CLIENT_ACKNOWLEDGE is session-wide: a later message's acknowledge() would also acknowledge this
            // one. So if the DLQ write fails, force redelivery (recover) instead of leaving it unacknowledged —
            // the message is never lost and never reaches the handler; it is rejected again when redelivered.
            // Settle first, report afterwards, so a throwing onError cannot bypass the ack/recover protection.
            try {
                deadLetterRaw(message);
            } catch (JMSException | RuntimeException dlqFailure) {
                session.recover();
                report(rejected, null, message);
                report(new BabelQueueException("Could not dead-letter a message rejected by the "
                    + JmsMessages.SCHEMA_VERSION + " gate; not acknowledged, session recovered for redelivery.",
                    dlqFailure), null, message);
                return;
            }
            message.acknowledge();
            report(rejected, null, message);
            return;
        }

        Envelope envelope = reconcile(EnvelopeCodec.decode(JmsMessages.text(message)), message);

        if (!EnvelopeCodec.accepts(envelope)) {
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Artemis."),
                envelope, message);
            settle(message, envelope, () -> deadLetterRaw(message), "dead-letter a non-conformant message");
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
        return delivered > envelope.attempts() ? envelope.withAttempts(delivered) : envelope;
    }

    private void onUnknownUrn(Message message, Envelope envelope, String urn) throws JMSException {
        switch (unknownUrn) {
            case UnknownUrnStrategy.DELETE -> message.acknowledge();
            case UnknownUrnStrategy.DEAD_LETTER -> {
                settle(message, envelope, () -> deadLetter(envelope, message, "unknown_urn", null),
                    "dead-letter a message with an unmapped URN");
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
            settle(message, envelope, () -> deadLetter(envelope, message, "failed", error),
                "dead-letter a failed message");
        } else {
            message.acknowledge(); // terminal, no DLQ → degrade to drop
        }
    }

    /**
     * Run a DLQ write, then acknowledge. If the write fails the message is <b>not</b> acknowledged and the session is
     * {@code recover()}ed so the broker redelivers it (a later message's session-wide {@code acknowledge()} could
     * otherwise swallow it); the failure goes to {@code onError}. Recover first, report afterwards, so a throwing
     * {@code onError} cannot bypass the protection.
     */
    private void settle(Message message, Envelope envelope, DlqWrite write, String action) throws JMSException {
        try {
            write.run();
        } catch (JMSException | RuntimeException dlqFailure) {
            session.recover();
            report(new BabelQueueException("Could not " + action + "; not acknowledged, session recovered for redelivery.",
                dlqFailure), envelope, message);
            return;
        }
        message.acknowledge();
    }

    /** A DLQ write that may fail with a JMS or runtime exception. */
    @FunctionalInterface
    private interface DlqWrite {
        void run() throws JMSException;
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

    /**
     * The shared cross-SDK "blank" definition for {@code bq_schema_version}: the empty string, or a value made up
     * <b>only</b> of ASCII whitespace ({@code ' '}, {@code \t}, {@code \n}, U+000B, {@code \f}, {@code \r}). Anything else
     * (NBSP, U+001C–U+001F, U+0085, U+FEFF, …) is <b>not</b> blank, unlike {@link String#isBlank()}.
     */
    private static boolean isBlankSchemaVersion(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != ' ' && (c < '\t' || c > '\r')) {
                return false;
            }
        }
        return true;
    }

    private static String urn(Message message, Envelope envelope) {
        String type = JmsMessages.type(message);
        return type != null && !type.isEmpty() ? type : EnvelopeCodec.urn(envelope);
    }

    private static String originalQueue(Envelope envelope) {
        return envelope.meta() != null && envelope.meta().queue() != null ? envelope.meta().queue() : "";
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

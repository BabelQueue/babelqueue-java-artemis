package com.babelqueue.artemis;

import com.babelqueue.BabelQueueException;
import com.babelqueue.DeadLetters;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.JsonValues;
import com.babelqueue.PoisonRecord;
import com.babelqueue.UnknownUrnException;
import com.babelqueue.UnknownUrnStrategy;
import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
 * <p><b>Version gate (§7.7, ADR-0037 decision 1).</b> The {@code bq_schema_version} string property is checked
 * <b>before</b> the body is decoded: when it is present, not blank (empty or ASCII-whitespace-only) and not exactly
 * {@link EnvelopeCodec#SCHEMA_VERSION} (compared as sent, no trimming), the message is never decoded, the handler is
 * never called and nothing is retried, on its first delivery whatever {@code JMSXDeliveryCount} says. {@code onError}
 * is notified (with a {@code null} envelope). Under the {@code delete} {@link Builder#unknownUrn strategy} it is
 * acknowledged. Otherwise, with a {@link Builder#deadLetterQueue dead-letter queue}, the body is forwarded to the DLQ
 * <b>exactly as received</b> (no {@code dead_letter} block) and the message acknowledged — but only after the DLQ write
 * succeeded; if it fails, the session is {@code recover()}ed (the message is redelivered and rejected again, never
 * lost, never handled — a later message's session-wide {@code acknowledge()} cannot swallow it) and the failure goes
 * to {@code onError}. With no dead-letter queue it is acknowledged and the drop is logged. An absent or blank property
 * changes nothing, and the post-decode {@link EnvelopeCodec#accepts} check still applies.
 *
 * <p><b>Poison (ADR-0037 decision 2).</b> A body that is not a valid envelope (not JSON, no valid identity, an
 * unsupported {@code meta.schema_version}) is never handed to a handler and never retried. Under {@code delete} it is
 * acknowledged; with a dead-letter queue the ADR-0035 record ({@link PoisonRecord}, {@code reason: "poison"},
 * {@code original_queue} = the consumed queue) is written to it and the message acknowledged only after the write
 * succeeded (same recover-on-failure rule as above); with no dead-letter queue it is acknowledged and the drop is
 * logged.
 *
 * <p>The same acknowledge-only-after-the-write rule holds for every other DLQ write (an unmapped URN under
 * {@code dead_letter}, a handler that exhausted max-tries): otherwise the session is {@code recover()}ed and the
 * failure goes to {@code onError}.
 *
 * <p>Process one message per {@link #poll()} so the per-message {@code acknowledge()} and
 * {@code recover()} stay correct under the session-wide ack model. Not thread-safe — a JMS
 * session is single-threaded; run one per thread.
 */
public final class ArtemisConsumer {

    private static final System.Logger LOGGER = System.getLogger(ArtemisConsumer.class.getName());

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
            if (UnknownUrnStrategy.DELETE.equals(unknownUrn)) {
                message.acknowledge();
                report(rejected, null, message);
                return;
            }
            if (!deadLetterEnabled()) {
                LOGGER.log(System.Logger.Level.WARNING,
                    "BabelQueue Artemis: dropped a version-gated message (no dead-letter queue configured): {0}",
                    rejected.getMessage());
                message.acknowledge();
                report(rejected, null, message);
                return;
            }
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

        // Strict UTF-8 over the exact body bytes (a TextMessage's text or a BytesMessage's content): a body that is not
        // valid UTF-8 is never decoded leniently, it is poison (ADR-0037 decision 2).
        String text = JmsMessages.strictUtf8(JmsMessages.bodyBytes(message));
        Envelope envelope = reconcile(EnvelopeCodec.decode(text == null ? "" : text), message);

        if (!EnvelopeCodec.accepts(envelope)) {
            BabelQueueException cause = new BabelQueueException(
                "Rejected a non-conformant BabelQueue envelope from Artemis.");
            report(cause, envelope, message);
            settlePoison(message, envelope, cause);
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

    /**
     * Settles a poison body (ADR-0037 decision 2): acknowledged under {@code delete}, written to the DLQ as the
     * ADR-0035 record and acknowledged after the write, or acknowledged and logged when no DLQ is configured.
     */
    private void settlePoison(Message message, Envelope envelope, Throwable cause) throws JMSException {
        if (UnknownUrnStrategy.DELETE.equals(unknownUrn)) {
            message.acknowledge();
            return;
        }
        if (!deadLetterEnabled()) {
            LOGGER.log(System.Logger.Level.WARNING,
                "BabelQueue Artemis: dropped a poison message (no dead-letter queue configured): {0}",
                cause.getMessage());
            message.acknowledge();
            return;
        }
        settle(message, envelope, () -> deadLetterPoison(message, cause), "dead-letter a poison message");
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

    /** Whether dead-lettering is on: a DLQ producer and queue name are configured. */
    private boolean deadLetterEnabled() {
        return dlqQueue != null && dlqProducer != null;
    }

    /** Writes the ADR-0035 record of a poison body to the DLQ. */
    private void deadLetterPoison(Message message, Throwable cause) throws JMSException {
        String record = poisonRecord(JmsMessages.bodyBytes(message),
            consumedQueue(message), Math.max(JmsMessages.deliveryCount(message) - 1, 0), cause);
        dlqProducer.send(session.createQueue(dlqQueue), session.createTextMessage(record));
    }

    /**
     * The ADR-0035 DLQ record of a poison {@code body}: the one builder this consumer writes with (every conformance
     * {@code poison_dlq} case reaches it through {@link #poll()}, invalid UTF-8 as a {@code BytesMessage}).
     */
    static String poisonRecord(byte[] body, String originalQueue, int attempts, Throwable cause) {
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("reason", "poison");
        block.put("error", cause.getMessage());
        block.put("exception", cause.getClass().getName());
        block.put("failed_at", System.currentTimeMillis());
        block.put("original_queue", originalQueue);
        block.put("attempts", attempts);
        block.put("lang", EnvelopeCodec.SOURCE_LANG);
        return PoisonRecord.build(body, JsonValues.encode(block));
    }

    /** The consumed queue's name: the message's JMS destination, else the DLQ name without its {@code .dlq} suffix. */
    private String consumedQueue(Message message) {
        try {
            if (message.getJMSDestination() instanceof Queue queue && queue.getQueueName() != null) {
                return queue.getQueueName();
            }
        } catch (JMSException unreadable) {
            // fall through to the DLQ-derived name
        }
        return dlqQueue != null && dlqQueue.endsWith(".dlq")
            ? dlqQueue.substring(0, dlqQueue.length() - ".dlq".length())
            : "";
    }

    /** Forward a version-gated message's raw body to the DLQ, exactly as received (no envelope to annotate). */
    private void deadLetterRaw(Message message) throws JMSException {
        if (!deadLetterEnabled()) {
            return;
        }
        byte[] body = JmsMessages.bodyBytes(message);
        Message forwarded;
        if (message instanceof BytesMessage) {
            // Keep the message kind: the bytes (valid UTF-8 or not) are forwarded exactly as received.
            BytesMessage bytes = session.createBytesMessage();
            bytes.writeBytes(body);
            forwarded = bytes;
        } else {
            forwarded = session.createTextMessage(new String(body, StandardCharsets.UTF_8));
        }
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

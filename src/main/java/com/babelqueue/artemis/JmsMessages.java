package com.babelqueue.artemis;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Projects the envelope's contract fields onto a native JMS message ({@code JMSType} = URN,
 * {@code JMSCorrelationID} = {@code trace_id}, plus the {@code bq_} string properties) and reads
 * the native metadata back. The body stays the canonical envelope (Contract §7.2–§7.3).
 *
 * <p>The {@code bq_} property names use <strong>underscores</strong>, not hyphens: a JMS property
 * name must be a valid Java identifier (no {@code -}), and every Artemis SDK uses the same
 * JMS-legal form for cross-protocol parity.
 */
final class JmsMessages {

    static final String SCHEMA_VERSION = "bq_schema_version";
    static final String SOURCE_LANG = "bq_source_lang";
    static final String ATTEMPTS = "bq_attempts";
    static final String APP_ID = "bq_app_id";
    static final String DELAY = "bq_delay";
    static final String DELIVERY_COUNT = "JMSXDeliveryCount";
    static final String APP_ID_VALUE = "babelqueue";

    private JmsMessages() {}

    /** Build a JMS {@link TextMessage} carrying the envelope, with the §7 native projection. */
    static TextMessage project(Session session, Envelope envelope) throws JMSException {
        TextMessage message = session.createTextMessage(EnvelopeCodec.encode(envelope));
        if (notBlank(envelope.job())) {
            message.setJMSType(envelope.job());
        }
        if (notBlank(envelope.traceId())) {
            message.setJMSCorrelationID(envelope.traceId());
        }
        if (envelope.meta() != null) {
            message.setStringProperty(SCHEMA_VERSION, Integer.toString(envelope.meta().schemaVersion()));
            if (notBlank(envelope.meta().lang())) {
                message.setStringProperty(SOURCE_LANG, envelope.meta().lang());
            }
        }
        message.setStringProperty(ATTEMPTS, Integer.toString(envelope.attempts()));
        message.setStringProperty(APP_ID, APP_ID_VALUE);
        return message;
    }

    /**
     * The exact body bytes of a received message: the UTF-8 of a {@link TextMessage} text (empty when it has none) or
     * the content of a {@link BytesMessage} (what an AMQP 1.0 producer's Data section arrives as); empty for any other
     * message type. Nothing is decoded or substituted, so a body that is not valid UTF-8 keeps its bytes.
     */
    static byte[] bodyBytes(Message message) throws JMSException {
        if (message instanceof TextMessage textMessage) {
            String text = textMessage.getText();
            return text == null ? new byte[0] : text.getBytes(StandardCharsets.UTF_8);
        }
        if (message instanceof BytesMessage bytesMessage) {
            bytesMessage.reset();
            try {
                long length = bytesMessage.getBodyLength();
                if (length > Integer.MAX_VALUE - 8) {
                    throw new JMSException("BytesMessage body too large to read: " + length + " bytes");
                }
                byte[] body = new byte[(int) length];
                int read = 0;
                while (read < body.length) {
                    byte[] chunk = new byte[body.length - read];
                    int n = bytesMessage.readBytes(chunk);
                    if (n <= 0) {
                        break;
                    }
                    System.arraycopy(chunk, 0, body, read, n);
                    read += n;
                }
                return read == body.length ? body : java.util.Arrays.copyOf(body, read);
            } finally {
                // Leave the read position at the start, so a handler given the raw message can read it too.
                bytesMessage.reset();
            }
        }
        return new byte[0];
    }

    /** The bytes as text, or {@code null} when they are not valid UTF-8 (never U+FFFD substitution). */
    static String strictUtf8(byte[] body) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(body)).toString();
        } catch (CharacterCodingException invalid) {
            return null;
        }
    }

    /** The {@code JMSType} (the URN), or {@code null} if unset / unreadable. */
    static String type(Message message) {
        try {
            return message.getJMSType();
        } catch (JMSException e) {
            return null;
        }
    }

    /**
     * The broker-maintained {@code JMSXDeliveryCount} (1-based), or 0 if absent — so a
     * first delivery (count 1) reconciles to {@code attempts = 0} and a missing count
     * (externally produced) falls back to the body.
     */
    static int deliveryCount(Message message) {
        try {
            if (message.propertyExists(DELIVERY_COUNT)) {
                return message.getIntProperty(DELIVERY_COUNT);
            }
        } catch (JMSException e) {
            return 0;
        }
        return 0;
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isEmpty();
    }
}

package com.babelqueue.artemis;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

/**
 * Projects the envelope's contract fields onto a native JMS message ({@code JMSType} = URN,
 * {@code JMSCorrelationID} = {@code trace_id}, plus the {@code bq-} string properties) and reads
 * the native metadata back. The body stays the canonical envelope (Contract §7.2–§7.3).
 */
final class JmsMessages {

    static final String SCHEMA_VERSION = "bq-schema-version";
    static final String SOURCE_LANG = "bq-source-lang";
    static final String ATTEMPTS = "bq-attempts";
    static final String APP_ID = "bq-app-id";
    static final String DELAY = "bq-delay";
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

    /** The body of a {@link TextMessage}, or empty for any other message type. */
    static String text(Message message) throws JMSException {
        return message instanceof TextMessage textMessage ? textMessage.getText() : "";
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

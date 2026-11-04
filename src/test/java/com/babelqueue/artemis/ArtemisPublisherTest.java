package com.babelqueue.artemis;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** §7 produce: body = envelope, JMSType = URN, JMSCorrelationID = trace_id, bq- properties; native delay. */
class ArtemisPublisherTest {

    private static final String URN = "urn:babel:orders:created";

    private static Session sessionReturning(TextMessage message) throws Exception {
        Session session = mock(Session.class);
        when(session.createTextMessage(anyString())).thenReturn(message);
        return session;
    }

    private static MessageProducer producerOn(String queueName) throws Exception {
        MessageProducer producer = mock(MessageProducer.class);
        Queue queue = mock(Queue.class);
        when(queue.getQueueName()).thenReturn(queueName);
        when(producer.getDestination()).thenReturn(queue);
        return producer;
    }

    @Test
    void publishProjectsBodyTypeCorrelationAndProperties() throws Exception {
        TextMessage message = mock(TextMessage.class);
        Session session = sessionReturning(message);
        MessageProducer producer = producerOn("orders");

        String id = ArtemisPublisher.create(session, producer)
            .publish(URN, Map.of("order_id", 7), "trace-1");

        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        verify(session).createTextMessage(body.capture());
        Envelope decoded = EnvelopeCodec.decode(body.getValue());
        assertEquals(URN, decoded.job());
        assertEquals(id, decoded.meta().id());
        assertEquals("orders", decoded.meta().queue());

        verify(message).setJMSType(URN);
        verify(message).setJMSCorrelationID("trace-1");
        verify(message).setStringProperty("bq_schema_version", "1");
        verify(message).setStringProperty("bq_source_lang", decoded.meta().lang());
        verify(message).setStringProperty("bq_attempts", "0");
        verify(message).setStringProperty("bq_app_id", "babelqueue");
        verify(producer).send(message);
    }

    @Test
    void publishWithDelayUsesNativeDeliveryDelay() throws Exception {
        TextMessage message = mock(TextMessage.class);
        Session session = sessionReturning(message);
        MessageProducer producer = producerOn("orders");

        ArtemisPublisher.create(session, producer).publish(URN, Map.of(), null, Duration.ofSeconds(30));

        verify(producer).setDeliveryDelay(30000L);
        verify(message).setStringProperty("bq_delay", "30000");
        verify(producer).send(message);
        verify(producer).setDeliveryDelay(0L);
    }

    @Test
    void publishWhenDestinationUnavailableUsesEmptyQueue() throws Exception {
        TextMessage message = mock(TextMessage.class);
        Session session = sessionReturning(message);
        MessageProducer producer = mock(MessageProducer.class);
        when(producer.getDestination()).thenThrow(new jakarta.jms.JMSException("no destination"));

        ArtemisPublisher.create(session, producer).publish(URN, Map.of());

        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        verify(session).createTextMessage(body.capture());
        assertEquals("", EnvelopeCodec.decode(body.getValue()).meta().queue());
    }

    @Test
    void publishWithoutTraceMintsAFreshTrace() throws Exception {
        TextMessage message = mock(TextMessage.class);
        Session session = sessionReturning(message);

        ArtemisPublisher.create(session, producerOn("orders")).publish(URN, Map.of());

        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        verify(session).createTextMessage(body.capture());
        org.junit.jupiter.api.Assertions.assertFalse(
            EnvelopeCodec.decode(body.getValue()).traceId().isEmpty());
    }

    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    /** A TextMessage mock backed by a real property store, so what is set can be read back (round-trip). */
    static TextMessage storingMessage(Map<String, Object> store) throws Exception {
        TextMessage message = mock(TextMessage.class);
        doAnswer(inv -> store.put(inv.getArgument(0), inv.getArgument(1)))
            .when(message).setStringProperty(anyString(), anyString());
        when(message.propertyExists(anyString())).thenAnswer(inv -> store.containsKey((String) inv.getArgument(0)));
        when(message.getObjectProperty(anyString())).thenAnswer(inv -> store.get((String) inv.getArgument(0)));
        when(message.getPropertyNames()).thenAnswer(inv -> Collections.enumeration(store.keySet()));
        return message;
    }

    @Test
    void publishWithHeadersCarriesTraceparentBesideContractPropertiesAndNotInTheBody() throws Exception {
        Map<String, Object> store = new LinkedHashMap<>();
        TextMessage message = storingMessage(store);
        Session session = sessionReturning(message);
        Envelope env = EnvelopeCodec.make(URN, Map.of("order_id", 7), "orders", "trace-1");

        String id = ArtemisPublisher.create(session, producerOn("orders"))
            .publishWithHeaders(env, Map.of("traceparent", TRACEPARENT, "tracestate", "vendor=1"));

        assertEquals(env.meta().id(), id);
        assertEquals(TRACEPARENT, store.get("traceparent"));
        assertEquals("vendor=1", store.get("tracestate"));
        assertEquals("1", store.get("bq_schema_version")); // contract projection intact
        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        verify(session).createTextMessage(body.capture());
        assertFalse(body.getValue().contains("traceparent"));
        assertFalse(body.getValue().contains(TRACEPARENT));
        assertEquals(EnvelopeCodec.encode(env), body.getValue());
    }

    @Test
    void headerlessPublishWithHeadersSetsExactlyTheContractProperties() throws Exception {
        Map<String, Object> plain = new LinkedHashMap<>();
        Map<String, Object> withNull = new LinkedHashMap<>();
        Map<String, Object> withEmpty = new LinkedHashMap<>();
        Envelope env = EnvelopeCodec.make(URN, Map.of(), "orders", "trace-1");

        ArtemisPublisher.create(sessionReturning(storingMessage(plain)), producerOn("orders"))
            .publishWithHeaders(env, null);
        ArtemisPublisher.create(sessionReturning(storingMessage(withNull)), producerOn("orders"))
            .publishWithHeaders(env, Map.of());
        ArtemisPublisher.create(sessionReturning(storingMessage(withEmpty)), producerOn("orders"))
            .publishWithHeaders(env, Map.of("traceparent", ""));

        assertEquals(plain, withNull);
        assertEquals(plain, withEmpty);
        assertFalse(plain.containsKey("traceparent"));
    }

    @Test
    void contractPropertyWinsACollisionAndIllegalJmsNamesAreSkipped() throws Exception {
        Map<String, Object> store = new LinkedHashMap<>();
        Envelope env = EnvelopeCodec.make(URN, Map.of(), "orders", "trace-1");

        ArtemisPublisher.create(sessionReturning(storingMessage(store)), producerOn("orders"))
            .publishWithHeaders(env, Map.of(
                "bq_attempts", "99", "x-hyphen", "v", "JMSXGroupID", "g", "JMSType", "t", "1bad", "v",
                "traceparent", TRACEPARENT));

        assertEquals("0", store.get("bq_attempts"));
        assertFalse(store.containsKey("x-hyphen"));
        assertFalse(store.containsKey("JMSXGroupID"));
        assertFalse(store.containsKey("JMSType"));
        assertFalse(store.containsKey("1bad"));
        assertEquals(TRACEPARENT, store.get("traceparent"));
    }

    @Test
    void publishOverloadWithHeadersRidesTheDelayedMessageToo() throws Exception {
        Map<String, Object> store = new LinkedHashMap<>();
        MessageProducer producer = producerOn("orders");

        ArtemisPublisher.create(sessionReturning(storingMessage(store)), producer)
            .publish(URN, Map.of(), null, Duration.ofSeconds(30), Map.of("traceparent", TRACEPARENT));

        assertEquals(TRACEPARENT, store.get("traceparent"));
        assertEquals("30000", store.get("bq_delay"));
        verify(producer).setDeliveryDelay(30000L);
    }

    @Test
    void publishWithHeadersMatchesTheOtelHeaderSenderContract() throws Exception {
        Map<String, Object> store = new LinkedHashMap<>();
        ArtemisPublisher publisher = ArtemisPublisher.create(
            sessionReturning(storingMessage(store)), producerOn("orders"));
        com.babelqueue.otel.HeaderSender sender = publisher::publishWithHeaders;

        assertDoesNotThrow(() -> sender.send(EnvelopeCodec.make(URN, Map.of(), "orders", "t"),
            Map.of("traceparent", TRACEPARENT)));

        assertEquals(TRACEPARENT, store.get("traceparent"));
    }
}

package com.babelqueue.artemis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
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
        verify(message).setStringProperty("bq-schema-version", "1");
        verify(message).setStringProperty("bq-source-lang", decoded.meta().lang());
        verify(message).setStringProperty("bq-attempts", "0");
        verify(message).setStringProperty("bq-app-id", "babelqueue");
        verify(producer).send(message);
    }

    @Test
    void publishWithDelayUsesNativeDeliveryDelay() throws Exception {
        TextMessage message = mock(TextMessage.class);
        Session session = sessionReturning(message);
        MessageProducer producer = producerOn("orders");

        ArtemisPublisher.create(session, producer).publish(URN, Map.of(), null, Duration.ofSeconds(30));

        verify(producer).setDeliveryDelay(30000L);
        verify(message).setStringProperty("bq-delay", "30000");
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
}

package com.babelqueue.artemis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import com.babelqueue.UnknownUrnStrategy;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** §7 consume: CLIENT_ACKNOWLEDGE, route by JMSType, attempts = max(body, JMSXDeliveryCount−1), recover/DLQ. */
class ArtemisConsumerTest {

    private static final String URN = "urn:babel:orders:created";

    private static String body(int attempts) {
        Envelope base = EnvelopeCodec.make(URN, Map.of("order_id", 1), "orders", "trace-1");
        Envelope env = new Envelope(base.job(), base.traceId(), base.data(), base.meta(), attempts, base.deadLetter());
        return EnvelopeCodec.encode(env);
    }

    private static TextMessage incoming(String text, String jmsType, int deliveryCount) throws Exception {
        TextMessage message = mock(TextMessage.class);
        when(message.getText()).thenReturn(text);
        when(message.getJMSType()).thenReturn(jmsType);
        if (deliveryCount > 0) {
            when(message.propertyExists("JMSXDeliveryCount")).thenReturn(true);
            when(message.getIntProperty("JMSXDeliveryCount")).thenReturn(deliveryCount);
        }
        return message;
    }

    private static MessageConsumer consumerWith(TextMessage message) throws Exception {
        MessageConsumer consumer = mock(MessageConsumer.class);
        when(consumer.receive(anyLong())).thenReturn(message);
        return consumer;
    }

    @Test
    void successAcknowledges() throws Exception {
        TextMessage message = incoming(body(0), URN, 1);
        MessageConsumer consumer = consumerWith(message);
        int[] seen = {-1};

        int count = ArtemisConsumer.builder(consumer, mock(Session.class))
            .handler(URN, (env, m) -> seen[0] = env.attempts())
            .build()
            .poll();

        assertEquals(1, count);
        assertEquals(0, seen[0]);
        verify(message).acknowledge();
    }

    @Test
    void attemptsIsDeliveryCountMinusOne() throws Exception {
        TextMessage message = incoming(body(0), URN, 3);
        int[] seen = {-1};
        ArtemisConsumer.builder(consumerWith(message), mock(Session.class))
            .handler(URN, (env, m) -> seen[0] = env.attempts())
            .build()
            .poll();
        assertEquals(2, seen[0]);
    }

    @Test
    void bodyCountNeverLoweredByDeliveryCount() throws Exception {
        TextMessage message = incoming(body(5), URN, 2); // native 1 < body 5 → body wins
        int[] seen = {-1};
        ArtemisConsumer.builder(consumerWith(message), mock(Session.class))
            .handler(URN, (env, m) -> seen[0] = env.attempts())
            .build()
            .poll();
        assertEquals(5, seen[0]);
    }

    @Test
    void throwingHandlerRecoversForRedelivery() throws Exception {
        TextMessage message = incoming(body(0), URN, 1);
        MessageConsumer consumer = consumerWith(message);
        Session session = mock(Session.class);
        Throwable[] reported = {null};

        ArtemisConsumer.builder(consumer, session)
            .handler(URN, (env, m) -> { throw new IllegalStateException("boom"); })
            .maxTries(3)
            .onError((e, env, m) -> reported[0] = e)
            .build()
            .poll();

        assertInstanceOf(IllegalStateException.class, reported[0]);
        verify(session).recover();
        verify(message, never()).acknowledge();
    }

    @Test
    void terminalFailureGoesToDlqWithDeadLetterBlock() throws Exception {
        TextMessage message = incoming(body(2), URN, 3); // attempts 2, nextAttempt 3 == maxTries
        MessageConsumer consumer = consumerWith(message);
        Session session = mock(Session.class);
        ArgumentCaptor<String> dlqBody = ArgumentCaptor.forClass(String.class);
        when(session.createTextMessage(dlqBody.capture())).thenReturn(mock(TextMessage.class));
        when(session.createQueue("orders.dlq")).thenReturn(mock(Queue.class));
        MessageProducer dlqProducer = mock(MessageProducer.class);

        ArtemisConsumer.builder(consumer, session)
            .handler(URN, (env, m) -> { throw new IllegalStateException("boom"); })
            .deadLetterQueue(dlqProducer, "orders.dlq")
            .maxTries(3)
            .build()
            .poll();

        verify(dlqProducer).send(any(Queue.class), any(Message.class));
        verify(message).acknowledge();
        Envelope dead = EnvelopeCodec.decode(dlqBody.getValue());
        assertEquals("failed", dead.deadLetter().reason());
    }

    @Test
    void unknownUrnFailRaisesAndDoesNotAcknowledge() throws Exception {
        TextMessage message = incoming(body(0), URN, 1);
        MessageConsumer consumer = consumerWith(message);
        ArtemisConsumer worker = ArtemisConsumer.builder(consumer, mock(Session.class)).build();

        assertThrows(UnknownUrnException.class, worker::poll);
        verify(message, never()).acknowledge();
    }

    @Test
    void unknownUrnDeleteAcknowledges() throws Exception {
        TextMessage message = incoming(body(0), URN, 1);
        ArtemisConsumer.builder(consumerWith(message), mock(Session.class))
            .unknownUrn(UnknownUrnStrategy.DELETE)
            .build()
            .poll();
        verify(message).acknowledge();
    }

    @Test
    void unknownUrnReleaseRecovers() throws Exception {
        TextMessage message = incoming(body(0), URN, 1);
        Session session = mock(Session.class);
        ArtemisConsumer.builder(consumerWith(message), session)
            .unknownUrn(UnknownUrnStrategy.RELEASE)
            .build()
            .poll();
        verify(session).recover();
    }

    @Test
    void nonConformantForwardedRawToDlq() throws Exception {
        TextMessage message = incoming("not-json", null, 1);
        MessageConsumer consumer = consumerWith(message);
        Session session = mock(Session.class);
        when(session.createTextMessage(anyString())).thenReturn(mock(TextMessage.class));
        when(session.createQueue("orders.dlq")).thenReturn(mock(Queue.class));
        MessageProducer dlqProducer = mock(MessageProducer.class);
        Throwable[] reported = {null};

        ArtemisConsumer.builder(consumer, session)
            .deadLetterQueue(dlqProducer, "orders.dlq")
            .onError((e, env, m) -> reported[0] = e)
            .build()
            .poll();

        verify(dlqProducer).send(any(Queue.class), any(Message.class));
        verify(message).acknowledge();
        org.junit.jupiter.api.Assertions.assertNotNull(reported[0]);
    }

    @Test
    void unknownUrnDeadLetterGoesToDlq() throws Exception {
        TextMessage message = incoming(body(0), URN, 1);
        Session session = mock(Session.class);
        ArgumentCaptor<String> dlqBody = ArgumentCaptor.forClass(String.class);
        when(session.createTextMessage(dlqBody.capture())).thenReturn(mock(TextMessage.class));
        when(session.createQueue("orders.dlq")).thenReturn(mock(Queue.class));
        MessageProducer dlqProducer = mock(MessageProducer.class);

        ArtemisConsumer.builder(consumerWith(message), session)
            .deadLetterQueue(dlqProducer, "orders.dlq")
            .unknownUrn(UnknownUrnStrategy.DEAD_LETTER)
            .build()
            .poll();

        verify(dlqProducer).send(any(Queue.class), any(Message.class));
        verify(message).acknowledge();
        assertEquals("unknown_urn", EnvelopeCodec.decode(dlqBody.getValue()).deadLetter().reason());
    }

    @Test
    void terminalFailureWithoutDlqDropsAndAcknowledges() throws Exception {
        TextMessage message = incoming(body(2), URN, 3);
        Session session = mock(Session.class);

        ArtemisConsumer.builder(consumerWith(message), session)
            .handler(URN, (env, m) -> { throw new IllegalStateException("boom"); })
            .maxTries(3)
            .build()
            .poll();

        verify(message).acknowledge();
        verify(session, never()).recover();
    }

    @Test
    void handlersMapAndReceiveTimeoutAreApplied() throws Exception {
        TextMessage message = incoming(body(0), URN, 1);
        int[] seen = {-1};
        ArtemisConsumer.builder(consumerWith(message), mock(Session.class))
            .handlers(Map.of(URN, (BabelHandler) (env, m) -> seen[0] = env.attempts()))
            .receiveTimeout(250)
            .build()
            .poll();
        assertEquals(0, seen[0]);
    }

    @Test
    void runReportsReceiveErrorThenStops() throws Exception {
        MessageConsumer consumer = mock(MessageConsumer.class);
        when(consumer.receive(anyLong())).thenThrow(new jakarta.jms.JMSException("receive failed"));
        Throwable[] reported = {null};
        boolean[] first = {true};

        ArtemisConsumer.builder(consumer, mock(Session.class))
            .onError((e, env, m) -> reported[0] = e)
            .build()
            .run(() -> {
                if (first[0]) {
                    first[0] = false;
                    return true;
                }
                return false;
            });

        assertInstanceOf(jakarta.jms.JMSException.class, reported[0]);
    }

    @Test
    void pollReturnsZeroOnTimeout() throws Exception {
        MessageConsumer consumer = mock(MessageConsumer.class);
        when(consumer.receive(anyLong())).thenReturn(null);
        assertEquals(0, ArtemisConsumer.builder(consumer, mock(Session.class)).build().poll());
    }

    @Test
    void runStopsWhenSupplierIsFalse() throws Exception {
        MessageConsumer consumer = mock(MessageConsumer.class);
        ArtemisConsumer.builder(consumer, mock(Session.class)).build().run(() -> false);
        verify(consumer, never()).receive(anyLong());
    }
}

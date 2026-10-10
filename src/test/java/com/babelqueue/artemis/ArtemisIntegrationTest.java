package com.babelqueue.artemis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Live-broker integration tests for the Artemis transport (§7, JMS) — opt-in.
 *
 * <p>Skipped in the normal suite. Enable with {@code BQ_INTEGRATION=1}; point at a broker with
 * {@code BQ_ARTEMIS_URL} (default {@code tcp://localhost:61616}), {@code BQ_ARTEMIS_USER} and
 * {@code BQ_ARTEMIS_PASSWORD} (both default {@code artemis}). The CI {@code integration} job runs an
 * {@code apache/activemq-artemis} service container. Every test uses unique queue names.
 *
 * <p>The DLQ receiver is always attached <b>before</b> anything is published, so the test never depends on the
 * broker's address routing type for messages sent to a queue that has no consumer yet.
 */
@EnabledIfEnvironmentVariable(named = "BQ_INTEGRATION", matches = "1")
class ArtemisIntegrationTest {

    private static final String URN = "urn:babel:orders:created";
    private static final Duration DEADLINE = Duration.ofSeconds(60);

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static Connection connect() throws JMSException {
        ActiveMQJMSConnectionFactory factory = new ActiveMQJMSConnectionFactory(
            env("BQ_ARTEMIS_URL", "tcp://localhost:61616"),
            env("BQ_ARTEMIS_USER", "artemis"),
            env("BQ_ARTEMIS_PASSWORD", "artemis"));
        Connection connection = factory.createConnection();
        connection.start();
        return connection;
    }

    private static TextMessage awaitDlq(MessageConsumer dlqConsumer) throws JMSException {
        long stop = System.nanoTime() + DEADLINE.toNanos();
        while (System.nanoTime() < stop) {
            Message message = dlqConsumer.receive(500);
            if (message instanceof TextMessage text) {
                return text;
            }
        }
        throw new AssertionError("no message reached the DLQ within " + DEADLINE);
    }

    @Test
    void handlerErrorRedeliversThenDeadLettersAfterMaxTries() throws Exception {
        String queue = "bq.it.orders." + UUID.randomUUID().toString().substring(0, 8);
        String dlq = queue + ".dlq";
        List<Integer> deliveryCounts = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();

        try (Connection connection = connect()) {
            Session dlqSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer dlqConsumer = dlqSession.createConsumer(dlqSession.createQueue(dlq));

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queue));
            MessageProducer dlqProducer = session.createProducer(null);
            ArtemisConsumer worker = ArtemisConsumer.builder(consumer, session)
                .deadLetterQueue(dlqProducer, dlq)
                .maxTries(3)
                .receiveTimeout(500)
                .handler(URN, (envelope, message) -> {
                    calls.incrementAndGet();
                    deliveryCounts.add(message.getIntProperty(JmsMessages.DELIVERY_COUNT));
                    throw new IllegalStateException("boom");
                })
                .build();

            Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ArtemisPublisher.create(producerSession, producerSession.createProducer(producerSession.createQueue(queue)))
                .publish(URN, Map.of("order_id", 7), "trace-it-1");

            long stop = System.nanoTime() + DEADLINE.toNanos();
            while (calls.get() < 3 && System.nanoTime() < stop) {
                worker.poll();
            }
            assertEquals(3, calls.get(), "handler must run once per attempt up to maxTries");

            TextMessage dead = awaitDlq(dlqConsumer);
            Envelope envelope = EnvelopeCodec.decode(dead.getText());
            assertNotNull(envelope.deadLetter(), "DLQ envelope must carry a dead_letter block");
            assertEquals("failed", envelope.deadLetter().reason());
            assertEquals("trace-it-1", envelope.traceId());
            assertEquals(URN, dead.getJMSType());
        }

        // Scenario 3: every handler failure is a broker redelivery, so JMSXDeliveryCount strictly increases.
        assertEquals(3, deliveryCounts.size());
        for (int i = 1; i < deliveryCounts.size(); i++) {
            assertTrue(deliveryCounts.get(i) > deliveryCounts.get(i - 1),
                "JMSXDeliveryCount must increase on each redelivery: " + deliveryCounts);
        }
        assertTrue(deliveryCounts.get(deliveryCounts.size() - 1) >= 3,
            "third delivery must report a delivery count of at least 3: " + deliveryCounts);
    }

    @Test
    void schemaVersionGateRejectionForwardsRawBodyToDlq() throws Exception {
        String queue = "bq.it.gate." + UUID.randomUUID().toString().substring(0, 8);
        String dlq = queue + ".dlq";
        // Deliberately not a valid v1 envelope: the gate must reject on the property alone (§7.7).
        String body = "{\"job\":\"urn:babel:orders:created\",\"data\":{},\"future\":true}";
        AtomicInteger handled = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        try (Connection connection = connect()) {
            Session dlqSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer dlqConsumer = dlqSession.createConsumer(dlqSession.createQueue(dlq));

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queue));
            ArtemisConsumer worker = ArtemisConsumer.builder(consumer, session)
                .deadLetterQueue(session.createProducer(null), dlq)
                .receiveTimeout(500)
                .onError((error, envelope, message) -> rejected.incrementAndGet())
                .handler(URN, (envelope, message) -> handled.incrementAndGet())
                .build();

            Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TextMessage seed = producerSession.createTextMessage(body);
            seed.setJMSType(URN);
            seed.setStringProperty(JmsMessages.SCHEMA_VERSION, "2");
            producerSession.createProducer(producerSession.createQueue(queue)).send(seed);

            long stop = System.nanoTime() + DEADLINE.toNanos();
            while (rejected.get() == 0 && System.nanoTime() < stop) {
                worker.poll();
            }
            assertTrue(rejected.get() >= 1, "the gate must report the rejection");

            TextMessage dead = awaitDlq(dlqConsumer);
            assertEquals(body, dead.getText(), "the raw body must reach the DLQ unmodified");
            assertEquals(0, handled.get(), "a gate-rejected message must never reach a handler");
        }
    }
}

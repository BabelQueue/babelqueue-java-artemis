package com.babelqueue.artemis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Apache ActiveMQ Artemis binding conformance against the vendored canonical suite's
 * {@code artemis} block: the §7 JMS projection (JMSType / JMSCorrelationID + the {@code bq-}
 * string properties) and the {@code attempts = max(body, JMSXDeliveryCount − 1)} reconciliation, plus the
 * ADR-0037 {@code version_gate_artemis} settle rule and the ADR-0035 {@code poison_dlq} record shape driven through
 * this consumer.
 * The conformance {@code delivery_count} is the 0-based AMQP counter; the JMS binding reads the
 * 1-based {@code JMSXDeliveryCount}, so the test sets it to {@code delivery_count + 1}. JMS is
 * mocked with Mockito — no Artemis, no network.
 */
class ArtemisConformanceTest {

    private static final String URN = "urn:babel:orders:created";

    private static String resource(String path) throws Exception {
        try (InputStream in = ArtemisConformanceTest.class.getResourceAsStream("/conformance/" + path)) {
            if (in == null) {
                throw new IllegalStateException("vendored conformance resource missing: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static JSONObject artemisBlock() throws Exception {
        return new JSONObject(resource("manifest.json")).getJSONObject("artemis");
    }

    @Test
    void propertyProjectionMatchesGolden() throws Exception {
        JSONObject projection = artemisBlock().getJSONObject("property_projection");
        Envelope envelope = EnvelopeCodec.decode(resource(projection.getString("envelope_file")));

        TextMessage message = mock(TextMessage.class);
        Session session = mock(Session.class);
        when(session.createTextMessage(anyString())).thenReturn(message);

        JmsMessages.project(session, envelope);

        verify(message).setJMSType(projection.getString("jms_type"));
        verify(message).setJMSCorrelationID(projection.getString("correlation_id"));
        JSONObject want = projection.getJSONObject("properties");
        for (String key : want.keySet()) {
            verify(message).setStringProperty(key, want.getString(key));
        }
    }

    @Test
    void attemptsReconciliationMatchesGolden() throws Exception {
        JSONArray cases = artemisBlock().getJSONObject("attempts_reconciliation").getJSONArray("cases");
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            Envelope base = EnvelopeCodec.make(URN, Map.of("x", 1), "orders", null);
            Envelope env = new Envelope(
                base.job(), base.traceId(), base.data(), base.meta(),
                testCase.getInt("body_attempts"), base.deadLetter());

            // The conformance delivery_count is 0-based (AMQP); JMS reports the 1-based
            // JMSXDeliveryCount, so it is delivery_count + 1.
            int jmsxDeliveryCount = testCase.getInt("delivery_count") + 1;
            TextMessage message = mock(TextMessage.class);
            when(message.getText()).thenReturn(EnvelopeCodec.encode(env));
            when(message.getJMSType()).thenReturn(URN);
            when(message.propertyExists("JMSXDeliveryCount")).thenReturn(true);
            when(message.getIntProperty("JMSXDeliveryCount")).thenReturn(jmsxDeliveryCount);

            MessageConsumer consumer = mock(MessageConsumer.class);
            when(consumer.receive(anyLong())).thenReturn(message);

            int[] seen = {-1};
            ArtemisConsumer.builder(consumer, mock(Session.class))
                .handler(URN, (envelopeIn, messageIn) -> seen[0] = envelopeIn.attempts())
                .maxTries(99)
                .build()
                .poll();

            assertEquals(testCase.getInt("expected_attempts"), seen[0], testCase.getString("name"));
        }
    }

    @Test
    void schemaVersionGateMatchesGolden() throws Exception {
        JSONObject gate = artemisBlock().getJSONObject("schema_version_gate");
        String property = gate.getString("property");
        String fixture = resource(gate.getString("fixture"));
        JSONArray cases = gate.getJSONArray("cases");
        assertTrue(cases.length() > 0);

        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            boolean decode = "decode".equals(testCase.getString("expect"));
            boolean jsonNull = testCase.has("value") && testCase.isNull("value");
            String label = jsonNull ? "null" : testCase.has("value") ? "value=" + testCase.getString("value") : "absent";

            TextMessage message = mock(TextMessage.class);
            when(message.getText()).thenReturn(fixture);
            when(message.getJMSType()).thenReturn(URN);
            if (jsonNull) {
                // A JMS property set to null reads back as null from getStringProperty — the same as an
                // absent property (§7.7: null value = property absent). The property is stubbed as PRESENT
                // (propertyExists = true, getObjectProperty = null) so this case stays distinct from the
                // "absent" case, where Mockito's default propertyExists = false applies.
                when(message.propertyExists(property)).thenReturn(true);
                when(message.getObjectProperty(property)).thenReturn(null);
                when(message.getStringProperty(property)).thenReturn(null);
            } else if (testCase.has("value")) {
                when(message.getStringProperty(property)).thenReturn(testCase.getString("value"));
            }
            Session session = mock(Session.class);
            when(session.createTextMessage(anyString())).thenReturn(mock(TextMessage.class));
            when(session.createQueue("orders.dlq")).thenReturn(mock(Queue.class));
            MessageProducer dlqProducer = mock(MessageProducer.class);
            MessageConsumer consumer = mock(MessageConsumer.class);
            when(consumer.receive(anyLong())).thenReturn(message);

            int[] handled = {0};
            ArtemisConsumer.builder(consumer, session)
                .deadLetterQueue(dlqProducer, "orders.dlq")
                .handler(URN, (envelopeIn, messageIn) -> handled[0]++)
                .build()
                .poll();

            assertEquals(decode ? 1 : 0, handled[0], label);
            // Reject = poison path: raw body to the DLQ, then acknowledge. Decode = handler then acknowledge.
            verify(dlqProducer, times(decode ? 0 : 1)).send(
                any(Queue.class), any(jakarta.jms.Message.class));
            verify(message, times(1)).acknowledge();
            verify(session, never()).recover();
        }
    }
    // ---- poison_dlq (ADR-0035 shape, ADR-0037 decision 2) -----------------------------------------

    @Test
    void poisonDlqRecordShapeMatchesGolden() throws Exception {
        JSONArray cases = new JSONObject(resource("manifest.json")).getJSONObject("poison_dlq").getJSONArray("cases");
        assertTrue(cases.length() > 0);
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            String name = testCase.getString("name");
            // Every case, invalid UTF-8 included, goes through the consumer: a text body arrives as a TextMessage,
            // raw bytes (an AMQP 1.0 Data section) as a BytesMessage.
            jakarta.jms.Message message;
            if (testCase.has("body_base64")) {
                message = ArtemisConsumerTest.bytesMessage(PoisonRecordChecks.bodyOf(testCase));
            } else {
                TextMessage text = mock(TextMessage.class);
                when(text.getText()).thenReturn(testCase.getString("body"));
                message = text;
            }
            Session session = mock(Session.class);
            ArgumentCaptor<String> written = ArgumentCaptor.forClass(String.class);
            when(session.createTextMessage(written.capture())).thenReturn(mock(TextMessage.class));
            when(session.createQueue("orders.dlq")).thenReturn(mock(Queue.class));
            MessageProducer dlqProducer = mock(MessageProducer.class);
            MessageConsumer consumer = mock(MessageConsumer.class);
            when(consumer.receive(anyLong())).thenReturn(message);
            int[] handled = {0};

            ArtemisConsumer.builder(consumer, session)
                .deadLetterQueue(dlqProducer, "orders.dlq")
                .handler(URN, (envelopeIn, messageIn) -> handled[0]++)
                .build()
                .poll();

            assertEquals(0, handled[0], name);
            verify(dlqProducer, times(1)).send(any(Queue.class), any(jakarta.jms.Message.class));
            verify(message, times(1)).acknowledge();
            String record = written.getValue();
            PoisonRecordChecks.check(testCase, record);
        }
    }

    // ---- version_gate_artemis (ADR-0037 decision 1) -----------------------------------------------

    @Test
    void versionGateSettleRuleMatchesGolden() throws Exception {
        JSONObject section = new JSONObject(resource("manifest.json")).getJSONObject("version_gate_artemis");
        JSONArray cases = section.getJSONArray("cases");
        JSONArray bodies = section.getJSONArray("incoming_bodies");
        assertEquals(17, cases.length());
        for (int i = 0; i < cases.length(); i++) {
            for (int b = 0; b < bodies.length(); b++) {
                runVersionGateCase(cases.getJSONObject(i), bodies.getJSONObject(b).getString("body"));
            }
        }
    }

    private static void runVersionGateCase(JSONObject testCase, String body) throws Exception {
        String label = testCase.getString("name");
        boolean deadLetter = "on".equals(testCase.getString("dead_letter"));
        JSONObject expect = testCase.getJSONObject("expect");

        TextMessage message = mock(TextMessage.class);
        when(message.getText()).thenReturn(body);
        when(message.getJMSType()).thenReturn(URN);
        when(message.getStringProperty("bq_schema_version")).thenReturn("2");
        if (testCase.has("bq_attempts")) {
            when(message.propertyExists("bq_attempts")).thenReturn(true);
            when(message.getStringProperty("bq_attempts")).thenReturn(testCase.getString("bq_attempts"));
            when(message.propertyExists("JMSXDeliveryCount")).thenReturn(true);
            when(message.getIntProperty("JMSXDeliveryCount")).thenReturn(8);
        }
        Session session = mock(Session.class);
        ArgumentCaptor<String> written = ArgumentCaptor.forClass(String.class);
        when(session.createTextMessage(written.capture())).thenReturn(mock(TextMessage.class));
        when(session.createQueue("orders.dlq")).thenReturn(mock(Queue.class));
        MessageProducer dlqProducer = mock(MessageProducer.class);
        if ("fails".equals(testCase.optString("dlq_write", "ok"))) {
            doThrow(new JMSException("dlq write failed")).when(dlqProducer)
                .send(any(Queue.class), any(jakarta.jms.Message.class));
        }
        MessageConsumer consumer = mock(MessageConsumer.class);
        when(consumer.receive(anyLong())).thenReturn(message);
        int[] handled = {0};
        ArtemisConsumer.Builder builder = ArtemisConsumer.builder(consumer, session)
            .handler(URN, (envelopeIn, messageIn) -> handled[0]++);
        if (testCase.has("on_unknown_urn")) {
            builder.unknownUrn(testCase.getString("on_unknown_urn"));
        }
        if (deadLetter) {
            builder.deadLetterQueue(dlqProducer, "orders.dlq");
        }
        builder.build().poll();

        assertEquals(0, handled[0], label);
        // "off" means no DLQ writer exists in this consumer: outcome_no_writer applies.
        String outcome = !deadLetter && expect.has("outcome_no_writer")
            ? expect.getString("outcome_no_writer") : expect.getString("outcome");
        switch (outcome) {
            case "dlq_raw" -> {
                verify(dlqProducer, times(1)).send(any(Queue.class), any(jakarta.jms.Message.class));
                assertEquals(body, written.getValue(), label);
                assertTrue(!written.getValue().contains("\"dead_letter\""), label);
                verify(message, times(1)).acknowledge();
                verify(session, never()).recover();
            }
            case "ack" -> {
                verify(dlqProducer, never()).send(any(Queue.class), any(jakarta.jms.Message.class));
                verify(message, times(1)).acknowledge();
                verify(session, never()).recover();
            }
            case "unsettled" -> {
                verify(message, never()).acknowledge();
                verify(session, times(1)).recover();
            }
            default -> throw new AssertionError("unknown outcome in " + label + ": " + outcome);
        }
    }
}

package com.babelqueue.artemis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Apache ActiveMQ Artemis binding conformance against the vendored canonical suite's
 * {@code artemis} block: the §7 JMS projection (JMSType / JMSCorrelationID + the {@code bq-}
 * string properties) and the {@code attempts = max(body, JMSXDeliveryCount − 1)} reconciliation.
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
}

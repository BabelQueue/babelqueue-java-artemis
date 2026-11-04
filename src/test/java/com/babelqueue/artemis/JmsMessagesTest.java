package com.babelqueue.artemis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import org.junit.jupiter.api.Test;

/** §7 native-metadata reads (no broker): defensive fallbacks on non-TextMessage / JMS errors. */
class JmsMessagesTest {

    @Test
    void bodyOfNeitherTextNorBytesMessageIsEmpty() throws Exception {
        assertEquals(0, JmsMessages.bodyBytes(mock(Message.class)).length);
    }

    @Test
    void bodyBytesOfATextMessageAreItsUtf8AndEmptyWhenUnset() throws Exception {
        jakarta.jms.TextMessage text = mock(jakarta.jms.TextMessage.class);
        when(text.getText()).thenReturn("caf\u00e9");
        org.junit.jupiter.api.Assertions.assertArrayEquals(
            "caf\u00e9".getBytes(java.nio.charset.StandardCharsets.UTF_8), JmsMessages.bodyBytes(text));
        jakarta.jms.TextMessage unset = mock(jakarta.jms.TextMessage.class);
        assertEquals(0, JmsMessages.bodyBytes(unset).length);
    }

    @Test
    void bodyBytesOfABytesMessageAreTheExactContentIncludingInvalidUtf8() throws Exception {
        byte[] content = {'{', (byte) 0xFF, (byte) 0xFE, '}'};
        jakarta.jms.BytesMessage bytes = ArtemisConsumerTest.bytesMessage(content);
        org.junit.jupiter.api.Assertions.assertArrayEquals(content, JmsMessages.bodyBytes(bytes));
        // reset() before the read and again after it: the message is left readable from the start.
        org.mockito.Mockito.verify(bytes, org.mockito.Mockito.times(2)).reset();
    }

    @Test
    void shortReadsAreAssembledAndAnEarlyEndIsTruncated() throws Exception {
        jakarta.jms.BytesMessage bytes = mock(jakarta.jms.BytesMessage.class);
        when(bytes.getBodyLength()).thenReturn(4L);
        when(bytes.readBytes(org.mockito.ArgumentMatchers.any(byte[].class))).thenAnswer(inv -> {
            byte[] target = inv.getArgument(0);
            target[0] = 'a';
            if (target.length == 4) {
                return 1; // short read
            }
            return -1; // premature end of stream
        });
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] {'a'}, JmsMessages.bodyBytes(bytes));
    }

    @Test
    void strictUtf8RejectsInvalidBytesWithoutSubstitution() {
        assertEquals("ok", JmsMessages.strictUtf8("ok".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertNull(JmsMessages.strictUtf8(new byte[] {(byte) 0xFF}));
    }

    @Test
    void typeReturnsNullOnError() throws Exception {
        Message message = mock(Message.class);
        when(message.getJMSType()).thenThrow(new JMSException("boom"));
        assertNull(JmsMessages.type(message));
    }

    @Test
    void deliveryCountIsZeroWhenAbsentOrOnError() throws Exception {
        Message absent = mock(Message.class);
        when(absent.propertyExists("JMSXDeliveryCount")).thenReturn(false);
        assertEquals(0, JmsMessages.deliveryCount(absent));

        Message throwing = mock(Message.class);
        when(throwing.propertyExists("JMSXDeliveryCount")).thenThrow(new JMSException("boom"));
        assertEquals(0, JmsMessages.deliveryCount(throwing));
    }

    @Test
    void propertyNameLegalityFollowsJmsIdentifierRules() {
        assertTrue(JmsMessages.isLegalPropertyName("traceparent"));
        assertTrue(JmsMessages.isLegalPropertyName("tracestate"));
        assertTrue(JmsMessages.isLegalPropertyName("bq_replay_bypass"));
        assertFalse(JmsMessages.isLegalPropertyName("bq-replay-bypass"));
        assertFalse(JmsMessages.isLegalPropertyName("JMSXGroupID"));
        assertFalse(JmsMessages.isLegalPropertyName("NULL"));
        assertFalse(JmsMessages.isLegalPropertyName("1abc"));
        assertTrue(JmsMessages.isLegalPropertyName("trace\uD835\uDC1Aparent")); // supplementary-plane letter
        assertFalse(JmsMessages.isLegalPropertyName(""));
        assertFalse(JmsMessages.isLegalPropertyName(null));
    }

    @Test
    void toMapSurfacesStringPropertiesAndToleratesNullUnreadableAndNonStrings() throws Exception {
        Message message = mock(Message.class);
        when(message.getPropertyNames()).thenReturn(
            java.util.Collections.enumeration(java.util.List.of("traceparent", "blank", "JMSXDeliveryCount")));
        when(message.getObjectProperty("traceparent")).thenReturn("00-x");
        when(message.getObjectProperty("blank")).thenReturn("");
        when(message.getObjectProperty("JMSXDeliveryCount")).thenReturn(2);

        assertEquals(java.util.Map.of("traceparent", "00-x"), JmsMessages.toMap(message));
        assertTrue(JmsMessages.toMap(null).isEmpty());
        assertTrue(JmsMessages.toMap(mock(Message.class)).isEmpty()); // null enumeration
        Message throwing = mock(Message.class);
        when(throwing.getPropertyNames()).thenThrow(new JMSException("boom"));
        assertTrue(JmsMessages.toMap(throwing).isEmpty());
        assertTrue(BabelHandler.headersOf(null).isEmpty());
    }
}

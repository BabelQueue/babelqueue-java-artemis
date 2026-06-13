package com.babelqueue.artemis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import org.junit.jupiter.api.Test;

/** §7 native-metadata reads (no broker): defensive fallbacks on non-TextMessage / JMS errors. */
class JmsMessagesTest {

    @Test
    void textOfNonTextMessageIsEmpty() throws Exception {
        assertEquals("", JmsMessages.text(mock(Message.class)));
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
}

package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;

/**
 * Implementations of this interface are used to map back and forth from Stomp
 * to ActiveMQ. There are several standard mappings which are semantically the
 * same, the inner class, Helper, provides functions to copy those properties
 * from one to the other
 */
public interface FrameTranslator {
    public ActiveMQMessage convertFrame(StompFrame frame) throws JMSException, ProtocolException;

    public StompFrame convertMessage(ActiveMQMessage message) throws IOException, JMSException;

    public String convertDestination(Destination d);

    public ActiveMQDestination convertDestination(String name) throws ProtocolException;

    /**
     * Helper class which holds commonly needed functions used when implementing
     * FrameTranslators
     */
    public final static class Helper {
        public static void copyStandardHeadersFromMessageToFrame(ActiveMQMessage message, StompFrame command, FrameTranslator ft) throws IOException {
            final Map headers = command.getHeaders();
            headers.put(Stomp.Headers.Message.DESTINATION, ft.convertDestination(message.getDestination()));
            headers.put(Stomp.Headers.Message.MESSAGE_ID, message.getJMSMessageID());

            if (message.getJMSCorrelationID() != null) {
                headers.put(Stomp.Headers.Message.CORRELATION_ID, message.getJMSCorrelationID());
            }
            headers.put(Stomp.Headers.Message.EXPIRATION_TIME, "" + message.getJMSExpiration());

            if (message.getJMSRedelivered()) {
                headers.put(Stomp.Headers.Message.REDELIVERED, "true");
            }
            headers.put(Stomp.Headers.Message.PRORITY, "" + message.getJMSPriority());

            if (message.getJMSReplyTo() != null) {
                headers.put(Stomp.Headers.Message.REPLY_TO, ft.convertDestination(message.getJMSReplyTo()));
            }
            headers.put(Stomp.Headers.Message.TIMESTAMP, "" + message.getJMSTimestamp());

            if (message.getJMSType() != null) {
                headers.put(Stomp.Headers.Message.TYPE, message.getJMSType());
            }

            // now lets add all the message headers
            final Map properties = message.getProperties();
            if (properties != null) {
                headers.putAll(properties);
            }
        }

        public static void copyStandardHeadersFromFrameToMessage(StompFrame command, ActiveMQMessage msg, FrameTranslator ft) throws ProtocolException, JMSException {
            final Map headers = new HashMap(command.getHeaders());
            final String destination = (String)headers.remove(Stomp.Headers.Send.DESTINATION);
            msg.setDestination(ft.convertDestination(destination));

            // the standard JMS headers
            msg.setJMSCorrelationID((String)headers.remove(Stomp.Headers.Send.CORRELATION_ID));

            Object o = headers.remove(Stomp.Headers.Send.EXPIRATION_TIME);
            if (o != null) {
                msg.setJMSExpiration(Long.parseLong((String)o));
            }

            o = headers.remove(Stomp.Headers.Send.PRIORITY);
            if (o != null) {
                msg.setJMSPriority(Integer.parseInt((String)o));
            }

            o = headers.remove(Stomp.Headers.Send.TYPE);
            if (o != null) {
                msg.setJMSType((String)o);
            }

            o = headers.remove(Stomp.Headers.Send.REPLY_TO);
            if (o != null) {
                msg.setJMSReplyTo(ft.convertDestination((String)o));
            }

            o = headers.remove(Stomp.Headers.Send.PERSISTENT);
            if (o != null) {
                msg.setPersistent("true".equals(o));
            }

            // now the general headers
            msg.setProperties(headers);
        }
    }
}

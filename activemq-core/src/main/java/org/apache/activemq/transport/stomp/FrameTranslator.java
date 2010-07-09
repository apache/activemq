/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    ActiveMQMessage convertFrame(ProtocolConverter converter, StompFrame frame) throws JMSException, ProtocolException;

    StompFrame convertMessage(ProtocolConverter converter, ActiveMQMessage message) throws IOException, JMSException;

    String convertDestination(ProtocolConverter converter, Destination d);

    ActiveMQDestination convertDestination(ProtocolConverter converter, String name) throws ProtocolException;

    /**
     * Helper class which holds commonly needed functions used when implementing
     * FrameTranslators
     */
    static final class Helper {

        private Helper() {
        }

        public static void copyStandardHeadersFromMessageToFrame(ProtocolConverter converter, ActiveMQMessage message, StompFrame command, FrameTranslator ft) throws IOException {
            final Map<String, String> headers = command.getHeaders();
            headers.put(Stomp.Headers.Message.DESTINATION, ft.convertDestination(converter, message.getDestination()));
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
                headers.put(Stomp.Headers.Message.REPLY_TO, ft.convertDestination(converter, message.getJMSReplyTo()));
            }
            headers.put(Stomp.Headers.Message.TIMESTAMP, "" + message.getJMSTimestamp());

            if (message.getJMSType() != null) {
                headers.put(Stomp.Headers.Message.TYPE, message.getJMSType());
            }

            if (message.getUserID() != null) {
                headers.put(Stomp.Headers.Message.USERID, message.getUserID());
            }
            
            // now lets add all the message headers
            final Map<String, Object> properties = message.getProperties();
            if (properties != null) {
                for (Map.Entry<String, Object> prop : properties.entrySet()) {
                    headers.put(prop.getKey(), "" + prop.getValue());
                }
            }
        }

        public static void copyStandardHeadersFromFrameToMessage(ProtocolConverter converter, StompFrame command, ActiveMQMessage msg, FrameTranslator ft) throws ProtocolException, JMSException {
            final Map<String, String> headers = new HashMap<String, String>(command.getHeaders());
            final String destination = headers.remove(Stomp.Headers.Send.DESTINATION);
            msg.setDestination(ft.convertDestination(converter, destination));

            // the standard JMS headers
            msg.setJMSCorrelationID(headers.remove(Stomp.Headers.Send.CORRELATION_ID));

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
                msg.setJMSReplyTo(ft.convertDestination(converter, (String)o));
            }

            o = headers.remove(Stomp.Headers.Send.PERSISTENT);
            if (o != null) {
                msg.setPersistent("true".equals(o));
            }

            // Stomp specific headers
            headers.remove(Stomp.Headers.RECEIPT_REQUESTED);

            // Since we take the rest of the header and put them in properties which could then
            // be sent back to a STOMP consumer we need to sanitize anything which could be in
            // Stomp.Headers.Message and might get passed through to the consumer
            headers.remove(Stomp.Headers.Message.MESSAGE_ID);
            headers.remove(Stomp.Headers.Message.TIMESTAMP);
            headers.remove(Stomp.Headers.Message.REDELIVERED);
            headers.remove(Stomp.Headers.Message.SUBSCRIPTION);
            headers.remove(Stomp.Headers.Message.USERID);

            // now the general headers
            msg.setProperties(headers);
        }
    }
}

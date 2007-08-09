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

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;

/**
 * Implements ActiveMQ 4.0 translations
 */
public class LegacyFrameTranslator implements FrameTranslator {
    public ActiveMQMessage convertFrame(StompFrame command) throws JMSException, ProtocolException {
        final Map headers = command.getHeaders();
        final ActiveMQMessage msg;
        if (headers.containsKey(Stomp.Headers.CONTENT_LENGTH)) {
            headers.remove(Stomp.Headers.CONTENT_LENGTH);
            ActiveMQBytesMessage bm = new ActiveMQBytesMessage();
            bm.writeBytes(command.getContent());
            msg = bm;
        } else {
            ActiveMQTextMessage text = new ActiveMQTextMessage();
            try {
                text.setText(new String(command.getContent(), "UTF-8"));
            } catch (Throwable e) {
                throw new ProtocolException("Text could not bet set: " + e, false, e);
            }
            msg = text;
        }
        FrameTranslator.Helper.copyStandardHeadersFromFrameToMessage(command, msg, this);
        return msg;
    }

    public StompFrame convertMessage(ActiveMQMessage message) throws IOException, JMSException {
        StompFrame command = new StompFrame();
        command.setAction(Stomp.Responses.MESSAGE);
        Map headers = new HashMap(25);
        command.setHeaders(headers);

        FrameTranslator.Helper.copyStandardHeadersFromMessageToFrame(message, command, this);

        if (message.getDataStructureType() == ActiveMQTextMessage.DATA_STRUCTURE_TYPE) {

            ActiveMQTextMessage msg = (ActiveMQTextMessage)message.copy();
            command.setContent(msg.getText().getBytes("UTF-8"));

        } else if (message.getDataStructureType() == ActiveMQBytesMessage.DATA_STRUCTURE_TYPE) {

            ActiveMQBytesMessage msg = (ActiveMQBytesMessage)message.copy();
            msg.setReadOnlyBody(true);
            byte[] data = new byte[(int)msg.getBodyLength()];
            msg.readBytes(data);

            headers.put(Stomp.Headers.CONTENT_LENGTH, "" + data.length);
            command.setContent(data);
        }
        return command;
    }

    public String convertDestination(Destination d) {
        if (d == null) {
            return null;
        }
        ActiveMQDestination activeMQDestination = (ActiveMQDestination)d;
        String physicalName = activeMQDestination.getPhysicalName();

        StringBuffer buffer = new StringBuffer();
        if (activeMQDestination.isQueue()) {
            if (activeMQDestination.isTemporary()) {
                buffer.append("/temp-queue/");
            } else {
                buffer.append("/queue/");
            }
        } else {
            if (activeMQDestination.isTemporary()) {
                buffer.append("/temp-topic/");
            } else {
                buffer.append("/topic/");
            }
        }
        buffer.append(physicalName);
        return buffer.toString();
    }

    public ActiveMQDestination convertDestination(String name) throws ProtocolException {
        if (name == null) {
            return null;
        } else if (name.startsWith("/queue/")) {
            String qName = name.substring("/queue/".length(), name.length());
            return ActiveMQDestination.createDestination(qName, ActiveMQDestination.QUEUE_TYPE);
        } else if (name.startsWith("/topic/")) {
            String tName = name.substring("/topic/".length(), name.length());
            return ActiveMQDestination.createDestination(tName, ActiveMQDestination.TOPIC_TYPE);
        } else if (name.startsWith("/temp-queue/")) {
            String tName = name.substring("/temp-queue/".length(), name.length());
            return ActiveMQDestination.createDestination(tName, ActiveMQDestination.TEMP_QUEUE_TYPE);
        } else if (name.startsWith("/temp-topic/")) {
            String tName = name.substring("/temp-topic/".length(), name.length());
            return ActiveMQDestination.createDestination(tName, ActiveMQDestination.TEMP_TOPIC_TYPE);
        } else {
            throw new ProtocolException("Illegal destination name: [" + name + "] -- ActiveMQ STOMP destinations "
                                        + "must begine with one of: /queue/ /topic/ /temp-queue/ /temp-topic/");
        }
    }
}

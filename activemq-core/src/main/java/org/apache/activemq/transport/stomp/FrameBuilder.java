/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.stomp;

import org.apache.activemq.command.ActiveMQMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

class FrameBuilder {
    private String command;
    private Properties headers = new Properties();
    private byte[] body = new byte[0];

    public FrameBuilder(String command) {
        this.command = command;
    }

    public FrameBuilder addHeader(String key, String value) {
        if (value != null) {
            this.headers.setProperty(key, value);
        }
        return this;
    }

    public FrameBuilder addHeader(String key, long value) {
        this.headers.put(key, new Long(value));
        return this;
    }

    public FrameBuilder addHeaders(ActiveMQMessage message) throws IOException {
        addHeader(Stomp.Headers.Message.DESTINATION, DestinationNamer.convert(message.getDestination()));
        addHeader(Stomp.Headers.Message.MESSAGE_ID, message.getJMSMessageID());
        addHeader(Stomp.Headers.Message.CORRELATION_ID, message.getJMSCorrelationID());
        addHeader(Stomp.Headers.Message.EXPIRATION_TIME, message.getJMSExpiration());
        if (message.getJMSRedelivered()) {
            addHeader(Stomp.Headers.Message.REDELIVERED, "true");
        }
        addHeader(Stomp.Headers.Message.PRORITY, message.getJMSPriority());
        addHeader(Stomp.Headers.Message.REPLY_TO, DestinationNamer.convert(message.getJMSReplyTo()));
        addHeader(Stomp.Headers.Message.TIMESTAMP, message.getJMSTimestamp());
        addHeader(Stomp.Headers.Message.TYPE, message.getJMSType());

        // now lets add all the message headers
        Map properties = message.getProperties();
        if (properties != null) {
            headers.putAll(properties);
        }
        return this;
    }

    public FrameBuilder setBody(byte[] body) {
        this.body = body;
        return this;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append(command);
        buffer.append(Stomp.NEWLINE);
        for (Iterator iterator = headers.keySet().iterator(); iterator.hasNext();) {
            String key = (String) iterator.next();
            String property = headers.getProperty(key);
            if (property != null) {
                buffer.append(key).append(Stomp.Headers.SEPERATOR).append(property).append(Stomp.NEWLINE);
            }
        }
        buffer.append(Stomp.NEWLINE);
        buffer.append(body);
        buffer.append(Stomp.NULL);
        buffer.append(Stomp.NEWLINE);
        return buffer.toString();
    }

    byte[] toFrame() {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            bout.write(command.getBytes());
            bout.write(Stomp.NEWLINE.getBytes());
            for (Iterator iterator = headers.keySet().iterator(); iterator.hasNext();) {
                String key = (String) iterator.next();
                String property = headers.getProperty(key);
                if (property != null) {
                    bout.write(key.getBytes());
                    bout.write(Stomp.Headers.SEPERATOR.getBytes());
                    bout.write(property.getBytes());
                    bout.write(Stomp.NEWLINE.getBytes());
                }
            }
            bout.write(Stomp.NEWLINE.getBytes());
            bout.write(body);
            bout.write(Stomp.NULL.getBytes());
            bout.write(Stomp.NEWLINE.getBytes());
        }
        catch (IOException e) {
            throw new RuntimeException("World is caving in, we just got io error writing to" + "a byte array output stream we instantiated!");
        }
        return bout.toByteArray();
    }
}

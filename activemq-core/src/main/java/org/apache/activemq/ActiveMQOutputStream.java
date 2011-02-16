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
package org.apache.activemq;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.util.IOExceptionSupport;

/**
 * 
 */
public class ActiveMQOutputStream extends OutputStream implements Disposable {

    protected int count;

    final byte buffer[];

    private final ActiveMQConnection connection;
    private final Map<String, Object> properties;
    private final ProducerInfo info;

    private long messageSequence;
    private boolean closed;
    private final int deliveryMode;
    private final int priority;
    private final long timeToLive;

    /**
     * JMS Property which is used to specify the size (in kb) which is used as chunk size when splitting the stream. Default is 64kb
     */
    public final static String AMQ_STREAM_CHUNK_SIZE = "AMQ_STREAM_CHUNK_SIZE";

    public ActiveMQOutputStream(ActiveMQConnection connection, ProducerId producerId, ActiveMQDestination destination, Map<String, Object> properties, int deliveryMode, int priority,
                                long timeToLive) throws JMSException {
        this.connection = connection;
        this.deliveryMode = deliveryMode;
        this.priority = priority;
        this.timeToLive = timeToLive;
        this.properties = properties == null ? null : new HashMap<String, Object>(properties);

        Integer chunkSize = this.properties == null ? null : (Integer) this.properties.get(AMQ_STREAM_CHUNK_SIZE);
        if (chunkSize == null) {
            chunkSize = 64 * 1024;
        } else {
            if (chunkSize < 1) {
                throw new IllegalArgumentException("Chunk size must be greater then 0");
            } else {
                chunkSize *= 1024;
            }
        }

        buffer = new byte[chunkSize];

        if (destination == null) {
            throw new InvalidDestinationException("Don't understand null destinations");
        }

        this.info = new ProducerInfo(producerId);
        this.info.setDestination(destination);

        this.connection.addOutputStream(this);
        this.connection.asyncSendPacket(info);
    }

    public void close() throws IOException {
        if (!closed) {
            flushBuffer();
            try {
                // Send an EOS style empty message to signal EOS.
                send(new ActiveMQMessage(), true);
                dispose();
                this.connection.asyncSendPacket(info.createRemoveCommand());
            } catch (JMSException e) {
                IOExceptionSupport.create(e);
            }
        }
    }

    public void dispose() {
        if (!closed) {
            this.connection.removeOutputStream(this);
            closed = true;
        }
    }

    public synchronized void write(int b) throws IOException {
        buffer[count++] = (byte) b;
        if (count == buffer.length) {
            flushBuffer();
        }
    }

    public synchronized void write(byte b[], int off, int len) throws IOException {
        while (len > 0) {
            int max = Math.min(len, buffer.length - count);
            System.arraycopy(b, off, buffer, count, max);

            len -= max;
            count += max;
            off += max;

            if (count == buffer.length) {
                flushBuffer();
            }
        }
    }

    public synchronized void flush() throws IOException {
        flushBuffer();
    }

    private void flushBuffer() throws IOException {
        if (count != 0) {
            try {
                ActiveMQBytesMessage msg = new ActiveMQBytesMessage();
                msg.writeBytes(buffer, 0, count);
                send(msg, false);
            } catch (JMSException e) {
                throw IOExceptionSupport.create(e);
            }
            count = 0;
        }
    }

    /**
     * @param msg
     * @throws JMSException
     */
    private void send(ActiveMQMessage msg, boolean eosMessage) throws JMSException {
        if (properties != null) {
            for (Iterator iter = properties.keySet().iterator(); iter.hasNext();) {
                String key = (String) iter.next();
                Object value = properties.get(key);
                msg.setObjectProperty(key, value);
            }
        }
        msg.setType("org.apache.activemq.Stream");
        msg.setGroupID(info.getProducerId().toString());
        if (eosMessage) {
            msg.setGroupSequence(-1);
        } else {
            msg.setGroupSequence((int) messageSequence);
        }
        MessageId id = new MessageId(info.getProducerId(), messageSequence++);
        connection.send(info.getDestination(), msg, id, deliveryMode, priority, timeToLive, !eosMessage);
    }

    public String toString() {
        return "ActiveMQOutputStream { producerId=" + info.getProducerId() + " }";
    }

}

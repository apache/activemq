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
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.JMSExceptionSupport;

/**
 * @version $Revision$
 */
public class ActiveMQInputStream extends InputStream implements ActiveMQDispatcher {

    private final ActiveMQConnection connection;
    private final ConsumerInfo info;
    // These are the messages waiting to be delivered to the client
    private final MessageDispatchChannel unconsumedMessages = new FifoMessageDispatchChannel();

    private int deliveredCounter;
    private MessageDispatch lastDelivered;
    private boolean eosReached;
    private byte buffer[];
    private int pos;
    private Map<String, Object> jmsProperties;

    private ProducerId producerId;
    private long nextSequenceId;

    public ActiveMQInputStream(ActiveMQConnection connection, ConsumerId consumerId, ActiveMQDestination dest, String selector, boolean noLocal, String name, int prefetch)
        throws JMSException {
        this.connection = connection;

        if (dest == null) {
            throw new InvalidDestinationException("Don't understand null destinations");
        } else if (dest.isTemporary()) {
            String physicalName = dest.getPhysicalName();

            if (physicalName == null) {
                throw new IllegalArgumentException("Physical name of Destination should be valid: " + dest);
            }

            String connectionID = connection.getConnectionInfo().getConnectionId().getValue();

            if (physicalName.indexOf(connectionID) < 0) {
                throw new InvalidDestinationException("Cannot use a Temporary destination from another Connection");
            }

            if (connection.isDeleted(dest)) {
                throw new InvalidDestinationException("Cannot use a Temporary destination that has been deleted");
            }
        }

        this.info = new ConsumerInfo(consumerId);
        this.info.setSubscriptionName(name);

        if (selector != null && selector.trim().length() != 0) {
            selector = "JMSType='org.apache.activemq.Stream' AND ( " + selector + " ) ";
        } else {
            selector = "JMSType='org.apache.activemq.Stream'";
        }

        SelectorParser.parse(selector);
        this.info.setSelector(selector);

        this.info.setPrefetchSize(prefetch);
        this.info.setNoLocal(noLocal);
        this.info.setBrowser(false);
        this.info.setDispatchAsync(false);

        // Allows the options on the destination to configure the consumerInfo
        if (dest.getOptions() != null) {
            Map<String, String> options = new HashMap<String, String>(dest.getOptions());
            IntrospectionSupport.setProperties(this.info, options, "consumer.");
        }

        this.info.setDestination(dest);

        this.connection.addInputStream(this);
        this.connection.addDispatcher(info.getConsumerId(), this);
        this.connection.syncSendPacket(info);
        unconsumedMessages.start();
    }

    @Override
    public void close() throws IOException {
        if (!unconsumedMessages.isClosed()) {
            try {
                if (lastDelivered != null) {
                    MessageAck ack = new MessageAck(lastDelivered, MessageAck.STANDARD_ACK_TYPE, deliveredCounter);
                    connection.asyncSendPacket(ack);
                }
                dispose();
                this.connection.syncSendPacket(info.createRemoveCommand());
            } catch (JMSException e) {
                throw IOExceptionSupport.create(e);
            }
        }
    }

    public void dispose() {
        if (!unconsumedMessages.isClosed()) {
            unconsumedMessages.close();
            this.connection.removeDispatcher(info.getConsumerId());
            this.connection.removeInputStream(this);
        }
    }

    /**
     * Return the JMS Properties which where used to send the InputStream
     *
     * @return jmsProperties
     * @throws IOException
     */
    public Map<String, Object> getJMSProperties() throws IOException {
        if (jmsProperties == null) {
            fillBuffer();
        }
        return jmsProperties;
    }

    public ActiveMQMessage receive() throws JMSException {
        checkClosed();
        MessageDispatch md;
        try {
            md = unconsumedMessages.dequeue(-1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw JMSExceptionSupport.create(e);
        }

        if (md == null || unconsumedMessages.isClosed() || md.getMessage().isExpired()) {
            return null;
        }

        deliveredCounter++;
        if ((0.75 * info.getPrefetchSize()) <= deliveredCounter) {
            MessageAck ack = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, deliveredCounter);
            connection.asyncSendPacket(ack);
            deliveredCounter = 0;
            lastDelivered = null;
        } else {
            lastDelivered = md;
        }

        return (ActiveMQMessage)md.getMessage();
    }

    /**
     * @throws IllegalStateException
     */
    protected void checkClosed() throws IllegalStateException {
        if (unconsumedMessages.isClosed()) {
            throw new IllegalStateException("The Consumer is closed");
        }
    }

    @Override
    public int read() throws IOException {
        fillBuffer();
        if (eosReached || buffer.length == 0) {
            return -1;
        }

        return buffer[pos++] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        fillBuffer();
        if (eosReached || buffer.length == 0) {
            return -1;
        }

        int max = Math.min(len, buffer.length - pos);
        System.arraycopy(buffer, pos, b, off, max);

        pos += max;
        return max;
    }

    private void fillBuffer() throws IOException {
        if (eosReached || (buffer != null && buffer.length > pos)) {
            return;
        }
        try {
            while (true) {
                ActiveMQMessage m = receive();
                if (m != null && m.getDataStructureType() == CommandTypes.ACTIVEMQ_BYTES_MESSAGE) {
                    // First message.
                    long producerSequenceId = m.getMessageId().getProducerSequenceId();
                    if (producerId == null) {
                        // We have to start a stream at sequence id = 0
                        if (producerSequenceId != 0) {
                            continue;
                        }
                        nextSequenceId++;
                        producerId = m.getMessageId().getProducerId();
                    } else {
                        // Verify it's the next message of the sequence.
                        if (!m.getMessageId().getProducerId().equals(producerId)) {
                            throw new IOException("Received an unexpected message: invalid producer: " + m);
                        }
                        if (producerSequenceId != nextSequenceId++) {
                            throw new IOException("Received an unexpected message: expected ID: " + (nextSequenceId - 1) + " but was: " + producerSequenceId + " for message: " + m);
                        }
                    }

                    // Read the buffer in.
                    ActiveMQBytesMessage bm = (ActiveMQBytesMessage)m;
                    buffer = new byte[(int)bm.getBodyLength()];
                    bm.readBytes(buffer);
                    pos = 0;
                    if (jmsProperties == null) {
                        jmsProperties = Collections.unmodifiableMap(new HashMap<String, Object>(bm.getProperties()));
                    }
                } else {
                    eosReached = true;
                    if (jmsProperties == null) {
                        // no properties found
                        jmsProperties = Collections.emptyMap();
                    }
                }
                return;
            }
        } catch (JMSException e) {
            eosReached = true;
            if (jmsProperties == null) {
                // no properties found
                jmsProperties = Collections.emptyMap();
            }
            throw IOExceptionSupport.create(e);
        }
    }

    public void dispatch(MessageDispatch md) {
        unconsumedMessages.enqueue(md);
    }

    @Override
    public String toString() {
        return "ActiveMQInputStream { value=" + info.getConsumerId() + ", producerId=" + producerId + " }";
    }

}

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
package org.apache.activemq.advisory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.Service;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An object which can be used to listen to the number of active consumers
 * available on a given destination.
 * 
 * 
 */
public class ProducerEventSource implements Service, MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerEventSource.class);

    private final Connection connection;
    private final ActiveMQDestination destination;
    private ProducerListener listener;
    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicInteger producerCount = new AtomicInteger();
    private Session session;
    private MessageConsumer consumer;

    public ProducerEventSource(Connection connection, Destination destination) throws JMSException {
        this.connection = connection;
        this.destination = ActiveMQDestination.transform(destination);
    }

    public void setProducerListener(ProducerListener listener) {
        this.listener = listener;
    }

    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ActiveMQTopic advisoryTopic = AdvisorySupport.getProducerAdvisoryTopic(destination);
            consumer = session.createConsumer(advisoryTopic);
            consumer.setMessageListener(this);
        }
    }

    public void stop() throws Exception {
        if (started.compareAndSet(true, false)) {
            if (session != null) {
                session.close();
            }
        }
    }

    public void onMessage(Message message) {
        if (message instanceof ActiveMQMessage) {
            ActiveMQMessage activeMessage = (ActiveMQMessage)message;
            Object command = activeMessage.getDataStructure();
            int count = 0;
            if (command instanceof ProducerInfo) {
                count = producerCount.incrementAndGet();
                count = extractProducerCountFromMessage(message, count);
                fireProducerEvent(new ProducerStartedEvent(this, destination, (ProducerInfo)command, count));
            } else if (command instanceof RemoveInfo) {
                RemoveInfo removeInfo = (RemoveInfo)command;
                if (removeInfo.isProducerRemove()) {
                    count = producerCount.decrementAndGet();
                    count = extractProducerCountFromMessage(message, count);
                    fireProducerEvent(new ProducerStoppedEvent(this, destination, (ProducerId)removeInfo.getObjectId(), count));
                }
            } else {
                LOG.warn("Unknown command: " + command);
            }
        } else {
            LOG.warn("Unknown message type: " + message + ". Message ignored");
        }
    }

    protected int extractProducerCountFromMessage(Message message, int count) {
        try {
            Object value = message.getObjectProperty("producerCount");
            if (value instanceof Number) {
                Number n = (Number)value;
                return n.intValue();
            }
            LOG.warn("No producerCount header available on the message: " + message);
        } catch (Exception e) {
            LOG.warn("Failed to extract producerCount from message: " + message + ".Reason: " + e, e);
        }
        return count;
    }

    protected void fireProducerEvent(ProducerEvent event) {
        if (listener != null) {
            listener.onProducerEvent(event);
        }
    }

}

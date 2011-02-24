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
package org.apache.activemq.tool;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Topic;

import org.apache.activemq.tool.properties.JmsClientProperties;
import org.apache.activemq.tool.properties.JmsConsumerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsConsumerClient extends AbstractJmsMeasurableClient {
    private static final Logger LOG = LoggerFactory.getLogger(JmsConsumerClient.class);

    protected MessageConsumer jmsConsumer;
    protected JmsConsumerProperties client;

    public JmsConsumerClient(ConnectionFactory factory) {
        this(new JmsConsumerProperties(), factory);
    }

    public JmsConsumerClient(JmsConsumerProperties clientProps, ConnectionFactory factory) {
        super(factory);
        client = clientProps;
    }

    public void receiveMessages() throws JMSException {
        if (client.isAsyncRecv()) {
            if (client.getRecvType().equalsIgnoreCase(JmsConsumerProperties.TIME_BASED_RECEIVING)) {
                receiveAsyncTimeBasedMessages(client.getRecvDuration());
            } else {
                receiveAsyncCountBasedMessages(client.getRecvCount());
            }
        } else {
            if (client.getRecvType().equalsIgnoreCase(JmsConsumerProperties.TIME_BASED_RECEIVING)) {
                receiveSyncTimeBasedMessages(client.getRecvDuration());
            } else {
                receiveSyncCountBasedMessages(client.getRecvCount());
            }
        }
    }

    public void receiveMessages(int destCount) throws JMSException {
        this.destCount = destCount;
        receiveMessages();
    }

    public void receiveMessages(int destIndex, int destCount) throws JMSException {
        this.destIndex = destIndex;
        receiveMessages(destCount);
    }

    public void receiveSyncTimeBasedMessages(long duration) throws JMSException {
        if (getJmsConsumer() == null) {
            createJmsConsumer();
        }

        try {
            getConnection().start();

            LOG.info("Starting to synchronously receive messages for " + duration + " ms...");
            long endTime = System.currentTimeMillis() + duration;
            while (System.currentTimeMillis() < endTime) {
                getJmsConsumer().receive();
                incThroughput();
                sleep();
            }
        } finally {
            if (client.isDurable() && client.isUnsubscribe()) {
                LOG.info("Unsubscribing durable subscriber: " + getClientName());
                getJmsConsumer().close();
                getSession().unsubscribe(getClientName());
            }
            getConnection().close();
        }
    }

    public void receiveSyncCountBasedMessages(long count) throws JMSException {
        if (getJmsConsumer() == null) {
            createJmsConsumer();
        }

        try {
            getConnection().start();
            LOG.info("Starting to synchronously receive " + count + " messages...");

            int recvCount = 0;
            while (recvCount < count) {
                getJmsConsumer().receive();
                incThroughput();
                recvCount++;
                sleep();
            }
        } finally {
            if (client.isDurable() && client.isUnsubscribe()) {
                LOG.info("Unsubscribing durable subscriber: " + getClientName());
                getJmsConsumer().close();
                getSession().unsubscribe(getClientName());
            }
            getConnection().close();
        }
    }

    public void receiveAsyncTimeBasedMessages(long duration) throws JMSException {
        if (getJmsConsumer() == null) {
            createJmsConsumer();
        }

        getJmsConsumer().setMessageListener(new MessageListener() {
            public void onMessage(Message msg) {
                incThroughput();
                sleep();
            }
        });

        try {
            getConnection().start();
            LOG.info("Starting to asynchronously receive messages for " + duration + " ms...");
            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                throw new JMSException("JMS consumer thread sleep has been interrupted. Message: " + e.getMessage());
            }
        } finally {
            if (client.isDurable() && client.isUnsubscribe()) {
                LOG.info("Unsubscribing durable subscriber: " + getClientName());
                getJmsConsumer().close();
                getSession().unsubscribe(getClientName());
            }
            getConnection().close();
        }
    }

    public void receiveAsyncCountBasedMessages(long count) throws JMSException {
        if (getJmsConsumer() == null) {
            createJmsConsumer();
        }

        final AtomicInteger recvCount = new AtomicInteger(0);
        getJmsConsumer().setMessageListener(new MessageListener() {
            public void onMessage(Message msg) {
                incThroughput();
                recvCount.incrementAndGet();
                synchronized (recvCount) {
                    recvCount.notify();
                }
            }
        });

        try {
            getConnection().start();
            LOG.info("Starting to asynchronously receive " + client.getRecvCount() + " messages...");
            try {
                while (recvCount.get() < count) {
                    synchronized (recvCount) {
                        recvCount.wait();
                    }
                }
            } catch (InterruptedException e) {
                throw new JMSException("JMS consumer thread wait has been interrupted. Message: " + e.getMessage());
            }
        } finally {
            if (client.isDurable() && client.isUnsubscribe()) {
                LOG.info("Unsubscribing durable subscriber: " + getClientName());
                getJmsConsumer().close();
                getSession().unsubscribe(getClientName());
            }
            getConnection().close();
        }
    }

    public MessageConsumer createJmsConsumer() throws JMSException {
        Destination[] dest = createDestination(destIndex, destCount);
        return createJmsConsumer(dest[0]);
    }

    public MessageConsumer createJmsConsumer(Destination dest) throws JMSException {
        if (client.isDurable()) {
            String clientName = getClientName();
            if (clientName == null) {
                clientName = "JmsConsumer";
                setClientName(clientName);
            }
            LOG.info("Creating durable subscriber (" + clientName + ") to: " + dest.toString());
            jmsConsumer = getSession().createDurableSubscriber((Topic) dest, clientName);
        } else {
            LOG.info("Creating non-durable consumer to: " + dest.toString());
            jmsConsumer = getSession().createConsumer(dest);
        }
        return jmsConsumer;
    }

    public MessageConsumer createJmsConsumer(Destination dest, String selector, boolean noLocal) throws JMSException {
        if (client.isDurable()) {
            String clientName = getClientName();
            if (clientName == null) {
                clientName = "JmsConsumer";
                setClientName(clientName);
            }
            LOG.info("Creating durable subscriber (" + clientName + ") to: " + dest.toString());
            jmsConsumer = getSession().createDurableSubscriber((Topic) dest, clientName, selector, noLocal);
        } else {
            LOG.info("Creating non-durable consumer to: " + dest.toString());
            jmsConsumer = getSession().createConsumer(dest, selector, noLocal);
        }
        return jmsConsumer;
    }

    public MessageConsumer getJmsConsumer() {
        return jmsConsumer;
    }

    public JmsClientProperties getClient() {
        return client;
    }

    public void setClient(JmsClientProperties clientProps) {
        client = (JmsConsumerProperties)clientProps;
    }
    
    protected void sleep() {
        if (client.getRecvDelay() > 0) {
        	try {
        		LOG.trace("Sleeping for " + client.getRecvDelay() + " milliseconds");
        		Thread.sleep(client.getRecvDelay());
        	} catch (java.lang.InterruptedException ex) {
        		LOG.warn(ex.getMessage());
        	}
        }
    }
}

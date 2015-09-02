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
package org.apache.activemq.network.jms;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;

import org.apache.activemq.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Destination bridge is used to bridge between to different JMS systems
 */
public abstract class DestinationBridge implements Service, MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(DestinationBridge.class);

    protected MessageConsumer consumer;
    protected AtomicBoolean started = new AtomicBoolean(false);
    protected JmsMesageConvertor jmsMessageConvertor;
    protected boolean doHandleReplyTo = true;
    protected JmsConnector jmsConnector;

    /**
     * @return Returns the consumer.
     */
    public MessageConsumer getConsumer() {
        return consumer;
    }

    /**
     * @param consumer The consumer to set.
     */
    public void setConsumer(MessageConsumer consumer) {
        this.consumer = consumer;
    }

    /**
     * @param connector
     */
    public void setJmsConnector(JmsConnector connector) {
        this.jmsConnector = connector;
    }

    /**
     * @return Returns the inboundMessageConvertor.
     */
    public JmsMesageConvertor getJmsMessageConvertor() {
        return jmsMessageConvertor;
    }

    /**
     * @param jmsMessageConvertor
     */
    public void setJmsMessageConvertor(JmsMesageConvertor jmsMessageConvertor) {
        this.jmsMessageConvertor = jmsMessageConvertor;
    }

    protected Destination processReplyToDestination(Destination destination) {
        return jmsConnector.createReplyToBridge(destination, getConnnectionForConsumer(), getConnectionForProducer());
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            createConsumer();
            createProducer();
        }
    }

    @Override
    public void stop() throws Exception {
        started.set(false);
    }

    @Override
    public void onMessage(Message message) {

        int attempt = 0;
        final int maxRetries = jmsConnector.getReconnectionPolicy().getMaxSendRetries();

        while (started.get() && message != null && (maxRetries == ReconnectionPolicy.INFINITE || attempt <= maxRetries)) {

            try {

                if (attempt++ > 0) {
                    try {
                        Thread.sleep(jmsConnector.getReconnectionPolicy().getNextDelay(attempt));
                    } catch(InterruptedException e) {
                        break;
                    }
                }

                Message converted;
                if (jmsMessageConvertor != null) {
                    if (doHandleReplyTo) {
                        Destination replyTo = message.getJMSReplyTo();
                        if (replyTo != null) {
                            converted = jmsMessageConvertor.convert(message, processReplyToDestination(replyTo));
                        } else {
                            converted = jmsMessageConvertor.convert(message);
                        }
                    } else {
                        message.setJMSReplyTo(null);
                        converted = jmsMessageConvertor.convert(message);
                    }
                } else {
                    // The Producer side is not up or not yet configured, retry.
                    continue;
                }

                try {
                    sendMessage(converted);
                } catch(Exception e) {
                    jmsConnector.handleConnectionFailure(getConnectionForProducer());
                    continue;
                }

                try {
                    message.acknowledge();
                } catch(Exception e) {
                    jmsConnector.handleConnectionFailure(getConnnectionForConsumer());
                    continue;
                }

                // if we got here then it made it out and was ack'd
                return;

            } catch (Exception e) {
                LOG.info("failed to forward message on attempt: {} reason: {} message: {}", new Object[]{ attempt, e, message }, e);
            }
        }
    }

    /**
     * @return Returns the doHandleReplyTo.
     */
    public boolean isDoHandleReplyTo() {
        return doHandleReplyTo;
    }

    /**
     * @param doHandleReplyTo The doHandleReplyTo to set.
     */
    public void setDoHandleReplyTo(boolean doHandleReplyTo) {
        this.doHandleReplyTo = doHandleReplyTo;
    }

    protected abstract MessageConsumer createConsumer() throws JMSException;

    protected abstract MessageProducer createProducer() throws JMSException;

    protected abstract void sendMessage(Message message) throws JMSException;

    protected abstract Connection getConnnectionForConsumer();

    protected abstract Connection getConnectionForProducer();

}

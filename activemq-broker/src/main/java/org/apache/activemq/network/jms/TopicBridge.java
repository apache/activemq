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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

/**
 * A Destination bridge is used to bridge between to different JMS systems
 *
 *
 */
class TopicBridge extends DestinationBridge {
    protected Topic consumerTopic;
    protected Topic producerTopic;
    protected TopicSession consumerSession;
    protected TopicSession producerSession;
    protected String consumerName;
    protected String selector;
    protected TopicPublisher producer;
    protected TopicConnection consumerConnection;
    protected TopicConnection producerConnection;

    @Override
    public void stop() throws Exception {
        super.stop();
        if (consumerSession != null) {
            consumerSession.close();
        }
        if (producerSession != null) {
            producerSession.close();
        }
    }

    @Override
    protected MessageConsumer createConsumer() throws JMSException {
        // set up the consumer
        if (consumerConnection == null) return null;
        consumerSession = consumerConnection.createTopicSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = null;
        if (consumerName != null && consumerName.length() > 0) {
            if (selector != null && selector.length() > 0) {
                consumer = consumerSession.createDurableSubscriber(consumerTopic, consumerName, selector,
                                                                   false);
            } else {
                consumer = consumerSession.createDurableSubscriber(consumerTopic, consumerName);
            }
        } else {
            if (selector != null && selector.length() > 0) {
                consumer = consumerSession.createSubscriber(consumerTopic, selector, false);
            } else {
                consumer = consumerSession.createSubscriber(consumerTopic);
            }
        }

        consumer.setMessageListener(this);

        return consumer;
    }

    @Override
    protected synchronized MessageProducer createProducer() throws JMSException {
        if (producerConnection == null) return null;
        producerSession = producerConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = producerSession.createPublisher(null);
        return producer;
    }

    @Override
    protected synchronized void sendMessage(Message message) throws JMSException {
        if (producer == null && createProducer() == null) {
            throw new JMSException("Producer for remote queue not available.");
        }
        try {
            producer.publish(producerTopic, message);
        } catch (JMSException e) {
            producer = null;
            throw e;
        }
    }

    /**
     * @return Returns the consumerConnection.
     */
    public TopicConnection getConsumerConnection() {
        return consumerConnection;
    }

    /**
     * @param consumerConnection The consumerConnection to set.
     */
    public void setConsumerConnection(TopicConnection consumerConnection) {
        this.consumerConnection = consumerConnection;
        if (started.get()) {
            try {
                createConsumer();
            } catch(Exception e) {
                jmsConnector.handleConnectionFailure(getConnnectionForConsumer());
            }
        }
    }

    /**
     * @return Returns the consumerName.
     */
    public String getConsumerName() {
        return consumerName;
    }

    /**
     * @param consumerName The consumerName to set.
     */
    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    /**
     * @return Returns the consumerTopic.
     */
    public Topic getConsumerTopic() {
        return consumerTopic;
    }

    /**
     * @param consumerTopic The consumerTopic to set.
     */
    public void setConsumerTopic(Topic consumerTopic) {
        this.consumerTopic = consumerTopic;
    }

    /**
     * @return Returns the producerConnection.
     */
    public TopicConnection getProducerConnection() {
        return producerConnection;
    }

    /**
     * @param producerConnection The producerConnection to set.
     */
    public void setProducerConnection(TopicConnection producerConnection) {
        this.producerConnection = producerConnection;
    }

    /**
     * @return Returns the producerTopic.
     */
    public Topic getProducerTopic() {
        return producerTopic;
    }

    /**
     * @param producerTopic The producerTopic to set.
     */
    public void setProducerTopic(Topic producerTopic) {
        this.producerTopic = producerTopic;
    }

    /**
     * @return Returns the selector.
     */
    public String getSelector() {
        return selector;
    }

    /**
     * @param selector The selector to set.
     */
    public void setSelector(String selector) {
        this.selector = selector;
    }

    @Override
    protected Connection getConnnectionForConsumer() {
        return getConsumerConnection();
    }

    @Override
    protected Connection getConnectionForProducer() {
        return getProducerConnection();
    }
}

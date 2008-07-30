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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.Service;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.DestinationInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A helper class which keeps track of the Destinations available in a broker and allows you to listen to them
 * being created or deleted.
 *
 * @version $Revision$
 */
public class DestinationSource implements MessageListener {
    private static final Log LOG = LogFactory.getLog(ConsumerEventSource.class);
    private AtomicBoolean started = new AtomicBoolean(false);
    private final Connection connection;
    private Session session;
    private MessageConsumer queueConsumer;
    private MessageConsumer topicConsumer;
    private MessageConsumer tempTopicConsumer;
    private MessageConsumer tempQueueConsumer;
    private Set<ActiveMQQueue> queues = new CopyOnWriteArraySet<ActiveMQQueue>();
    private Set<ActiveMQTopic> topics = new CopyOnWriteArraySet<ActiveMQTopic>();
    private Set<ActiveMQTempQueue> temporaryQueues = new CopyOnWriteArraySet<ActiveMQTempQueue>();
    private Set<ActiveMQTempTopic> temporaryTopics = new CopyOnWriteArraySet<ActiveMQTempTopic>();
    private DestinationListener listener;

    public DestinationSource(Connection connection) throws JMSException {
        this.connection = connection;
    }

    public DestinationListener getListener() {
        return listener;
    }

    public void setDestinationListener(DestinationListener listener) {
        this.listener = listener;
    }

    /**
     * Returns the current queues available on the broker
     */
    public Set<ActiveMQQueue> getQueues() {
        return queues;
    }

    /**
     * Returns the current topics on the broker
     */
    public Set<ActiveMQTopic> getTopics() {
        return topics;
    }

    /**
     * Returns the current temporary topics available on the broker
     */
    public Set<ActiveMQTempQueue> getTemporaryQueues() {
        return temporaryQueues;
    }

    /**
     * Returns the current temporary queues available on the broker
     */
    public Set<ActiveMQTempTopic> getTemporaryTopics() {
        return temporaryTopics;
    }

    public void start() throws JMSException {
        if (started.compareAndSet(false, true)) {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            queueConsumer = session.createConsumer(AdvisorySupport.QUEUE_ADVISORY_TOPIC);
            queueConsumer.setMessageListener(this);

            topicConsumer = session.createConsumer(AdvisorySupport.TOPIC_ADVISORY_TOPIC);
            topicConsumer.setMessageListener(this);

            tempQueueConsumer = session.createConsumer(AdvisorySupport.TEMP_QUEUE_ADVISORY_TOPIC);
            tempQueueConsumer.setMessageListener(this);

            tempTopicConsumer = session.createConsumer(AdvisorySupport.TEMP_TOPIC_ADVISORY_TOPIC);
            tempTopicConsumer.setMessageListener(this);
        }
    }

    public void stop() throws JMSException {
        if (started.compareAndSet(true, false)) {
            if (session != null) {
                session.close();
            }
        }
    }

    public void onMessage(Message message) {
        if (message instanceof ActiveMQMessage) {
            ActiveMQMessage activeMessage = (ActiveMQMessage) message;
            Object command = activeMessage.getDataStructure();
            if (command instanceof DestinationInfo) {
                DestinationInfo destinationInfo = (DestinationInfo) command;
                DestinationEvent event = new DestinationEvent(this, destinationInfo);
                fireDestinationEvent(event);
            }
            else {
                LOG.warn("Unknown dataStructure: " + command);
            }
        }
        else {
            LOG.warn("Unknown message type: " + message + ". Message ignored");
        }
    }

    protected void fireDestinationEvent(DestinationEvent event) {
        // now lets update the data structures
        ActiveMQDestination destination = event.getDestination();
        boolean add = event.isAddOperation();
        if (destination instanceof ActiveMQQueue) {
            ActiveMQQueue queue = (ActiveMQQueue) destination;
            if (add) {
                queues.add(queue);
            }
            else {
                queues.remove(queue);
            }
        }
        else if (destination instanceof ActiveMQTopic) {
            ActiveMQTopic topic = (ActiveMQTopic) destination;
            if (add) {
                topics.add(topic);
            }
            else {
                topics.remove(topic);
            }
        }
        else if (destination instanceof ActiveMQTempQueue) {
            ActiveMQTempQueue queue = (ActiveMQTempQueue) destination;
            if (add) {
                temporaryQueues.add(queue);
            }
            else {
                temporaryQueues.remove(queue);
            }
        }
        else if (destination instanceof ActiveMQTempTopic) {
            ActiveMQTempTopic topic = (ActiveMQTempTopic) destination;
            if (add) {
                temporaryTopics.add(topic);
            }
            else {
                temporaryTopics.remove(topic);
            }
        }
        else {
            LOG.warn("Unknown destination type: " + destination);
        }
        if (listener != null) {
            listener.onDestinationEvent(event);
        }
    }
}
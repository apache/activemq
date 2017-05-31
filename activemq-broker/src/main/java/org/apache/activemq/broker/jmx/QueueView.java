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
package org.apache.activemq.broker.jmx;

import java.util.Map;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.jms.JMSException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.util.BrokerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a JMX Management view of a Queue.
 */
public class QueueView extends DestinationView implements QueueViewMBean {
    private static final Logger LOG = LoggerFactory.getLogger(QueueView.class);

    public QueueView(ManagedRegionBroker broker, Queue destination) {
        super(broker, destination);
    }

    public CompositeData getMessage(String messageId) throws OpenDataException {
    	CompositeData result = null;
    	QueueMessageReference ref = ((Queue)destination).getMessage(messageId);

        if (ref != null) {
	        Message rc = ref.getMessage();
	        if (rc == null) {
	            return null;
	        }
	        result = OpenTypeSupport.convert(rc);
        }

        return result;
    }

    public synchronized void purge() throws Exception {
        final long originalMessageCount = destination.getDestinationStatistics().getMessages().getCount();

        ((Queue)destination).purge();

        LOG.info("{} purge of {} messages", destination.getActiveMQDestination().getQualifiedName(), originalMessageCount);
    }

    public boolean removeMessage(String messageId) throws Exception {
        return ((Queue)destination).removeMessage(messageId);
    }

    public synchronized int removeMatchingMessages(String selector) throws Exception {
        return ((Queue)destination).removeMatchingMessages(selector);
    }

    public synchronized int removeMatchingMessages(String selector, int maximumMessages) throws Exception {
        return ((Queue)destination).removeMatchingMessages(selector, maximumMessages);
    }

    public boolean copyMessageTo(String messageId, String destinationName) throws Exception {
        ConnectionContext context = BrokerSupport.getConnectionContext(broker.getContextBroker());
        ActiveMQDestination toDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        return ((Queue)destination).copyMessageTo(context, messageId, toDestination);
    }

    public int copyMatchingMessagesTo(String selector, String destinationName) throws Exception {
        ConnectionContext context = BrokerSupport.getConnectionContext(broker.getContextBroker());
        ActiveMQDestination toDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        return ((Queue)destination).copyMatchingMessagesTo(context, selector, toDestination);
    }

    public int copyMatchingMessagesTo(String selector, String destinationName, int maximumMessages) throws Exception {
        ConnectionContext context = BrokerSupport.getConnectionContext(broker.getContextBroker());
        ActiveMQDestination toDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        return ((Queue)destination).copyMatchingMessagesTo(context, selector, toDestination, maximumMessages);
    }

    public boolean moveMessageTo(String messageId, String destinationName) throws Exception {
        ConnectionContext context = BrokerSupport.getConnectionContext(broker.getContextBroker());
        ActiveMQDestination toDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        return ((Queue)destination).moveMessageTo(context, messageId, toDestination);
    }

    public synchronized int moveMatchingMessagesTo(String selector, String destinationName) throws Exception {
        ConnectionContext context = BrokerSupport.getConnectionContext(broker.getContextBroker());
        ActiveMQDestination toDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        return ((Queue)destination).moveMatchingMessagesTo(context, selector, toDestination);
    }

    public synchronized int moveMatchingMessagesTo(String selector, String destinationName, int maximumMessages) throws Exception {
        ConnectionContext context = BrokerSupport.getConnectionContext(broker.getContextBroker());
        ActiveMQDestination toDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        return ((Queue)destination).moveMatchingMessagesTo(context, selector, toDestination, maximumMessages);
    }

    public synchronized int retryMessages() throws Exception {
        ConnectionContext context = BrokerSupport.getConnectionContext(broker.getContextBroker());
        return ((Queue)destination).retryMessages(context, Integer.MAX_VALUE);
    }

    /**
     * Moves a message back to its original destination
     */
    public boolean retryMessage(String messageId) throws Exception {
        Queue queue = (Queue) destination;
        QueueMessageReference ref = queue.getMessage(messageId);
        Message rc = ref.getMessage();
        if (rc != null) {
            ActiveMQDestination originalDestination = rc.getOriginalDestination();
            if (originalDestination != null) {
                ConnectionContext context = BrokerSupport.getConnectionContext(broker.getContextBroker());
                return queue.moveMessageTo(context, ref, originalDestination);
            }
            else {
                throw new JMSException("No original destination for message: "+ messageId);
            }
        }
        else {
            throw new JMSException("Could not find message: "+ messageId);
        }
    }

    public int cursorSize() {
        Queue queue = (Queue) destination;
        if (queue.getMessages() != null){
            return queue.getMessages().size();
        }
        return 0;
    }


    public boolean doesCursorHaveMessagesBuffered() {
       Queue queue = (Queue) destination;
       if (queue.getMessages() != null){
           return queue.getMessages().hasMessagesBufferedToDeliver();
       }
       return false;

    }


    public boolean doesCursorHaveSpace() {
        Queue queue = (Queue) destination;
        if (queue.getMessages() != null){
            return queue.getMessages().hasSpace();
        }
        return false;
    }


    public long getCursorMemoryUsage() {
        Queue queue = (Queue) destination;
        if (queue.getMessages() != null &&  queue.getMessages().getSystemUsage() != null){
            return queue.getMessages().getSystemUsage().getMemoryUsage().getUsage();
        }
        return 0;
    }

    public int getCursorPercentUsage() {
        Queue queue = (Queue) destination;
        if (queue.getMessages() != null &&  queue.getMessages().getSystemUsage() != null){
            return queue.getMessages().getSystemUsage().getMemoryUsage().getPercentUsage();
        }
        return 0;
    }

    public boolean isCursorFull() {
        Queue queue = (Queue) destination;
        if (queue.getMessages() != null){
            return queue.getMessages().isFull();
        }
        return false;
    }

    public boolean isCacheEnabled() {
        Queue queue = (Queue) destination;
        if (queue.getMessages() != null){
            return queue.getMessages().isCacheEnabled();
        }
        return false;
    }

    /**
     * @return a Map of groupNames and ConsumerIds
     */
    @Override
    public Map<String, String> getMessageGroups() {
        Queue queue = (Queue) destination;
        return queue.getMessageGroupOwners().getGroups();
    }

    /**
     * @return the message group type implementation (simple,bucket,cached)
     */
    @Override
    public String getMessageGroupType() {
        Queue queue = (Queue) destination;
        return queue.getMessageGroupOwners().getType();
    }

    /**
     * remove a message group = has the effect of rebalancing group
     */
    @Override
    public void removeMessageGroup(@MBeanInfo("groupName") String groupName) {
        Queue queue = (Queue) destination;
        queue.getMessageGroupOwners().removeGroup(groupName);
    }

    /**
     * remove all the message groups - will rebalance all message groups across consumers
     */
    @Override
    public void removeAllMessageGroups() {
        Queue queue = (Queue) destination;
        queue.getMessageGroupOwners().removeAll();
    }

    @Override
    public void pause() {
        Queue queue = (Queue) destination;
        queue.pauseDispatch();
    }

    @Override
    public void resume() {
        Queue queue = (Queue) destination;
        queue.resumeDispatch();
    }

    @Override
    public boolean isPaused() {
        Queue queue = (Queue) destination;
        return queue.isDispatchPaused();
    }
}

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
package org.apache.activemq.broker.region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueBrowserSubscription extends QueueSubscription {

    protected static final Logger LOG = LoggerFactory.getLogger(QueueBrowserSubscription.class);

    int queueRefs;
    boolean browseDone;
    boolean destinationsAdded;

    private final ConcurrentMap<MessageId, Object> audit = new ConcurrentHashMap<MessageId, Object>();
    private long maxMessages;

    public QueueBrowserSubscription(Broker broker, SystemUsage usageManager, ConnectionContext context, ConsumerInfo info) throws JMSException {
        super(broker, usageManager, context, info);
    }

    @Override
    protected boolean canDispatch(MessageReference node) {
        return !((QueueMessageReference) node).isAcked();
    }

    @Override
    public synchronized String toString() {
        return "QueueBrowserSubscription:" + " consumer=" + info.getConsumerId() +
               ", destinations=" + destinations.size() + ", dispatched=" + dispatched.size() +
               ", delivered=" + this.prefetchExtension + ", pending=" + getPendingQueueSize();
    }

    synchronized public void destinationsAdded() throws Exception {
        destinationsAdded = true;
        checkDone();
    }

    public boolean isDuplicate(MessageId messageId) {
        return audit.putIfAbsent(messageId, Boolean.TRUE) != null;
    }

    private void checkDone() throws Exception {
        if (!browseDone && queueRefs == 0 && destinationsAdded) {
            browseDone = true;
            add(QueueMessageReference.NULL_MESSAGE);
        }
    }

    @Override
    public boolean matches(MessageReference node, MessageEvaluationContext context) throws IOException {
        return !browseDone && super.matches(node, context);
    }

    /**
     * Since we are a browser we don't really remove the message from the queue.
     */
    @Override
    protected void acknowledge(ConnectionContext context, final MessageAck ack, final MessageReference n) throws IOException {
        if (info.isNetworkSubscription()) {
            super.acknowledge(context, ack, n);
        }
    }

    synchronized public void incrementQueueRef() {
        queueRefs++;
    }

    synchronized public void decrementQueueRef() throws Exception {
        if (queueRefs > 0) {
            queueRefs--;
        }
        checkDone();
    }

    @Override
    public List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception {
        super.remove(context, destination);
        // there's no unacked messages that needs to be redelivered
        // in case of browser
        return new ArrayList<MessageReference>();
    }

    public boolean atMax() {
        return maxMessages > 0 && getEnqueueCounter() >= maxMessages;
    }

    public void setMaxMessages(long max) {
        maxMessages = max;
    }
}

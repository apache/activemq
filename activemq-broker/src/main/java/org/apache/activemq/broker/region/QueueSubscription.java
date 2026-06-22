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

import jakarta.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.ExceptionUtils;
import org.apache.activemq.ActiveMQMessageFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueSubscription extends PrefetchSubscription implements LockOwner {

    private static final Logger LOG = LoggerFactory.getLogger(QueueSubscription.class);

    public QueueSubscription(Broker broker, SystemUsage usageManager, ConnectionContext context, ConsumerInfo info) throws JMSException {
        super(broker,usageManager, context, info);
    }

    /**
     * In the queue case, mark the node as dropped and then a gc cycle will
     * remove it from the queue.
     *
     * @throws IOException
     */
    @Override
    protected void acknowledge(final ConnectionContext context, final MessageAck ack, final MessageReference n) throws IOException {
        this.setTimeOfLastMessageAck(System.currentTimeMillis());

        final Destination q = (Destination) n.getRegionDestination();
        final QueueMessageReference node = (QueueMessageReference)n;
        final Queue queue = (Queue)q;
        queue.removeMessage(context, this, node, ack);
    }

    @Override
    protected boolean canDispatch(MessageReference n) throws IOException {
        boolean result = true;
        QueueMessageReference node = (QueueMessageReference)n;
        if (node.isAcked() || node.isDropped()) {
            result = false;
        }
        result = result && (isBrowser() || node.lock(this));
        return result;
    }

    @Override
    public synchronized String toString() {
        return "QueueSubscription:" + " consumer=" + info.getConsumerId() + ", destinations=" + destinations.size() + ", dispatched=" + dispatched.size() + ", delivered="
               + this.prefetchExtension + ", pending=" + getPendingQueueSize() + ", prefetch=" + getPrefetchSize() + ", prefetchExtension=" + prefetchExtension.get();
    }

    @Override
    public int getLockPriority() {
        return info.getPriority();
    }

    @Override
    public boolean isLockExclusive() {
        return info.isExclusive();
    }

    /**
     */
    @Override
    public void destroy() {
        setSlowConsumer(false);
    }


    @Override
    protected boolean isDropped(MessageReference node) {
       boolean result = false;
       if(node instanceof IndirectMessageReference) {
           QueueMessageReference qmr = (QueueMessageReference) node;
           result = qmr.isDropped();
       }
       return result;
    }

    // For queues if a message is in a bad state it could get stuck and will block good
    // messages from being processed. We don't want to handle all errors as not all
    // mean the message is bad, but if we specifically know the message is corrupted
    // then we should remove and DLQ as it may be stuck and not possible to ever dispatch.
    @Override
    protected boolean matchesSelector(MessageReference node, MessageEvaluationContext context)
            throws JMSException, ActiveMQMessageFormatException {
        try {
            return super.matchesSelector(node, context);
        } catch (JMSException e) {
            // This may cause the headers to unmarshal which could throw ActiveMQUnmarshalEOFException
            // The body could also be unmarshaled if using XPATH and could trigger a
            // MessageDataFormat exception which this also handles.
            ActiveMQMessageFormatException formatError = ExceptionUtils.createMessageFormatException(e);
            if (formatError != null) {
                LOG.error("Message could not be read for selector evaluation: {}", e.getMessage(), e);
                throw formatError;
            }
            // it error is not an ActiveMQUnmarshalEOFException, just rethrow the JMSException
            // which will be caught and handled
            throw e;
        }
    }
}

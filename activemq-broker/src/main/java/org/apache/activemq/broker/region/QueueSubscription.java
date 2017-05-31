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

import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.group.MessageGroupMap;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.usage.SystemUsage;
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

}

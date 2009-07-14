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
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.group.MessageGroupMap;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QueueSubscription extends PrefetchSubscription implements LockOwner {

    private static final Log LOG = LogFactory.getLog(QueueSubscription.class);

    public QueueSubscription(Broker broker, SystemUsage usageManager, ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        super(broker,usageManager, context, info);
    }

    /**
     * In the queue case, mark the node as dropped and then a gc cycle will
     * remove it from the queue.
     * 
     * @throws IOException
     */
    protected void acknowledge(final ConnectionContext context, final MessageAck ack, final MessageReference n) throws IOException {
        if (n.isExpired()) {
            if (!broker.isExpired(n)) {
                LOG.info("ignoring ack " + ack + ", for already expired message: " + n);
                return;
            }
        }
        final Destination q = n.getRegionDestination();
        final QueueMessageReference node = (QueueMessageReference)n;
        final Queue queue = (Queue)q;
        queue.removeMessage(context, this, node, ack);
    }

    protected boolean canDispatch(MessageReference n) throws IOException {
        boolean result = true;
        QueueMessageReference node = (QueueMessageReference)n;
        if (node.isAcked() || node.isDropped()) {
            result = false;
        }
        result = result && (isBrowser() || node.lock(this));
        return result;
    }

    /**
     * Assigns the message group to this subscription and set the flag on the
     * message that it is the first message to be dispatched.
     */
    protected void assignGroupToMe(MessageGroupMap messageGroupOwners, MessageReference n, String groupId) throws IOException {
        messageGroupOwners.put(groupId, info.getConsumerId());
        Message message = n.getMessage();
        if (message instanceof ActiveMQMessage) {
            ActiveMQMessage activeMessage = (ActiveMQMessage)message;
            try {
                activeMessage.setBooleanProperty("JMSXGroupFirstForConsumer", true, false);
            } catch (JMSException e) {
                LOG.warn("Failed to set boolean header: " + e, e);
            }
        }
    }

    public synchronized String toString() {
        return "QueueSubscription:" + " consumer=" + info.getConsumerId() + ", destinations=" + destinations.size() + ", dispatched=" + dispatched.size() + ", delivered="
               + this.prefetchExtension + ", pending=" + getPendingQueueSize();
    }

    public int getLockPriority() {
        return info.getPriority();
    }

    public boolean isLockExclusive() {
        return info.isExclusive();
    }

    /**
     */
    public void destroy() {
    }

   
    protected boolean isDropped(MessageReference node) {
       boolean result = false;
       if(node instanceof IndirectMessageReference) {
           QueueMessageReference qmr = (QueueMessageReference) node;
           result = qmr.isDropped();
       }
       return result;
    }

}

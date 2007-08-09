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
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.transaction.Synchronization;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QueueSubscription extends PrefetchSubscription implements LockOwner {

    private static final Log LOG = LogFactory.getLog(QueueSubscription.class);

    public QueueSubscription(Broker broker, ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        super(broker, context, info);
    }

    /**
     * In the queue case, mark the node as dropped and then a gc cycle will
     * remove it from the queue.
     * 
     * @throws IOException
     */
    protected void acknowledge(ConnectionContext context, final MessageAck ack, final MessageReference n) throws IOException {

        final Destination q = n.getRegionDestination();
        q.acknowledge(context, this, ack, n);

        final QueueMessageReference node = (QueueMessageReference)n;
        final Queue queue = (Queue)q;
        if (!ack.isInTransaction()) {
            node.drop();
            queue.dropEvent();
        } else {
            node.setAcked(true);
            context.getTransaction().addSynchronization(new Synchronization() {
                public void afterCommit() throws Exception {
                    node.drop();
                    queue.dropEvent();
                }

                public void afterRollback() throws Exception {
                    node.setAcked(false);
                }
            });
        }
    }

    protected boolean canDispatch(MessageReference n) throws IOException {
        QueueMessageReference node = (QueueMessageReference)n;
        if (node.isAcked())
            return false;
        // Keep message groups together.
        String groupId = node.getGroupID();
        int sequence = node.getGroupSequence();
        if (groupId != null) {
            MessageGroupMap messageGroupOwners = ((Queue)node.getRegionDestination()).getMessageGroupOwners();

            // If we can own the first, then no-one else should own the rest.
            if (sequence == 1) {
                if (node.lock(this)) {
                    assignGroupToMe(messageGroupOwners, n, groupId);
                    return true;
                } else {
                    return false;
                }
            }

            // Make sure that the previous owner is still valid, we may
            // need to become the new owner.
            ConsumerId groupOwner;
            synchronized (node) {
                groupOwner = messageGroupOwners.get(groupId);
                if (groupOwner == null) {
                    if (node.lock(this)) {
                        assignGroupToMe(messageGroupOwners, n, groupId);
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            if (groupOwner.equals(info.getConsumerId())) {
                // A group sequence < 1 is an end of group signal.
                if (sequence < 0) {
                    messageGroupOwners.removeGroup(groupId);
                }
                return true;
            }

            return false;

        } else {
            return node.lock(this);
        }
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
     * Override so that the message ref count is > 0 only when the message is
     * being dispatched to a client. Keeping it at 0 when it is in the pending
     * list allows the message to be swapped out to disk.
     * 
     * @return true if the message was dispatched.
     */
    protected boolean dispatch(MessageReference node) throws IOException {
        boolean rc = false;
        // This brings the message into memory if it was swapped out.
        node.incrementReferenceCount();
        try {
            rc = super.dispatch(node);
        } finally {
            // If the message was dispatched, it could be getting dispatched
            // async, so we
            // can only drop the reference count when that completes @see
            // onDispatch
            if (!rc) {
                node.decrementReferenceCount();
            }
        }
        return rc;
    }

    /**
     * OK Message was transmitted, we can now drop the reference count.
     * 
     * @see org.apache.activemq.broker.region.PrefetchSubscription#onDispatch(org.apache.activemq.broker.region.MessageReference,
     *      org.apache.activemq.command.Message)
     */
    protected void onDispatch(MessageReference node, Message message) {
        // Now that the message has been sent over the wire to the client,
        // we can let it get swapped out.
        node.decrementReferenceCount();
        super.onDispatch(node, message);
    }

    /**
     * Sending a message to the DQL will require us to increment the ref count
     * so we can get at the content.
     */
    protected void sendToDLQ(ConnectionContext context, MessageReference node) throws IOException, Exception {
        // This brings the message into memory if it was swapped out.
        node.incrementReferenceCount();
        try {
            super.sendToDLQ(context, node);
        } finally {
            // This let's the message be swapped out of needed.
            node.decrementReferenceCount();
        }
    }

    /**
     */
    public void destroy() {
    }

}

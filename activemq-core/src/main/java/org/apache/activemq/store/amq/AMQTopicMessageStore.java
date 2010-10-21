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
package org.apache.activemq.store.amq;

import java.io.IOException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.JournalTopicAck;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TopicReferenceStore;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A MessageStore that uses a Journal to store it's messages.
 * 
 * @version $Revision: 1.13 $
 */
public class AMQTopicMessageStore extends AMQMessageStore implements TopicMessageStore {

    private static final Log LOG = LogFactory.getLog(AMQTopicMessageStore.class);
    private TopicReferenceStore topicReferenceStore;
    public AMQTopicMessageStore(AMQPersistenceAdapter adapter,TopicReferenceStore topicReferenceStore, ActiveMQTopic destinationName) {
        super(adapter, topicReferenceStore, destinationName);
        this.topicReferenceStore = topicReferenceStore;
    }

    public void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener) throws Exception {
        flush();
        topicReferenceStore.recoverSubscription(clientId, subscriptionName, new RecoveryListenerAdapter(this, listener));
    }

    public void recoverNextMessages(String clientId, String subscriptionName,
            int maxReturned, final MessageRecoveryListener listener)
            throws Exception {
        RecoveryListenerAdapter recoveryListener = new RecoveryListenerAdapter(this, listener);
            topicReferenceStore.recoverNextMessages(clientId, subscriptionName,maxReturned, recoveryListener);
            if (recoveryListener.size() == 0) {
                flush();
                topicReferenceStore.recoverNextMessages(clientId,subscriptionName, maxReturned, recoveryListener);
            }
    }

    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return topicReferenceStore.lookupSubscription(clientId, subscriptionName);
    }

    public void addSubsciption(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
        peristenceAdapter.writeCommand(subscriptionInfo, false);
        topicReferenceStore.addSubsciption(subscriptionInfo, retroactive);
    }

    /**
     */
    public void acknowledge(final ConnectionContext context, final String clientId, final String subscriptionName,
                            final MessageId messageId, final MessageAck originalAck) throws IOException {
        final boolean debug = LOG.isDebugEnabled();
        JournalTopicAck ack = new JournalTopicAck();
        ack.setDestination(destination);
        ack.setMessageId(messageId);
        ack.setMessageSequenceId(messageId.getBrokerSequenceId());
        ack.setSubscritionName(subscriptionName);
        ack.setClientId(clientId);
        ack.setTransactionId(context.getTransaction() != null ? context.getTransaction().getTransactionId() : null);
        final Location location = peristenceAdapter.writeCommand(ack, false);
        final SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
        if (!context.isInTransaction()) {
            if (debug) {
                LOG.debug("Journalled acknowledge for: " + messageId + ", at: " + location);
            }
            acknowledge(context,messageId, location, clientId,subscriptionName);
        } else {
            if (debug) {
                LOG.debug("Journalled transacted acknowledge for: " + messageId + ", at: " + location);
            }
            lock.lock();
            try {
                inFlightTxLocations.add(location);
            }finally {
                lock.unlock();
            }
            transactionStore.acknowledge(this, ack, location);
            context.getTransaction().addSynchronization(new Synchronization() {

                public void afterCommit() throws Exception {
                    if (debug) {
                        LOG.debug("Transacted acknowledge commit for: " + messageId + ", at: " + location);
                    }
                    lock.lock();
                    try {
                        inFlightTxLocations.remove(location);
                        acknowledge(context,messageId, location, clientId,subscriptionName);
                    }finally {
                        lock.unlock();
                    }
                }

                public void afterRollback() throws Exception {
                    if (debug) {
                        LOG.debug("Transacted acknowledge rollback for: " + messageId + ", at: " + location);
                    }
                    lock.lock();
                    try{
                        inFlightTxLocations.remove(location);
                    }finally {
                        lock.unlock();
                    }
                }
            });
        }
    }

    public boolean replayAcknowledge(ConnectionContext context, String clientId, String subscritionName, MessageId messageId) {
        try {
            SubscriptionInfo sub = topicReferenceStore.lookupSubscription(clientId, subscritionName);
            if (sub != null) {
                topicReferenceStore.acknowledge(context, clientId, subscritionName, messageId, null);
                return true;
            }
        } catch (Throwable e) {
            LOG.debug("Could not replay acknowledge for message '" + messageId + "'.  Message may have already been acknowledged. reason: " + e);
        }
        return false;
    }

    /**
     * @param messageId
     * @param location
     * @param key
     * @throws IOException 
     */
    protected void acknowledge(final ConnectionContext context, MessageId messageId,
            Location location, String clientId, String subscriptionName)
            throws IOException {
        MessageAck ack = null;
        lock.lock();
        try {
            lastLocation = location;
        }finally {
            lock.unlock();
        }
        
            if (topicReferenceStore.acknowledgeReference(context, clientId,
                    subscriptionName, messageId)) {
                ack = new MessageAck();
                ack.setLastMessageId(messageId);
               
            }
        
        if (ack != null) {
            removeMessage(context, ack);
        }
    }

    /**
     * @return Returns the longTermStore.
     */
    public TopicReferenceStore getTopicReferenceStore() {
        return topicReferenceStore;
    }

    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        topicReferenceStore.deleteSubscription(clientId, subscriptionName);
    }

    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        return topicReferenceStore.getAllSubscriptions();
    }

    public int getMessageCount(String clientId, String subscriberName) throws IOException {
        flush();
        SubscriptionInfo info = lookupSubscription(clientId, subscriberName);
        try {
            MessageCounter counter = new MessageCounter(info, this);
            topicReferenceStore.recoverSubscription(clientId, subscriberName, counter);
            return counter.count;
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }
    
    private class MessageCounter implements MessageRecoveryListener {
        
        int count = 0;
        SubscriptionInfo info;
        BooleanExpression selectorExpression;
        TopicMessageStore store;
        
        public MessageCounter(SubscriptionInfo info, TopicMessageStore store) throws Exception {
            this.info = info;
            if (info != null) {
                String selector = info.getSelector();
                if (selector != null) {
                    this.selectorExpression = SelectorParser.parse(selector);
                }
            }
            this.store = store;
        }

        public boolean recoverMessageReference(MessageId ref) throws Exception {
            if (selectorExpression != null) {
                MessageEvaluationContext ctx = new MessageEvaluationContext();
                ctx.setMessageReference(store.getMessage(ref));
                if (selectorExpression.matches(ctx)) {
                    count++;
                }
            } else {
                count ++;
            }
            return true;
        }

        public boolean recoverMessage(Message message) throws Exception {
            if (selectorExpression != null) {
                MessageEvaluationContext ctx = new MessageEvaluationContext();
                ctx.setMessageReference(store.getMessage(message.getMessageId()));
                if (selectorExpression.matches(ctx)) {
                    count++;
                }
            } else {
                count++;
            }
            return true;
        }

        public boolean isDuplicate(MessageId ref) {
            return false;
        }

        public boolean hasSpace() {
            return true;
        }
    }

    public void resetBatching(String clientId, String subscriptionName) {
        topicReferenceStore.resetBatching(clientId, subscriptionName);
    }
}

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
package org.apache.activemq.store.journal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.activeio.journal.RecordLocation;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.JournalTopicAck;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.Callback;
import org.apache.activemq.util.SubscriptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A MessageStore that uses a Journal to store it's messages.
 *
 *
 */
public class JournalTopicMessageStore extends JournalMessageStore implements TopicMessageStore {

    private static final Logger LOG = LoggerFactory.getLogger(JournalTopicMessageStore.class);

    private TopicMessageStore longTermStore;
    private HashMap<SubscriptionKey, MessageId> ackedLastAckLocations = new HashMap<SubscriptionKey, MessageId>();

    public JournalTopicMessageStore(JournalPersistenceAdapter adapter, TopicMessageStore checkpointStore,
                                    ActiveMQTopic destinationName) {
        super(adapter, checkpointStore, destinationName);
        this.longTermStore = checkpointStore;
    }

    @Override
    public void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener)
        throws Exception {
        this.peristenceAdapter.checkpoint(true, true);
        longTermStore.recoverSubscription(clientId, subscriptionName, listener);
    }

    @Override
    public void recoverNextMessages(String clientId, String subscriptionName, int maxReturned,
                                    MessageRecoveryListener listener) throws Exception {
        this.peristenceAdapter.checkpoint(true, true);
        longTermStore.recoverNextMessages(clientId, subscriptionName, maxReturned, listener);

    }

    @Override
    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return longTermStore.lookupSubscription(clientId, subscriptionName);
    }

    @Override
    public void addSubscription(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
        this.peristenceAdapter.checkpoint(true, true);
        longTermStore.addSubscription(subscriptionInfo, retroactive);
    }

    @Override
    public void addMessage(ConnectionContext context, Message message) throws IOException {
        super.addMessage(context, message);
    }

    /**
     */
    @Override
    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName,
                            final MessageId messageId, MessageAck originalAck) throws IOException {
        final boolean debug = LOG.isDebugEnabled();

        JournalTopicAck ack = new JournalTopicAck();
        ack.setDestination(destination);
        ack.setMessageId(messageId);
        ack.setMessageSequenceId(messageId.getBrokerSequenceId());
        ack.setSubscritionName(subscriptionName);
        ack.setClientId(clientId);
        ack.setTransactionId(context.getTransaction() != null
            ? context.getTransaction().getTransactionId() : null);
        final RecordLocation location = peristenceAdapter.writeCommand(ack, false);

        final SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
        if (!context.isInTransaction()) {
            if (debug) {
                LOG.debug("Journalled acknowledge for: " + messageId + ", at: " + location);
            }
            acknowledge(messageId, location, key);
        } else {
            if (debug) {
                LOG.debug("Journalled transacted acknowledge for: " + messageId + ", at: " + location);
            }
            synchronized (this) {
                inFlightTxLocations.add(location);
            }
            transactionStore.acknowledge(this, ack, location);
            context.getTransaction().addSynchronization(new Synchronization() {
                @Override
                public void afterCommit() throws Exception {
                    if (debug) {
                        LOG.debug("Transacted acknowledge commit for: " + messageId + ", at: " + location);
                    }
                    synchronized (JournalTopicMessageStore.this) {
                        inFlightTxLocations.remove(location);
                        acknowledge(messageId, location, key);
                    }
                }

                @Override
                public void afterRollback() throws Exception {
                    if (debug) {
                        LOG.debug("Transacted acknowledge rollback for: " + messageId + ", at: " + location);
                    }
                    synchronized (JournalTopicMessageStore.this) {
                        inFlightTxLocations.remove(location);
                    }
                }
            });
        }

    }

    public void replayAcknowledge(ConnectionContext context, String clientId, String subscritionName,
                                  MessageId messageId) {
        try {
            SubscriptionInfo sub = longTermStore.lookupSubscription(clientId, subscritionName);
            if (sub != null) {
                longTermStore.acknowledge(context, clientId, subscritionName, messageId, null);
            }
        } catch (Throwable e) {
            LOG.debug("Could not replay acknowledge for message '" + messageId
                      + "'.  Message may have already been acknowledged. reason: " + e);
        }
    }

    /**
     * @param messageId
     * @param location
     * @param key
     */
    protected void acknowledge(MessageId messageId, RecordLocation location, SubscriptionKey key) {
        synchronized (this) {
            lastLocation = location;
            ackedLastAckLocations.put(key, messageId);
        }
    }

    @Override
    public RecordLocation checkpoint() throws IOException {

        final HashMap<SubscriptionKey, MessageId> cpAckedLastAckLocations;

        // swap out the hash maps..
        synchronized (this) {
            cpAckedLastAckLocations = this.ackedLastAckLocations;
            this.ackedLastAckLocations = new HashMap<SubscriptionKey, MessageId>();
        }

        return super.checkpoint(new Callback() {
            @Override
            public void execute() throws Exception {

                // Checkpoint the acknowledged messages.
                Iterator<SubscriptionKey> iterator = cpAckedLastAckLocations.keySet().iterator();
                while (iterator.hasNext()) {
                    SubscriptionKey subscriptionKey = iterator.next();
                    MessageId identity = cpAckedLastAckLocations.get(subscriptionKey);
                    longTermStore.acknowledge(transactionTemplate.getContext(), subscriptionKey.clientId,
                                              subscriptionKey.subscriptionName, identity, null);
                }

            }
        });

    }

    /**
     * @return Returns the longTermStore.
     */
    public TopicMessageStore getLongTermTopicMessageStore() {
        return longTermStore;
    }

    @Override
    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        longTermStore.deleteSubscription(clientId, subscriptionName);
    }

    @Override
    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        return longTermStore.getAllSubscriptions();
    }

    @Override
    public int getMessageCount(String clientId, String subscriberName) throws IOException {
        this.peristenceAdapter.checkpoint(true, true);
        return longTermStore.getMessageCount(clientId, subscriberName);
    }

    @Override
    public long getMessageSize(String clientId, String subscriberName) throws IOException {
        this.peristenceAdapter.checkpoint(true, true);
        return longTermStore.getMessageSize(clientId, subscriberName);
    }

    @Override
    public void resetBatching(String clientId, String subscriptionName) {
        longTermStore.resetBatching(clientId, subscriptionName);
    }

}

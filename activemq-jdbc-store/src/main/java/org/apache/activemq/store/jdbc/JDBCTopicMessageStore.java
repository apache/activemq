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
package org.apache.activemq.store.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStoreSubscriptionStatistics;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class JDBCTopicMessageStore extends JDBCMessageStore implements TopicMessageStore {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCTopicMessageStore.class);
    private Map<String, LastRecovered> subscriberLastRecoveredMap = new ConcurrentHashMap<String, LastRecovered>();
    private Set<String> pendingCompletion = new HashSet<String>();

    public static final String PROPERTY_SEQUENCE_ID_CACHE_SIZE = "org.apache.activemq.store.jdbc.SEQUENCE_ID_CACHE_SIZE";
    private static final int SEQUENCE_ID_CACHE_SIZE = Integer.parseInt(System.getProperty(
               PROPERTY_SEQUENCE_ID_CACHE_SIZE, "1000"), 10);
    private final ReentrantReadWriteLock sequenceIdCacheSizeLock = new ReentrantReadWriteLock();
    private Map<MessageId, long[]> sequenceIdCache = new LinkedHashMap<MessageId, long[]>() {
         @Override
        protected boolean removeEldestEntry(Map.Entry<MessageId, long[]> eldest) {
           return size() > SEQUENCE_ID_CACHE_SIZE;
        }
    };


    public JDBCTopicMessageStore(JDBCPersistenceAdapter persistenceAdapter, JDBCAdapter adapter, WireFormat wireFormat, ActiveMQTopic topic, ActiveMQMessageAudit audit) throws IOException {
        super(persistenceAdapter, adapter, wireFormat, topic, audit);
    }

    @Override
    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId, MessageAck ack) throws IOException {
        if (ack != null && ack.isUnmatchedAck()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("ignoring unmatched selector ack for: " + messageId + ", cleanup will get to this message after subsequent acks.");
            }
            return;
        }
        TransactionContext c = persistenceAdapter.getTransactionContext(context);
        try {
            long[] res = getCachedStoreSequenceId(c, destination, messageId);
            if (this.isPrioritizedMessages()) {
                adapter.doSetLastAckWithPriority(c, destination, context != null ? context.getXid() : null, clientId, subscriptionName, res[0], res[1]);
            } else {
                adapter.doSetLastAck(c, destination, context != null ? context.getXid() : null, clientId, subscriptionName, res[0], res[1]);
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace(clientId + ":" + subscriptionName + " ack, seq: " + res[0] + ", priority: " + res[1] + " mid:" + messageId);
            }
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to store acknowledgment for: " + clientId + " on message " + messageId + " in container: " + e, e);
        } finally {
            c.close();
        }
    }

    public long[] getCachedStoreSequenceId(TransactionContext transactionContext, ActiveMQDestination destination, MessageId messageId) throws SQLException, IOException {
        long[] val = null;
        sequenceIdCacheSizeLock.readLock().lock();
        try {
            val = sequenceIdCache.get(messageId);
        } finally {
            sequenceIdCacheSizeLock.readLock().unlock();
        }
        if (val == null) {
            val = adapter.getStoreSequenceId(transactionContext, destination, messageId);
        }
        return val;
    }

    /**
     * @throws Exception
     */
    @Override
    public void recoverSubscription(String clientId, String subscriptionName, final MessageRecoveryListener listener) throws Exception {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            adapter.doRecoverSubscription(c, destination, clientId, subscriptionName, new JDBCMessageRecoveryListener() {
                @Override
                public boolean recoverMessage(long sequenceId, byte[] data) throws Exception {
                    Message msg = (Message)wireFormat.unmarshal(new ByteSequence(data));
                    msg.getMessageId().setBrokerSequenceId(sequenceId);
                    return listener.recoverMessage(msg);
                }

                @Override
                public boolean recoverMessageReference(String reference) throws Exception {
                    return listener.recoverMessageReference(new MessageId(reference));
                }

            });
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to recover subscription: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    private class LastRecovered implements Iterable<LastRecoveredEntry> {
        LastRecoveredEntry[] perPriority = new LastRecoveredEntry[10];
        LastRecovered() {
            for (int i=0; i<perPriority.length; i++) {
                perPriority[i] = new LastRecoveredEntry(i);
            }
        }

        public void updateStored(long sequence, int priority) {
            perPriority[priority].stored = sequence;
        }

        public LastRecoveredEntry defaultPriority() {
            return perPriority[javax.jms.Message.DEFAULT_PRIORITY];
        }

        @Override
        public String toString() {
            return Arrays.deepToString(perPriority);
        }

        @Override
        public Iterator<LastRecoveredEntry> iterator() {
            return new PriorityIterator();
        }

        class PriorityIterator implements Iterator<LastRecoveredEntry> {
            int current = 9;
            @Override
            public boolean hasNext() {
                for (int i=current; i>=0; i--) {
                    if (perPriority[i].hasMessages()) {
                        current = i;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public LastRecoveredEntry next() {
                return perPriority[current];
            }

            @Override
            public void remove() {
                throw new RuntimeException("not implemented");
            }
        }
    }

    private class LastRecoveredEntry {
        final int priority;
        long recovered = 0;
        long stored = Integer.MAX_VALUE;

        public LastRecoveredEntry(int priority) {
            this.priority = priority;
        }

        @Override
        public String toString() {
            return priority + "-" + stored + ":" + recovered;
        }

        public void exhausted() {
            stored = recovered;
        }

        public boolean hasMessages() {
            return stored > recovered;
        }
    }

    class LastRecoveredAwareListener implements JDBCMessageRecoveryListener {
        final MessageRecoveryListener delegate;
        final int maxMessages;
        LastRecoveredEntry lastRecovered;
        int recoveredCount;
        int recoveredMarker;

        public LastRecoveredAwareListener(MessageRecoveryListener delegate, int maxMessages) {
            this.delegate = delegate;
            this.maxMessages = maxMessages;
        }

        @Override
        public boolean recoverMessage(long sequenceId, byte[] data) throws Exception {
            if (delegate.hasSpace() && recoveredCount < maxMessages) {
                Message msg = (Message) wireFormat.unmarshal(new ByteSequence(data));
                msg.getMessageId().setBrokerSequenceId(sequenceId);
                lastRecovered.recovered = sequenceId;
                if (delegate.recoverMessage(msg)) {
                    recoveredCount++;
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean recoverMessageReference(String reference) throws Exception {
            return delegate.recoverMessageReference(new MessageId(reference));
        }

        public void setLastRecovered(LastRecoveredEntry lastRecovered) {
            this.lastRecovered = lastRecovered;
            recoveredMarker = recoveredCount;
        }

        public boolean complete() {
            return  !delegate.hasSpace() || recoveredCount == maxMessages;
        }

        public boolean stalled() {
            return recoveredMarker == recoveredCount;
        }
    }

    @Override
    public synchronized void recoverNextMessages(final String clientId, final String subscriptionName, final int maxReturned, final MessageRecoveryListener listener)
            throws Exception {
        //Duration duration = new Duration("recoverNextMessages");
        TransactionContext c = persistenceAdapter.getTransactionContext();

        String key = getSubscriptionKey(clientId, subscriptionName);
        if (!subscriberLastRecoveredMap.containsKey(key)) {
           subscriberLastRecoveredMap.put(key, new LastRecovered());
        }
        final LastRecovered lastRecovered = subscriberLastRecoveredMap.get(key);
        LastRecoveredAwareListener recoveredAwareListener = new LastRecoveredAwareListener(listener, maxReturned);
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace(this + ", " + key + " existing last recovered: " + lastRecovered);
            }
            if (isPrioritizedMessages()) {
                Iterator<LastRecoveredEntry> it = lastRecovered.iterator();
                for ( ; it.hasNext() && !recoveredAwareListener.complete(); ) {
                    LastRecoveredEntry entry = it.next();
                    recoveredAwareListener.setLastRecovered(entry);
                    //Duration microDuration = new Duration("recoverNextMessages:loop");
                    adapter.doRecoverNextMessagesWithPriority(c, destination, clientId, subscriptionName,
                        entry.recovered, entry.priority, maxReturned, recoveredAwareListener);
                    //microDuration.end(new String(entry + " recoveredCount:" + recoveredAwareListener.recoveredCount));
                    if (recoveredAwareListener.stalled()) {
                        if (recoveredAwareListener.complete()) {
                            break;
                        } else {
                            entry.exhausted();
                        }
                    }
                }
            } else {
                LastRecoveredEntry last = lastRecovered.defaultPriority();
                recoveredAwareListener.setLastRecovered(last);
                adapter.doRecoverNextMessages(c, destination, clientId, subscriptionName,
                        last.recovered, 0, maxReturned, recoveredAwareListener);
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace(key + " last recovered: " + lastRecovered);
            }
            //duration.end();
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
        } finally {
            c.close();
        }
    }

    @Override
    public void resetBatching(String clientId, String subscriptionName) {
        String key = getSubscriptionKey(clientId, subscriptionName);
        if (!pendingCompletion.contains(key))  {
            subscriberLastRecoveredMap.remove(key);
        } else {
            LOG.trace(this +  ", skip resetBatch during pending completion for: " + key);
        }
    }

    public void pendingCompletion(String clientId, String subscriptionName, long sequenceId, byte priority) {
        final String key = getSubscriptionKey(clientId, subscriptionName);
        LastRecovered recovered = new LastRecovered();
        recovered.perPriority[isPrioritizedMessages() ? priority : javax.jms.Message.DEFAULT_PRIORITY].recovered = sequenceId;
        subscriberLastRecoveredMap.put(key, recovered);
        pendingCompletion.add(key);
        LOG.trace(this + ", pending completion: " + key + ", last: " + recovered);
    }

    public void complete(String clientId, String subscriptionName) {
        pendingCompletion.remove(getSubscriptionKey(clientId, subscriptionName));
        LOG.trace(this + ", completion for: " + getSubscriptionKey(clientId, subscriptionName));
    }

    @Override
    protected void onAdd(Message message, long sequenceId, byte priority) {
        // update last recovered state
        for (LastRecovered last : subscriberLastRecoveredMap.values()) {
            last.updateStored(sequenceId, priority);
        }
        sequenceIdCacheSizeLock.writeLock().lock();
        try {
            sequenceIdCache.put(message.getMessageId(), new long[]{sequenceId, priority});
        } finally {
            sequenceIdCacheSizeLock.writeLock().unlock();
        }
    }

    @Override
    public void addSubscription(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            c = persistenceAdapter.getTransactionContext();
            adapter.doSetSubscriberEntry(c, subscriptionInfo, retroactive, isPrioritizedMessages());
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to lookup subscription for info: " + subscriptionInfo.getClientId() + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    /**
     * @see org.apache.activemq.store.TopicMessageStore#lookupSubscription(String,
     *      String)
     */
    @Override
    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            return adapter.doGetSubscriberEntry(c, destination, clientId, subscriptionName);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to lookup subscription for: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    @Override
    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            adapter.doDeleteSubscription(c, destination, clientId, subscriptionName);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to remove subscription for: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
            resetBatching(clientId, subscriptionName);
        }
    }

    @Override
    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            return adapter.doGetAllSubscriptions(c, destination);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to lookup subscriptions. Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    @Override
    public int getMessageCount(String clientId, String subscriberName) throws IOException {
        //Duration duration = new Duration("getMessageCount");
        int result = 0;
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            result = adapter.doGetDurableSubscriberMessageCount(c, destination, clientId, subscriberName, isPrioritizedMessages());
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to get Message Count: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace(clientId + ":" + subscriberName + ", messageCount: " + result);
        }
        //duration.end();
        return result;
    }

    @Override
    public long getMessageSize(String clientId, String subscriberName) throws IOException {
        return 0;
    }

    protected String getSubscriptionKey(String clientId, String subscriberName) {
        String result = clientId + ":";
        result += subscriberName != null ? subscriberName : "NOT_SET";
        return result;
    }

    private final MessageStoreSubscriptionStatistics stats = new MessageStoreSubscriptionStatistics(false);

    @Override
    public MessageStoreSubscriptionStatistics getMessageStoreSubStatistics() {
        return stats;
    }

}

/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * @version $Revision: 1.6 $
 */
public class JDBCTopicMessageStore extends JDBCMessageStore implements TopicMessageStore {

    public JDBCTopicMessageStore(JDBCPersistenceAdapter persistenceAdapter, JDBCAdapter adapter, WireFormat wireFormat,
            ActiveMQTopic topic) {
        super(persistenceAdapter, adapter, wireFormat, topic);
    }

    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId)
            throws IOException {
        long seq = messageId.getBrokerSequenceId();
        // Get a connection and insert the message into the DB.
        TransactionContext c = persistenceAdapter.getTransactionContext(context);
        try {
            adapter.doSetLastAck(c, destination, clientId, subscriptionName, seq);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ",e);
            throw IOExceptionSupport.create("Failed to store acknowledgment for: " + clientId + " on message "
                    + messageId + " in container: " + e, e);
        } finally {
            c.close();
        }
    }

    /**
     * @throws Exception
     * 
     */
    public void recoverSubscription(String clientId, String subscriptionName, final MessageRecoveryListener listener)
            throws Exception {

        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            adapter.doRecoverSubscription(c, destination, clientId, subscriptionName,
                    new JDBCMessageRecoveryListener() {
                        public void recoverMessage(long sequenceId, byte[] data) throws Exception {
                            Message msg = (Message) wireFormat.unmarshal(new ByteSequence(data));
                            msg.getMessageId().setBrokerSequenceId(sequenceId);
                            listener.recoverMessage(msg);
                        }
                        public void recoverMessageReference(String reference) throws Exception {
                            listener.recoverMessageReference(reference);
                        }
                        
                        public void finished(){
                            listener.finished();
                        }
                    });
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ",e);
            throw IOExceptionSupport.create("Failed to recover subscription: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    public void recoverNextMessages(final String clientId,final String subscriptionName, final MessageId lastMessageId,final int maxReturned,final MessageRecoveryListener listener) throws Exception{
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            long lastSequence = lastMessageId != null ? lastMessageId.getBrokerSequenceId() : -1;
            adapter.doRecoverNextMessages(c, destination, clientId, subscriptionName,lastSequence,maxReturned,
                    new JDBCMessageRecoveryListener() {
                        public void recoverMessage(long sequenceId, byte[] data) throws Exception {
                            Message msg = (Message) wireFormat.unmarshal(new ByteSequence(data));
                            msg.getMessageId().setBrokerSequenceId(sequenceId);
                            listener.recoverMessage(msg);
                        }
                        public void recoverMessageReference(String reference) throws Exception {
                            listener.recoverMessageReference(reference);
                        }
                        
                        public void finished(){
                            listener.finished();
                        }
                    });
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ",e);

        } finally {
            c.close();
        }
        
    }
    
    public void resetBatching(String clientId,String subscriptionName,MessageId id) {
    }
    
    /**
     * @see org.apache.activemq.store.TopicMessageStore#storeSubsciption(org.apache.activemq.service.SubscriptionInfo,
     *      boolean)
     */
    public void addSubsciption(String clientId, String subscriptionName, String selector, boolean retroactive)
            throws IOException {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            c = persistenceAdapter.getTransactionContext();
            adapter.doSetSubscriberEntry(c, destination, clientId, subscriptionName, selector, retroactive);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ",e);
            throw IOExceptionSupport
                    .create("Failed to lookup subscription for info: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    /**
     * @see org.apache.activemq.store.TopicMessageStore#lookupSubscription(String,
     *      String)
     */
    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            return adapter.doGetSubscriberEntry(c, destination, clientId, subscriptionName);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ",e);
            throw IOExceptionSupport.create("Failed to lookup subscription for: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            adapter.doDeleteSubscription(c, destination, clientId, subscriptionName);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ",e);
            throw IOExceptionSupport.create("Failed to remove subscription for: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            return adapter.doGetAllSubscriptions(c, destination);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ",e);
            throw IOExceptionSupport.create("Failed to lookup subscriptions. Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    public MessageId getNextMessageIdToDeliver(String clientId,String subscriptionName,MessageId id) throws Exception{
        
        final MessageId result = new MessageId();
        final AtomicBoolean initalized = new AtomicBoolean();
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            long sequence = id != null ? id.getBrokerSequenceId() : -1;
           adapter.doGetNextDurableSubscriberMessageIdStatement(c, destination, clientId, subscriptionName,sequence,new JDBCMessageRecoveryListener() {
               public void recoverMessage(long sequenceId, byte[] data) throws Exception {
                   Message msg = (Message) wireFormat.unmarshal(new ByteSequence(data));
                   msg.getMessageId().setBrokerSequenceId(sequenceId);
                   result.setBrokerSequenceId(msg.getMessageId().getBrokerSequenceId());
                   initalized.set(true);
                   
               }
               public void recoverMessageReference(String reference) throws Exception {
                   result.setValue(reference);
                   initalized.set(true);
                   
               }
               
               public void finished(){          
               }
           });
           
               
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ",e);
            throw IOExceptionSupport.create("Failed to get next MessageId to deliver: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
        return initalized.get () ? result : null;
    }
    
    public MessageId getPreviousMessageIdToDeliver(String clientId,String subscriptionName,MessageId id) throws Exception{
        final MessageId result = new MessageId();
        final AtomicBoolean initalized = new AtomicBoolean();
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            long sequence = id != null ? id.getBrokerSequenceId() : -1;
           adapter.doGetPrevDurableSubscriberMessageIdStatement(c, destination, clientId, subscriptionName,sequence,new JDBCMessageRecoveryListener() {
               public void recoverMessage(long sequenceId, byte[] data) throws Exception {
                   Message msg = (Message) wireFormat.unmarshal(new ByteSequence(data));
                   msg.getMessageId().setBrokerSequenceId(sequenceId);
                   result.setProducerId(msg.getMessageId().getProducerId());
                   result.setProducerSequenceId(msg.getMessageId().getProducerSequenceId());
                   result.setBrokerSequenceId(msg.getMessageId().getBrokerSequenceId());
                   initalized.set(true);
                   
               }
               public void recoverMessageReference(String reference) throws Exception {
                   result.setValue(reference);
                   initalized.set(true);
                   
               }
               
               public void finished(){          
               }
           });
           
               
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ",e);
            throw IOExceptionSupport.create("Failed to get next MessageId to deliver: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
        return initalized.get () ? result : null;
    }

    public int getMessageCount(String clientId,String subscriberName) throws IOException{
        int result = 0;
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            result = adapter.doGetDurableSubscriberMessageCount(c, destination, clientId, subscriberName);
               
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ",e);
            throw IOExceptionSupport.create("Failed to get Message Count: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
        return result;
    }

    

    

}

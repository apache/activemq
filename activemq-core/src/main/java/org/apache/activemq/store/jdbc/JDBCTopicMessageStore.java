/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.activeio.command.WireFormat;
import org.activeio.packet.ByteArrayPacket;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.IOExceptionSupport;

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
            throw IOExceptionSupport.create("Failed to store acknowledgment for: " + clientId + " on message "
                    + messageId + " in container: " + e, e);
        } finally {
            c.close();
        }
    }

    /**
     * @throws Throwable
     * 
     */
    public void recoverSubscription(String clientId, String subscriptionName, final MessageRecoveryListener listener)
            throws Throwable {

        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            adapter.doRecoverSubscription(c, destination, clientId, subscriptionName,
                    new JDBCMessageRecoveryListener() {
                        public void recoverMessage(long sequenceId, byte[] data) throws Throwable {
                            Message msg = (Message) wireFormat.unmarshal(new ByteArrayPacket(data));
                            msg.getMessageId().setBrokerSequenceId(sequenceId);
                            listener.recoverMessage(msg);
                        }
                        public void recoverMessageReference(String reference) throws IOException, Throwable {
                            listener.recoverMessageReference(reference);
                        }
                    });
        } catch (SQLException e) {
            throw IOExceptionSupport.create("Failed to recover subscription: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
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
            throw IOExceptionSupport.create("Failed to remove subscription for: " + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

}

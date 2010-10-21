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
import java.util.Set;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SubscriptionInfo;

/**
 * @version $Revision: 1.5 $
 */
public interface JDBCAdapter {

    void setStatements(Statements statementProvider);
    
    void setPrioritizedMessages(boolean prioritizedMessages);

    void doCreateTables(TransactionContext c) throws SQLException, IOException;

    void doDropTables(TransactionContext c) throws SQLException, IOException;

    void doAddMessage(TransactionContext c, long sequence, MessageId messageID, ActiveMQDestination destination, byte[] data, long expiration, byte priority) throws SQLException, IOException;

    void doAddMessageReference(TransactionContext c, long sequence, MessageId messageId, ActiveMQDestination destination, long expirationTime, String messageRef) throws SQLException, IOException;

    byte[] doGetMessage(TransactionContext c, MessageId id) throws SQLException, IOException;
    
    byte[] doGetMessageById(TransactionContext c, long storeSequenceId) throws SQLException, IOException;

    String doGetMessageReference(TransactionContext c, long id) throws SQLException, IOException;

    void doRemoveMessage(TransactionContext c, long seq) throws SQLException, IOException;

    void doRecover(TransactionContext c, ActiveMQDestination destination, JDBCMessageRecoveryListener listener) throws Exception;

    void doSetLastAck(TransactionContext c, ActiveMQDestination destination, String clientId, String subscriptionName, long seq, long prio) throws SQLException, IOException;

    void doRecoverSubscription(TransactionContext c, ActiveMQDestination destination, String clientId, String subscriptionName, JDBCMessageRecoveryListener listener)
        throws Exception;

    void doRecoverNextMessages(TransactionContext c, ActiveMQDestination destination, String clientId, String subscriptionName, long seq, long priority, int maxReturned,
                               JDBCMessageRecoveryListener listener) throws Exception;

    void doSetSubscriberEntry(TransactionContext c, SubscriptionInfo subscriptionInfo, boolean retroactive) throws SQLException, IOException;

    SubscriptionInfo doGetSubscriberEntry(TransactionContext c, ActiveMQDestination destination, String clientId, String subscriptionName) throws SQLException, IOException;

    long[] getStoreSequenceId(TransactionContext c, ActiveMQDestination destination, MessageId messageID) throws SQLException, IOException;

    void doRemoveAllMessages(TransactionContext c, ActiveMQDestination destinationName) throws SQLException, IOException;

    void doDeleteSubscription(TransactionContext c, ActiveMQDestination destinationName, String clientId, String subscriptionName) throws SQLException, IOException;

    void doDeleteOldMessages(TransactionContext c) throws SQLException, IOException;

    long doGetLastMessageStoreSequenceId(TransactionContext c) throws SQLException, IOException;

    Set<ActiveMQDestination> doGetDestinations(TransactionContext c) throws SQLException, IOException;

    void setUseExternalMessageReferences(boolean useExternalMessageReferences);

    SubscriptionInfo[] doGetAllSubscriptions(TransactionContext c, ActiveMQDestination destination) throws SQLException, IOException;

    int doGetDurableSubscriberMessageCount(TransactionContext c, ActiveMQDestination destination, String clientId, String subscriptionName) throws SQLException, IOException;

    int doGetMessageCount(TransactionContext c, ActiveMQDestination destination) throws SQLException, IOException;

    void doRecoverNextMessages(TransactionContext c, ActiveMQDestination destination, long nextSeq, long priority, int maxReturned, JDBCMessageRecoveryListener listener) throws Exception;

    long[] doGetLastAckedDurableSubscriberMessageId(TransactionContext c, ActiveMQDestination destination, String clientId, String subscriberName) throws SQLException, IOException;

    void doMessageIdScan(TransactionContext c, int limit, JDBCMessageIdScanListener listener) throws SQLException, IOException;

    long doGetLastProducerSequenceId(TransactionContext c, ProducerId id) throws SQLException, IOException;
}

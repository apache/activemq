/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.store.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;

/**
 * @version $Revision: 1.5 $
 */
public interface JDBCAdapter{

    public void setStatements(Statements statementProvider);

    public abstract void doCreateTables(TransactionContext c) throws SQLException,IOException;

    public abstract void doDropTables(TransactionContext c) throws SQLException,IOException;

    public abstract void doAddMessage(TransactionContext c,MessageId messageID,ActiveMQDestination destination,
            byte[] data,long expiration) throws SQLException,IOException;

    public abstract void doAddMessageReference(TransactionContext c,MessageId messageId,
            ActiveMQDestination destination,long expirationTime,String messageRef) throws SQLException,IOException;

    public abstract byte[] doGetMessage(TransactionContext c,long seq) throws SQLException,IOException;

    public abstract String doGetMessageReference(TransactionContext c,long id) throws SQLException,IOException;

    public abstract void doRemoveMessage(TransactionContext c,long seq) throws SQLException,IOException;

    public abstract void doRecover(TransactionContext c,ActiveMQDestination destination,
            JDBCMessageRecoveryListener listener) throws Exception;

    public abstract void doSetLastAck(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName,long seq) throws SQLException,IOException;

    public abstract void doRecoverSubscription(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName,JDBCMessageRecoveryListener listener) throws Exception;

    public abstract void doRecoverNextMessages(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName,long seq,int maxReturned,JDBCMessageRecoveryListener listener) throws Exception;

    public abstract void doSetSubscriberEntry(TransactionContext c,SubscriptionInfo subscriptionInfo,boolean retroactive) throws SQLException,IOException;

    public abstract SubscriptionInfo doGetSubscriberEntry(TransactionContext c,ActiveMQDestination destination,
            String clientId,String subscriptionName) throws SQLException,IOException;

    public abstract long getBrokerSequenceId(TransactionContext c,MessageId messageID) throws SQLException,IOException;

    public abstract void doRemoveAllMessages(TransactionContext c,ActiveMQDestination destinationName)
            throws SQLException,IOException;

    public abstract void doDeleteSubscription(TransactionContext c,ActiveMQDestination destinationName,String clientId,
            String subscriptionName) throws SQLException,IOException;

    public abstract void doDeleteOldMessages(TransactionContext c) throws SQLException,IOException;

    public abstract long doGetLastMessageBrokerSequenceId(TransactionContext c) throws SQLException,IOException;

    public abstract Set doGetDestinations(TransactionContext c) throws SQLException,IOException;

    public abstract void setUseExternalMessageReferences(boolean useExternalMessageReferences);

    public abstract SubscriptionInfo[] doGetAllSubscriptions(TransactionContext c,ActiveMQDestination destination)
            throws SQLException,IOException;

    public int doGetDurableSubscriberMessageCount(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName) throws SQLException,IOException;

    public int doGetMessageCount(TransactionContext c, ActiveMQDestination destination) throws SQLException, IOException;
    
    public void doRecoverNextMessages(TransactionContext c,ActiveMQDestination destination,long nextSeq,int maxReturned,
            JDBCMessageRecoveryListener listener) throws Exception;
    
    public long doGetLastAckedDurableSubscriberMessageId(TransactionContext c,ActiveMQDestination destination,String clientId, String subscriberName) throws SQLException,IOException;
}
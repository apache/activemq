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
package org.apache.activemq.store.jdbc.adapter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.jdbc.JDBCAdapter;
import org.apache.activemq.store.jdbc.JDBCMessageIdScanListener;
import org.apache.activemq.store.jdbc.JDBCMessageRecoveryListener;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.jdbc.Statements;
import org.apache.activemq.store.jdbc.TransactionContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implements all the default JDBC operations that are used by the JDBCPersistenceAdapter. <p/> sub-classing is
 * encouraged to override the default implementation of methods to account for differences in JDBC Driver
 * implementations. <p/> The JDBCAdapter inserts and extracts BLOB data using the getBytes()/setBytes() operations. <p/>
 * The databases/JDBC drivers that use this adapter are:
 * <ul>
 * <li></li>
 * </ul>
 * 
 * @org.apache.xbean.XBean element="defaultJDBCAdapter"
 * 
 * @version $Revision: 1.10 $
 */
public class DefaultJDBCAdapter implements JDBCAdapter {
    private static final Log LOG = LogFactory.getLog(DefaultJDBCAdapter.class);
    protected Statements statements;
    protected boolean batchStatments = true;

    protected void setBinaryData(PreparedStatement s, int index, byte data[]) throws SQLException {
        s.setBytes(index, data);
    }

    protected byte[] getBinaryData(ResultSet rs, int index) throws SQLException {
        return rs.getBytes(index);
    }

    public void doCreateTables(TransactionContext c) throws SQLException, IOException {
        Statement s = null;
        try {
            // Check to see if the table already exists. If it does, then don't
            // log warnings during startup.
            // Need to run the scripts anyways since they may contain ALTER
            // statements that upgrade a previous version
            // of the table
            boolean alreadyExists = false;
            ResultSet rs = null;
            try {
                rs = c.getConnection().getMetaData().getTables(null, null, this.statements.getFullMessageTableName(),
                        new String[] { "TABLE" });
                alreadyExists = rs.next();
            } catch (Throwable ignore) {
            } finally {
                close(rs);
            }
            s = c.getConnection().createStatement();
            String[] createStatments = this.statements.getCreateSchemaStatements();
            for (int i = 0; i < createStatments.length; i++) {
                // This will fail usually since the tables will be
                // created already.
                try {
                    LOG.debug("Executing SQL: " + createStatments[i]);
                    s.execute(createStatments[i]);
                } catch (SQLException e) {
                    if (alreadyExists) {
                        LOG.debug("Could not create JDBC tables; The message table already existed." + " Failure was: "
                                + createStatments[i] + " Message: " + e.getMessage() + " SQLState: " + e.getSQLState()
                                + " Vendor code: " + e.getErrorCode());
                    } else {
                        LOG.warn("Could not create JDBC tables; they could already exist." + " Failure was: "
                                + createStatments[i] + " Message: " + e.getMessage() + " SQLState: " + e.getSQLState()
                                + " Vendor code: " + e.getErrorCode());
                        JDBCPersistenceAdapter.log("Failure details: ", e);
                    }
                }
            }
            c.getConnection().commit();
        } finally {
            try {
                s.close();
            } catch (Throwable e) {
            }
        }
    }

    public void doDropTables(TransactionContext c) throws SQLException, IOException {
        Statement s = null;
        try {
            s = c.getConnection().createStatement();
            String[] dropStatments = this.statements.getDropSchemaStatements();
            for (int i = 0; i < dropStatments.length; i++) {
                // This will fail usually since the tables will be
                // created already.
                try {
                    LOG.debug("Executing SQL: " + dropStatments[i]);
                    s.execute(dropStatments[i]);
                } catch (SQLException e) {
                    LOG.warn("Could not drop JDBC tables; they may not exist." + " Failure was: " + dropStatments[i]
                            + " Message: " + e.getMessage() + " SQLState: " + e.getSQLState() + " Vendor code: "
                            + e.getErrorCode());
                    JDBCPersistenceAdapter.log("Failure details: ", e);
                }
            }
            c.getConnection().commit();
        } finally {
            try {
                s.close();
            } catch (Throwable e) {
            }
        }
    }

    public long doGetLastMessageStoreSequenceId(TransactionContext c) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindLastSequenceIdInMsgsStatement());
            rs = s.executeQuery();
            long seq1 = 0;
            if (rs.next()) {
                seq1 = rs.getLong(1);
            }
            rs.close();
            s.close();
            s = c.getConnection().prepareStatement(this.statements.getFindLastSequenceIdInAcksStatement());
            rs = s.executeQuery();
            long seq2 = 0;
            if (rs.next()) {
                seq2 = rs.getLong(1);
            }
            return Math.max(seq1, seq2);
        } finally {
            close(rs);
            close(s);
        }
    }
    
    public byte[] doGetMessageById(TransactionContext c, long storeSequenceId) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(
                    this.statements.getFindMessageByIdStatement());
            s.setLong(1, storeSequenceId);
            rs = s.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return getBinaryData(rs, 1);
        } finally {
            close(rs);
            close(s);
        }
    }
    

    public void doAddMessage(TransactionContext c, long sequence, MessageId messageID, ActiveMQDestination destination, byte[] data,
            long expiration) throws SQLException, IOException {
        PreparedStatement s = c.getAddMessageStatement();
        try {
            if (s == null) {
                s = c.getConnection().prepareStatement(this.statements.getAddMessageStatement());
                if (this.batchStatments) {
                    c.setAddMessageStatement(s);
                }
            }
            s.setLong(1, sequence);
            s.setString(2, messageID.getProducerId().toString());
            s.setLong(3, messageID.getProducerSequenceId());
            s.setString(4, destination.getQualifiedName());
            s.setLong(5, expiration);
            setBinaryData(s, 6, data);
            if (this.batchStatments) {
                s.addBatch();
            } else if (s.executeUpdate() != 1) {
                throw new SQLException("Failed add a message");
            }
        } finally {
            if (!this.batchStatments) {
                if (s != null) {
                    s.close();
                }
            }
        }
    }

    public void doAddMessageReference(TransactionContext c, long sequence, MessageId messageID, ActiveMQDestination destination,
            long expirationTime, String messageRef) throws SQLException, IOException {
        PreparedStatement s = c.getAddMessageStatement();
        try {
            if (s == null) {
                s = c.getConnection().prepareStatement(this.statements.getAddMessageStatement());
                if (this.batchStatments) {
                    c.setAddMessageStatement(s);
                }
            }
            s.setLong(1, messageID.getBrokerSequenceId());
            s.setString(2, messageID.getProducerId().toString());
            s.setLong(3, messageID.getProducerSequenceId());
            s.setString(4, destination.getQualifiedName());
            s.setLong(5, expirationTime);
            s.setString(6, messageRef);
            if (this.batchStatments) {
                s.addBatch();
            } else if (s.executeUpdate() != 1) {
                throw new SQLException("Failed add a message");
            }
        } finally {
            if (!this.batchStatments) {
                s.close();
            }
        }
    }

    public long getStoreSequenceId(TransactionContext c, MessageId messageID) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindMessageSequenceIdStatement());
            s.setString(1, messageID.getProducerId().toString());
            s.setLong(2, messageID.getProducerSequenceId());
            rs = s.executeQuery();
            if (!rs.next()) {
                return 0;
            }
            return rs.getLong(1);
        } finally {
            close(rs);
            close(s);
        }
    }

    public byte[] doGetMessage(TransactionContext c, MessageId id) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindMessageStatement());
            s.setString(1, id.getProducerId().toString());
            s.setLong(2, id.getProducerSequenceId());
            rs = s.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return getBinaryData(rs, 1);
        } finally {
            close(rs);
            close(s);
        }
    }

    public String doGetMessageReference(TransactionContext c, long seq) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindMessageStatement());
            s.setLong(1, seq);
            rs = s.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return rs.getString(1);
        } finally {
            close(rs);
            close(s);
        }
    }

    public void doRemoveMessage(TransactionContext c, long seq) throws SQLException, IOException {
        PreparedStatement s = c.getRemovedMessageStatement();
        try {
            if (s == null) {
                s = c.getConnection().prepareStatement(this.statements.getRemoveMessageStatment());
                if (this.batchStatments) {
                    c.setRemovedMessageStatement(s);
                }
            }
            s.setLong(1, seq);
            if (this.batchStatments) {
                s.addBatch();
            } else if (s.executeUpdate() != 1) {
                throw new SQLException("Failed to remove message");
            }
        } finally {
            if (!this.batchStatments && s != null) {
                s.close();
            }
        }
    }

    public void doRecover(TransactionContext c, ActiveMQDestination destination, JDBCMessageRecoveryListener listener)
            throws Exception {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindAllMessagesStatement());
            s.setString(1, destination.getQualifiedName());
            rs = s.executeQuery();
            if (this.statements.isUseExternalMessageReferences()) {
                while (rs.next()) {
                    if (!listener.recoverMessageReference(rs.getString(2))) {
                        break;
                    }
                }
            } else {
                while (rs.next()) {
                    if (!listener.recoverMessage(rs.getLong(1), getBinaryData(rs, 2))) {
                        break;
                    }
                }
            }
        } finally {
            close(rs);
            close(s);
        }
    }

    public void doMessageIdScan(TransactionContext c, int limit, 
            JDBCMessageIdScanListener listener) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindAllMessageIdsStatement());
            s.setMaxRows(limit);
            rs = s.executeQuery();
            // jdbc scrollable cursor requires jdbc ver > 1.0 and is often implemented locally so avoid
            LinkedList<MessageId> reverseOrderIds = new LinkedList<MessageId>();
            while (rs.next()) {
                reverseOrderIds.addFirst(new MessageId(rs.getString(2), rs.getLong(3)));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("messageIdScan with limit (" + limit + "), resulted in: " + reverseOrderIds.size() + " ids");
            }
            for (MessageId id : reverseOrderIds) {
                listener.messageId(id);
            }
        } finally {
            close(rs);
            close(s);
        }
    }
    
    public void doSetLastAck(TransactionContext c, ActiveMQDestination destination, String clientId,
            String subscriptionName, long seq) throws SQLException, IOException {
        PreparedStatement s = c.getUpdateLastAckStatement();
        try {
            if (s == null) {
                s = c.getConnection().prepareStatement(this.statements.getUpdateLastAckOfDurableSubStatement());
                if (this.batchStatments) {
                    c.setUpdateLastAckStatement(s);
                }
            }
            s.setLong(1, seq);
            s.setString(2, destination.getQualifiedName());
            s.setString(3, clientId);
            s.setString(4, subscriptionName);
            if (this.batchStatments) {
                s.addBatch();
            } else if (s.executeUpdate() != 1) {
                throw new SQLException("Failed add a message");
            }
        } finally {
            if (!this.batchStatments) {
                s.close();
            }
        }
    }

    public void doRecoverSubscription(TransactionContext c, ActiveMQDestination destination, String clientId,
            String subscriptionName, JDBCMessageRecoveryListener listener) throws Exception {
        // dumpTables(c,
        // destination.getQualifiedName(),clientId,subscriptionName);
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindAllDurableSubMessagesStatement());
            s.setString(1, destination.getQualifiedName());
            s.setString(2, clientId);
            s.setString(3, subscriptionName);
            rs = s.executeQuery();
            if (this.statements.isUseExternalMessageReferences()) {
                while (rs.next()) {
                    if (!listener.recoverMessageReference(rs.getString(2))) {
                        break;
                    }
                }
            } else {
                while (rs.next()) {
                    if (!listener.recoverMessage(rs.getLong(1), getBinaryData(rs, 2))) {
                        break;
                    }
                }
            }
        } finally {
            close(rs);
            close(s);
        }
    }

    public void doRecoverNextMessages(TransactionContext c, ActiveMQDestination destination, String clientId,
            String subscriptionName, long seq, int maxReturned, JDBCMessageRecoveryListener listener) throws Exception {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindDurableSubMessagesStatement());
            s.setMaxRows(maxReturned);
            s.setString(1, destination.getQualifiedName());
            s.setString(2, clientId);
            s.setString(3, subscriptionName);
            s.setLong(4, seq);
            rs = s.executeQuery();
            int count = 0;
            if (this.statements.isUseExternalMessageReferences()) {
                while (rs.next() && count < maxReturned) {
                    if (listener.recoverMessageReference(rs.getString(1))) {
                        count++;
                    } else {
                        break;
                    }
                }
            } else {
                while (rs.next() && count < maxReturned) {
                    if (listener.recoverMessage(rs.getLong(1), getBinaryData(rs, 2))) {
                        count++;
                    } else {
                        break;
                    }
                }
            }
        } finally {
            close(rs);
            close(s);
        }
    }

    public int doGetDurableSubscriberMessageCount(TransactionContext c, ActiveMQDestination destination,
            String clientId, String subscriptionName) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        int result = 0;
        try {
            s = c.getConnection().prepareStatement(this.statements.getDurableSubscriberMessageCountStatement());
            s.setString(1, destination.getQualifiedName());
            s.setString(2, clientId);
            s.setString(3, subscriptionName);
            rs = s.executeQuery();
            if (rs.next()) {
                result = rs.getInt(1);
            }
        } finally {
            close(rs);
            close(s);
        }
        return result;
    }

    /**
     * @param c 
     * @param info 
     * @param retroactive 
     * @throws SQLException 
     * @throws IOException 
     * @see org.apache.activemq.store.jdbc.JDBCAdapter#doSetSubscriberEntry(java.sql.Connection, java.lang.Object,
     *      org.apache.activemq.service.SubscriptionInfo)
     */
    public void doSetSubscriberEntry(TransactionContext c, SubscriptionInfo info, boolean retroactive)
            throws SQLException, IOException {
        // dumpTables(c, destination.getQualifiedName(), clientId,
        // subscriptionName);
        PreparedStatement s = null;
        try {
            long lastMessageId = -1;
            if (!retroactive) {
                s = c.getConnection().prepareStatement(this.statements.getFindLastSequenceIdInMsgsStatement());
                ResultSet rs = null;
                try {
                    rs = s.executeQuery();
                    if (rs.next()) {
                        lastMessageId = rs.getLong(1);
                    }
                } finally {
                    close(rs);
                    close(s);
                }
            }
            s = c.getConnection().prepareStatement(this.statements.getCreateDurableSubStatement());
            s.setString(1, info.getDestination().getQualifiedName());
            s.setString(2, info.getClientId());
            s.setString(3, info.getSubscriptionName());
            s.setString(4, info.getSelector());
            s.setLong(5, lastMessageId);
            s.setString(6, info.getSubscribedDestination().getQualifiedName());
            if (s.executeUpdate() != 1) {
                throw new IOException("Could not create durable subscription for: " + info.getClientId());
            }
        } finally {
            close(s);
        }
    }

    public SubscriptionInfo doGetSubscriberEntry(TransactionContext c, ActiveMQDestination destination,
            String clientId, String subscriptionName) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindDurableSubStatement());
            s.setString(1, destination.getQualifiedName());
            s.setString(2, clientId);
            s.setString(3, subscriptionName);
            rs = s.executeQuery();
            if (!rs.next()) {
                return null;
            }
            SubscriptionInfo subscription = new SubscriptionInfo();
            subscription.setDestination(destination);
            subscription.setClientId(clientId);
            subscription.setSubscriptionName(subscriptionName);
            subscription.setSelector(rs.getString(1));
            subscription.setSubscribedDestination(ActiveMQDestination.createDestination(rs.getString(2),
                    ActiveMQDestination.QUEUE_TYPE));
            return subscription;
        } finally {
            close(rs);
            close(s);
        }
    }

    public SubscriptionInfo[] doGetAllSubscriptions(TransactionContext c, ActiveMQDestination destination)
            throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindAllDurableSubsStatement());
            s.setString(1, destination.getQualifiedName());
            rs = s.executeQuery();
            ArrayList<SubscriptionInfo> rc = new ArrayList<SubscriptionInfo>();
            while (rs.next()) {
                SubscriptionInfo subscription = new SubscriptionInfo();
                subscription.setDestination(destination);
                subscription.setSelector(rs.getString(1));
                subscription.setSubscriptionName(rs.getString(2));
                subscription.setClientId(rs.getString(3));
                subscription.setSubscribedDestination(ActiveMQDestination.createDestination(rs.getString(4),
                        ActiveMQDestination.QUEUE_TYPE));
                rc.add(subscription);
            }
            return rc.toArray(new SubscriptionInfo[rc.size()]);
        } finally {
            close(rs);
            close(s);
        }
    }

    public void doRemoveAllMessages(TransactionContext c, ActiveMQDestination destinationName) throws SQLException,
            IOException {
        PreparedStatement s = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getRemoveAllMessagesStatement());
            s.setString(1, destinationName.getQualifiedName());
            s.executeUpdate();
            s.close();
            s = c.getConnection().prepareStatement(this.statements.getRemoveAllSubscriptionsStatement());
            s.setString(1, destinationName.getQualifiedName());
            s.executeUpdate();
        } finally {
            close(s);
        }
    }

    public void doDeleteSubscription(TransactionContext c, ActiveMQDestination destination, String clientId,
            String subscriptionName) throws SQLException, IOException {
        PreparedStatement s = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getDeleteSubscriptionStatement());
            s.setString(1, destination.getQualifiedName());
            s.setString(2, clientId);
            s.setString(3, subscriptionName);
            s.executeUpdate();
        } finally {
            close(s);
        }
    }

    public void doDeleteOldMessages(TransactionContext c) throws SQLException, IOException {
        PreparedStatement s = null;
        try {
            LOG.debug("Executing SQL: " + this.statements.getDeleteOldMessagesStatement());
            s = c.getConnection().prepareStatement(this.statements.getDeleteOldMessagesStatement());
            s.setLong(1, System.currentTimeMillis());
            int i = s.executeUpdate();
            LOG.debug("Deleted " + i + " old message(s).");
        } finally {
            close(s);
        }
    }

    public long doGetLastAckedDurableSubscriberMessageId(TransactionContext c, ActiveMQDestination destination,
            String clientId, String subscriberName) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        long result = -1;
        try {
            s = c.getConnection().prepareStatement(this.statements.getLastAckedDurableSubscriberMessageStatement());
            s.setString(1, destination.getQualifiedName());
            s.setString(2, clientId);
            s.setString(3, subscriberName);
            rs = s.executeQuery();
            if (rs.next()) {
                result = rs.getLong(1);
            }
            rs.close();
            s.close();
        } finally {
            close(rs);
            close(s);
        }
        return result;
    }

    private static void close(PreparedStatement s) {
        try {
            s.close();
        } catch (Throwable e) {
        }
    }

    private static void close(ResultSet rs) {
        try {
            rs.close();
        } catch (Throwable e) {
        }
    }

    public Set<ActiveMQDestination> doGetDestinations(TransactionContext c) throws SQLException, IOException {
        HashSet<ActiveMQDestination> rc = new HashSet<ActiveMQDestination>();
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindAllDestinationsStatement());
            rs = s.executeQuery();
            while (rs.next()) {
                rc.add(ActiveMQDestination.createDestination(rs.getString(1), ActiveMQDestination.QUEUE_TYPE));
            }
        } finally {
            close(rs);
            close(s);
        }
        return rc;
    }

    /**
     * @return true if batchStements
     */
    public boolean isBatchStatments() {
        return this.batchStatments;
    }

    /**
     * @param batchStatments
     */
    public void setBatchStatments(boolean batchStatments) {
        this.batchStatments = batchStatments;
    }

    public void setUseExternalMessageReferences(boolean useExternalMessageReferences) {
        this.statements.setUseExternalMessageReferences(useExternalMessageReferences);
    }

    /**
     * @return the statements
     */
    public Statements getStatements() {
        return this.statements;
    }

    public void setStatements(Statements statements) {
        this.statements = statements;
    }

    /**
     * @param c
     * @param destination
     * @param clientId
     * @param subscriberName
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public byte[] doGetNextDurableSubscriberMessageStatement(TransactionContext c, ActiveMQDestination destination,
            String clientId, String subscriberName) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getNextDurableSubscriberMessageStatement());
            s.setString(1, destination.getQualifiedName());
            s.setString(2, clientId);
            s.setString(3, subscriberName);
            rs = s.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return getBinaryData(rs, 1);
        } finally {
            close(rs);
            close(s);
        }
    }

    public int doGetMessageCount(TransactionContext c, ActiveMQDestination destination) throws SQLException,
            IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        int result = 0;
        try {
            s = c.getConnection().prepareStatement(this.statements.getDestinationMessageCountStatement());
            s.setString(1, destination.getQualifiedName());
            rs = s.executeQuery();
            if (rs.next()) {
                result = rs.getInt(1);
            }
        } finally {
            close(rs);
            close(s);
        }
        return result;
    }

    public void doRecoverNextMessages(TransactionContext c, ActiveMQDestination destination, long nextSeq,
            int maxReturned, JDBCMessageRecoveryListener listener) throws Exception {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindNextMessagesStatement());
            s.setMaxRows(maxReturned * 2);
            s.setString(1, destination.getQualifiedName());
            s.setLong(2, nextSeq);
            rs = s.executeQuery();
            int count = 0;
            if (this.statements.isUseExternalMessageReferences()) {
                while (rs.next() && count < maxReturned) {
                    if (listener.recoverMessageReference(rs.getString(1))) {
                        count++;
                    } else {
                        LOG.debug("Stopped recover next messages");
                        break;
                    }
                }
            } else {
                while (rs.next() && count < maxReturned) {
                    if (listener.recoverMessage(rs.getLong(1), getBinaryData(rs, 2))) {
                        count++;
                    } else {
                        LOG.debug("Stopped recover next messages");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(rs);
            close(s);
        }
    }
    /*
     * Useful for debugging. public void dumpTables(Connection c, String destinationName, String clientId, String
     * subscriptionName) throws SQLException { printQuery(c, "Select * from ACTIVEMQ_MSGS", System.out); printQuery(c,
     * "Select * from ACTIVEMQ_ACKS", System.out); PreparedStatement s = c.prepareStatement("SELECT M.ID,
     * D.LAST_ACKED_ID FROM " +"ACTIVEMQ_MSGS M, " +"ACTIVEMQ_ACKS D " +"WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND
     * D.SUB_NAME=?" +" AND M.CONTAINER=D.CONTAINER AND M.ID > D.LAST_ACKED_ID" +" ORDER BY M.ID");
     * s.setString(1,destinationName); s.setString(2,clientId); s.setString(3,subscriptionName);
     * printQuery(s,System.out); }
     * 
     * public void dumpTables(Connection c) throws SQLException { printQuery(c, "Select * from ACTIVEMQ_MSGS",
     * System.out); printQuery(c, "Select * from ACTIVEMQ_ACKS", System.out); }
     * 
     * private void printQuery(Connection c, String query, PrintStream out) throws SQLException {
     * printQuery(c.prepareStatement(query), out); }
     * 
     * private void printQuery(PreparedStatement s, PrintStream out) throws SQLException {
     * 
     * ResultSet set=null; try { set = s.executeQuery(); ResultSetMetaData metaData = set.getMetaData(); for( int i=1; i<=
     * metaData.getColumnCount(); i++ ) { if(i==1) out.print("||"); out.print(metaData.getColumnName(i)+"||"); }
     * out.println(); while(set.next()) { for( int i=1; i<= metaData.getColumnCount(); i++ ) { if(i==1) out.print("|");
     * out.print(set.getString(i)+"|"); } out.println(); } } finally { try { set.close(); } catch (Throwable ignore) {}
     * try { s.close(); } catch (Throwable ignore) {} } }
     */

}

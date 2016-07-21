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

import static javax.xml.bind.DatatypeConverter.parseBase64Binary;
import static javax.xml.bind.DatatypeConverter.printBase64Binary;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.jdbc.JDBCAdapter;
import org.apache.activemq.store.jdbc.JDBCMessageIdScanListener;
import org.apache.activemq.store.jdbc.JDBCMessageRecoveryListener;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.jdbc.JdbcMemoryTransactionStore;
import org.apache.activemq.store.jdbc.Statements;
import org.apache.activemq.store.jdbc.TransactionContext;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 */
public class DefaultJDBCAdapter implements JDBCAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultJDBCAdapter.class);
    public static final int MAX_ROWS = org.apache.activemq.ActiveMQPrefetchPolicy.MAX_PREFETCH_SIZE;
    private static final String FAILURE_MESSAGE = "Failure was: %s Message: %s SQLState: %s Vendor code: %s";
    protected Statements statements;
    private boolean batchStatements = true;
    //This is deprecated and should be removed in a future release
    protected boolean batchStatments = true;
    protected boolean prioritizedMessages;
    protected int maxRows = MAX_ROWS;

    protected void setBinaryData(PreparedStatement s, int index, byte data[]) throws SQLException {
        s.setBytes(index, data);
    }

    protected byte[] getBinaryData(ResultSet rs, int index) throws SQLException {
        return rs.getBytes(index);
    }

    @Override
    public void doCreateTables(TransactionContext transactionContext) throws SQLException, IOException {
        // Check to see if the table already exists. If it does, then don't log warnings during startup.
        // Need to run the scripts anyways since they may contain ALTER statements that upgrade a previous version of the table
        boolean messageTableAlreadyExists = messageTableAlreadyExists(transactionContext);

        for (String createStatement : this.statements.getCreateSchemaStatements()) {
            // This will fail usually since the tables will be
            // created already.
            executeStatement(transactionContext, createStatement, messageTableAlreadyExists);
        }
    }

    private boolean messageTableAlreadyExists(TransactionContext transactionContext) {
        boolean alreadyExists = false;
        ResultSet rs = null;
        try {
            rs = transactionContext.getConnection().getMetaData().getTables(null, null, this.statements.getFullMessageTableName(), new String[] { "TABLE" });
            alreadyExists = rs.next();
        } catch (Throwable ignore) {
        } finally {
            close(rs);
        }
        return alreadyExists;
    }

    private void executeStatement(TransactionContext transactionContext, String createStatement, boolean ignoreStatementExecutionFailure) throws IOException {
        Statement statement = null;
        try {
            LOG.debug("Executing SQL: " + createStatement);
            statement = transactionContext.getConnection().createStatement();
            statement.execute(createStatement);

            commitIfAutoCommitIsDisabled(transactionContext);
        } catch (SQLException e) {
            if (ignoreStatementExecutionFailure) {
                LOG.debug("Could not create JDBC tables; The message table already existed. " + String.format(FAILURE_MESSAGE, createStatement, e.getMessage(), e.getSQLState(), e.getErrorCode()));
            } else {
                LOG.warn("Could not create JDBC tables; they could already exist. " + String.format(FAILURE_MESSAGE, createStatement, e.getMessage(), e.getSQLState(), e.getErrorCode()));
                JDBCPersistenceAdapter.log("Failure details: ", e);
            }
        } finally {
            closeStatement(statement);
        }
    }

    private void closeStatement(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException ignored) {}
    }

    private void commitIfAutoCommitIsDisabled(TransactionContext c) throws SQLException, IOException {
        if (!c.getConnection().getAutoCommit()) {
            c.getConnection().commit();
        }
    }

    @Override
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
            commitIfAutoCommitIsDisabled(c);
        } finally {
            try {
                s.close();
            } catch (Throwable e) {
            }
        }
    }

    @Override
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
            long seq = Math.max(seq1, seq2);
            return seq;
        } finally {
            close(rs);
            close(s);
        }
    }

    @Override
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


    /**
     * A non null xid indicated the op is part of 2pc prepare, so ops are flagged pending outcome
     */
    @Override
    public void doAddMessage(TransactionContext c, long sequence, MessageId messageID, ActiveMQDestination destination, byte[] data,
                             long expiration, byte priority, XATransactionId xid) throws SQLException, IOException {
        PreparedStatement s = c.getAddMessageStatement();
        try {
            if (s == null) {
                s = c.getConnection().prepareStatement(this.statements.getAddMessageStatement());
                if (this.batchStatements) {
                    c.setAddMessageStatement(s);
                }
            }
            s.setLong(1, sequence);
            s.setString(2, messageID.getProducerId().toString());
            s.setLong(3, messageID.getProducerSequenceId());
            s.setString(4, destination.getQualifiedName());
            s.setLong(5, expiration);
            s.setLong(6, priority);
            setBinaryData(s, 7, data);
            if (xid != null) {
                byte[] xidVal = xid.getEncodedXidBytes();
                xidVal[0] = '+';
                String xidString = printBase64Binary(xidVal);
                s.setString(8, xidString);
            } else {
                s.setString(8, null);
            }
            if (this.batchStatements) {
                s.addBatch();
            } else if (s.executeUpdate() != 1) {
                throw new SQLException("Failed add a message");
            }
        } finally {
            if (!this.batchStatements) {
                if (s != null) {
                    s.close();
                }
            }
        }
    }

    @Override
    public void doUpdateMessage(TransactionContext c, ActiveMQDestination destination, MessageId id, byte[] data) throws SQLException, IOException {
        PreparedStatement s = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getUpdateMessageStatement());
            setBinaryData(s, 1, data);
            s.setString(2, id.getProducerId().toString());
            s.setLong(3, id.getProducerSequenceId());
            s.setString(4, destination.getQualifiedName());
            if (s.executeUpdate() != 1) {
                throw new IOException("Could not update message: " + id + " in " + destination);
            }
        } finally {
            close(s);
        }
    }


    @Override
    public void doAddMessageReference(TransactionContext c, long sequence, MessageId messageID, ActiveMQDestination destination,
            long expirationTime, String messageRef) throws SQLException, IOException {
        PreparedStatement s = c.getAddMessageStatement();
        try {
            if (s == null) {
                s = c.getConnection().prepareStatement(this.statements.getAddMessageStatement());
                if (this.batchStatements) {
                    c.setAddMessageStatement(s);
                }
            }
            s.setLong(1, messageID.getBrokerSequenceId());
            s.setString(2, messageID.getProducerId().toString());
            s.setLong(3, messageID.getProducerSequenceId());
            s.setString(4, destination.getQualifiedName());
            s.setLong(5, expirationTime);
            s.setString(6, messageRef);
            if (this.batchStatements) {
                s.addBatch();
            } else if (s.executeUpdate() != 1) {
                throw new SQLException("Failed add a message");
            }
        } finally {
            if (!this.batchStatements) {
                s.close();
            }
        }
    }

    @Override
    public long[] getStoreSequenceId(TransactionContext c, ActiveMQDestination destination, MessageId messageID) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindMessageSequenceIdStatement());
            s.setString(1, messageID.getProducerId().toString());
            s.setLong(2, messageID.getProducerSequenceId());
            s.setString(3, destination.getQualifiedName());
            rs = s.executeQuery();
            if (!rs.next()) {
                return new long[]{0,0};
            }
            return new long[]{rs.getLong(1), rs.getLong(2)};
        } finally {
            close(rs);
            close(s);
        }
    }

    @Override
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

    @Override
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

    /**
     * A non null xid indicated the op is part of 2pc prepare, so ops are flagged pending outcome
     */
    @Override
    public void doRemoveMessage(TransactionContext c, long seq, XATransactionId xid) throws SQLException, IOException {
        PreparedStatement s = c.getRemovedMessageStatement();
        try {
            if (s == null) {
                s = c.getConnection().prepareStatement(xid == null ?
                        this.statements.getRemoveMessageStatement() : this.statements.getUpdateXidFlagStatement());
                if (this.batchStatements) {
                    c.setRemovedMessageStatement(s);
                }
            }
            if (xid == null) {
                s.setLong(1, seq);
            } else {
                byte[] xidVal = xid.getEncodedXidBytes();
                xidVal[0] = '-';
                String xidString = printBase64Binary(xidVal);
                s.setString(1, xidString);
                s.setLong(2, seq);
            }
            if (this.batchStatements) {
                s.addBatch();
            } else if (s.executeUpdate() != 1) {
                throw new SQLException("Failed to remove message seq: " + seq);
            }
        } finally {
            if (!this.batchStatements && s != null) {
                s.close();
            }
        }
    }

    @Override
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

    @Override
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

    @Override
    public void doSetLastAckWithPriority(TransactionContext c, ActiveMQDestination destination, XATransactionId xid, String clientId,
                                         String subscriptionName, long seq, long priority) throws SQLException, IOException {
        PreparedStatement s = c.getUpdateLastAckStatement();
        try {
            if (s == null) {
                s = c.getConnection().prepareStatement(xid == null ?
                        this.statements.getUpdateDurableLastAckWithPriorityStatement() :
                        this.statements.getUpdateDurableLastAckWithPriorityInTxStatement());
                if (this.batchStatements) {
                    c.setUpdateLastAckStatement(s);
                }
            }
            if (xid != null) {
                byte[] xidVal = encodeXid(xid, seq, priority);
                String xidString = printBase64Binary(xidVal);
                s.setString(1, xidString);
            } else {
                s.setLong(1, seq);
            }
            s.setString(2, destination.getQualifiedName());
            s.setString(3, clientId);
            s.setString(4, subscriptionName);
            s.setLong(5, priority);
            if (this.batchStatements) {
                s.addBatch();
            } else if (s.executeUpdate() != 1) {
                throw new SQLException("Failed update last ack with priority: " + priority + ", for sub: " + subscriptionName);
            }
        } finally {
            if (!this.batchStatements) {
                close(s);
            }
        }
    }


    @Override
    public void doSetLastAck(TransactionContext c, ActiveMQDestination destination, XATransactionId xid, String clientId,
                             String subscriptionName, long seq, long priority) throws SQLException, IOException {
        PreparedStatement s = c.getUpdateLastAckStatement();
        try {
            if (s == null) {
                s = c.getConnection().prepareStatement(xid == null ?
                        this.statements.getUpdateDurableLastAckStatement() :
                        this.statements.getUpdateDurableLastAckInTxStatement());
                if (this.batchStatements) {
                    c.setUpdateLastAckStatement(s);
                }
            }
            if (xid != null) {
                byte[] xidVal = encodeXid(xid, seq, priority);
                String xidString = printBase64Binary(xidVal);
                s.setString(1, xidString);
            } else {
                s.setLong(1, seq);
            }
            s.setString(2, destination.getQualifiedName());
            s.setString(3, clientId);
            s.setString(4, subscriptionName);

            if (this.batchStatements) {
                s.addBatch();
            } else if (s.executeUpdate() != 1) {
                throw new IOException("Could not update last ack seq : "
                            + seq + ", for sub: " + subscriptionName);
            }
        } finally {
            if (!this.batchStatements) {
                close(s);
            }
        }
    }

    private byte[] encodeXid(XATransactionId xid, long seq, long priority) {
        byte[] xidVal = xid.getEncodedXidBytes();
        // encode the update
        DataByteArrayOutputStream outputStream = xid.internalOutputStream();
        outputStream.position(1);
        outputStream.writeLong(seq);
        outputStream.writeByte(Long.valueOf(priority).byteValue());
        return xidVal;
    }

    @Override
    public void doClearLastAck(TransactionContext c, ActiveMQDestination destination, byte priority, String clientId, String subName) throws SQLException, IOException {
        PreparedStatement s = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getClearDurableLastAckInTxStatement());
            s.setString(1, destination.getQualifiedName());
            s.setString(2, clientId);
            s.setString(3, subName);
            s.setLong(4, priority);
            if (s.executeUpdate() != 1) {
                throw new IOException("Could not remove prepared transaction state from message ack for: " + clientId + ":" + subName);
            }
        } finally {
            close(s);
        }
    }

    @Override
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

    @Override
    public void doRecoverNextMessages(TransactionContext c, ActiveMQDestination destination, String clientId,
            String subscriptionName, long seq, long priority, int maxReturned, JDBCMessageRecoveryListener listener) throws Exception {

        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindDurableSubMessagesStatement());
            s.setMaxRows(Math.min(maxReturned * 2, maxRows));
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
                    }
                }
            } else {
                while (rs.next() && count < maxReturned) {
                    if (listener.recoverMessage(rs.getLong(1), getBinaryData(rs, 2))) {
                        count++;
                    }
                }
            }
        } finally {
            close(rs);
            close(s);
        }
    }

    @Override
    public void doRecoverNextMessagesWithPriority(TransactionContext c, ActiveMQDestination destination, String clientId,
            String subscriptionName, long seq, long priority, int maxReturned, JDBCMessageRecoveryListener listener) throws Exception {

        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindDurableSubMessagesByPriorityStatement());
            s.setMaxRows(Math.min(maxReturned * 2, maxRows));
            s.setString(1, destination.getQualifiedName());
            s.setString(2, clientId);
            s.setString(3, subscriptionName);
            s.setLong(4, seq);
            s.setLong(5, priority);
            rs = s.executeQuery();
            int count = 0;
            if (this.statements.isUseExternalMessageReferences()) {
                while (rs.next() && count < maxReturned) {
                    if (listener.recoverMessageReference(rs.getString(1))) {
                        count++;
                    }
                }
            } else {
                while (rs.next() && count < maxReturned) {
                    if (listener.recoverMessage(rs.getLong(1), getBinaryData(rs, 2))) {
                        count++;
                    }
                }
            }
        } finally {
            close(rs);
            close(s);
        }
    }

    @Override
    public int doGetDurableSubscriberMessageCount(TransactionContext c, ActiveMQDestination destination,
            String clientId, String subscriptionName, boolean isPrioritizedMessages) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        int result = 0;
        try {
            if (isPrioritizedMessages) {
                s = c.getConnection().prepareStatement(this.statements.getDurableSubscriberMessageCountStatementWithPriority());
            } else {
                s = c.getConnection().prepareStatement(this.statements.getDurableSubscriberMessageCountStatement());
            }
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
     */
    @Override
    public void doSetSubscriberEntry(TransactionContext c, SubscriptionInfo info, boolean retroactive, boolean isPrioritizedMessages)
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
            int maxPriority = 1;
            if (isPrioritizedMessages) {
                maxPriority = 10;
            }

            for (int priority = 0; priority < maxPriority; priority++) {
                s.setString(1, info.getDestination().getQualifiedName());
                s.setString(2, info.getClientId());
                s.setString(3, info.getSubscriptionName());
                s.setString(4, info.getSelector());
                s.setLong(5, lastMessageId);
                s.setString(6, info.getSubscribedDestination().getQualifiedName());
                s.setLong(7, priority);

                if (s.executeUpdate() != 1) {
                    throw new IOException("Could not create durable subscription for: " + info.getClientId());
                }
            }

        } finally {
            close(s);
        }
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
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

    char priorityIterator = 0; // unsigned
    @Override
    public void doDeleteOldMessages(TransactionContext c) throws SQLException, IOException {
        PreparedStatement s = null;
        try {
            LOG.debug("Executing SQL: " + this.statements.getDeleteOldMessagesStatementWithPriority());
            s = c.getExclusiveConnection().prepareStatement(this.statements.getDeleteOldMessagesStatementWithPriority());
            int priority = priorityIterator++%10;
            s.setInt(1, priority);
            s.setInt(2, priority);
            int i = s.executeUpdate();
            LOG.debug("Deleted " + i + " old message(s) at priority: " + priority);
        } finally {
            close(s);
        }
    }

    @Override
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
                if (result == 0 && rs.wasNull()) {
                    result = -1;
                }
            }
        } finally {
            close(rs);
            close(s);
        }
        return result;
    }

    protected static void close(PreparedStatement s) {
        try {
            s.close();
        } catch (Throwable e) {
        }
    }

    protected static void close(ResultSet rs) {
        try {
            rs.close();
        } catch (Throwable e) {
        }
    }

    @Override
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
     * @return true if batchStatements
     */
    public boolean isBatchStatements() {
        return batchStatements;
    }

    /**
     * Set the number of statements to process as a single batch DB update
     * @param batchStatements
     */
    public void setBatchStatements(boolean batchStatements) {
        this.batchStatements = batchStatements;
        // The next lines are deprecated and should be removed in a future release
        // and is here in case someone created their own
       // this.batchStatments = batchStatements;
    }

    // Note - remove batchStatment in future distributions.  Here for backward compatibility
    /**
     * @return true if batchStements
     */
    public boolean isBatchStatments() {
        return this.batchStatements;
    }

    /**
     * This value batchStatments is deprecated and will be removed in a future release.  Use batchStatements instead (Note the 'e' in Statement)"
     * @deprecated
     * @param batchStatments
     */
    public void setBatchStatments(boolean batchStatments) {
        LOG.warn("batchStatments is deprecated and will be removed in a future release.  Use batchStatements instead (Note the 'e' in Statement)");
        this.batchStatements = batchStatments;
        this.batchStatments = batchStatments;
    }

    @Override
    public void setUseExternalMessageReferences(boolean useExternalMessageReferences) {
        this.statements.setUseExternalMessageReferences(useExternalMessageReferences);
    }

    /**
     * @return the statements
     */
    public Statements getStatements() {
        return this.statements;
    }

    @Override
    public void setStatements(Statements statements) {
        this.statements = statements;
    }

    @Override
    public int getMaxRows() {
        return maxRows;
    }

    /**
     * the max value for statement maxRows, used to limit jdbc queries
     */
    @Override
    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    @Override
    public void doRecordDestination(TransactionContext c, ActiveMQDestination destination) throws SQLException, IOException {
        PreparedStatement s = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getCreateDurableSubStatement());
            s.setString(1, destination.getQualifiedName());
            s.setString(2, destination.getQualifiedName());
            s.setString(3, destination.getQualifiedName());
            s.setString(4, null);
            s.setLong(5, 0);
            s.setString(6, destination.getQualifiedName());
            s.setLong(7, 11);  // entry out of priority range

            if (s.executeUpdate() != 1) {
                throw new IOException("Could not create ack record for destination: " + destination);
            }
        } finally {
            close(s);
        }
    }

    @Override
    public void doRecoverPreparedOps(TransactionContext c, JdbcMemoryTransactionStore jdbcMemoryTransactionStore) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getFindOpsPendingOutcomeStatement());
            rs = s.executeQuery();
            while (rs.next()) {
                long id = rs.getLong(1);
                String encodedString = rs.getString(2);
                byte[] encodedXid = parseBase64Binary(encodedString);
                if (encodedXid[0] == '+') {
                    jdbcMemoryTransactionStore.recoverAdd(id, getBinaryData(rs, 3));
                } else {
                    jdbcMemoryTransactionStore.recoverAck(id, encodedXid, getBinaryData(rs, 3));
                }
            }

            close(rs);
            close(s);

            s = c.getConnection().prepareStatement(this.statements.getFindAcksPendingOutcomeStatement());
            rs = s.executeQuery();
            while (rs.next()) {
                String encodedString = rs.getString(1);
                byte[] encodedXid = parseBase64Binary(encodedString);
                String destination = rs.getString(2);
                String subName = rs.getString(3);
                String subId = rs.getString(4);
                jdbcMemoryTransactionStore.recoverLastAck(encodedXid,
                        ActiveMQDestination.createDestination(destination, ActiveMQDestination.TOPIC_TYPE),
                        subName, subId);
            }
        } finally {
            close(rs);
            close(s);
        }
    }

    @Override
    public void doCommitAddOp(TransactionContext c, long preparedSequence, long sequence) throws SQLException, IOException {
        PreparedStatement s = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getClearXidFlagStatement());
            s.setLong(1, sequence);
            s.setLong(2, preparedSequence);
            if (s.executeUpdate() != 1) {
                throw new IOException("Could not remove prepared transaction state from message add for sequenceId: " + sequence);
            }
        } finally {
            close(s);
        }
    }


    @Override
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

    @Override
    public void doRecoverNextMessages(TransactionContext c, ActiveMQDestination destination, long[] lastRecoveredEntries,
            long maxSeq, int maxReturned, boolean isPrioritizedMessages, JDBCMessageRecoveryListener listener) throws Exception {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            if (isPrioritizedMessages) {
                s = c.getConnection().prepareStatement(limitQuery(this.statements.getFindNextMessagesByPriorityStatement()));
            } else {
                s = c.getConnection().prepareStatement(limitQuery(this.statements.getFindNextMessagesStatement()));
            }
            s.setMaxRows(Math.min(maxReturned, maxRows));
            s.setString(1, destination.getQualifiedName());
            s.setLong(2, maxSeq);
            int paramId = 3;
            if (isPrioritizedMessages) {
                for (int i=9;i>=0;i--) {
                    s.setLong(paramId++, lastRecoveredEntries[i]);
                }
            } else {
                s.setLong(paramId, lastRecoveredEntries[0]);
            }
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
            LOG.warn("Exception recovering next messages", e);
        } finally {
            close(rs);
            close(s);
        }
    }

    @Override
    public long doGetLastProducerSequenceId(TransactionContext c, ProducerId id)
            throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            s = c.getConnection().prepareStatement(this.statements.getLastProducerSequenceIdStatement());
            s.setString(1, id.toString());
            rs = s.executeQuery();
            long seq = -1;
            if (rs.next()) {
                seq = rs.getLong(1);
            }
            return seq;
        } finally {
            close(rs);
            close(s);
        }
    }

    public static void dumpTables(Connection c, String destinationName, String clientId, String
      subscriptionName) throws SQLException {
        printQuery(c, "Select * from ACTIVEMQ_MSGS", System.out);
        printQuery(c, "Select * from ACTIVEMQ_ACKS", System.out);
        PreparedStatement s = c.prepareStatement("SELECT M.ID, D.LAST_ACKED_ID FROM "
                + "ACTIVEMQ_MSGS M, " +"ACTIVEMQ_ACKS D "
                + "WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND D.SUB_NAME=?"
                + " AND M.CONTAINER=D.CONTAINER AND M.ID > D.LAST_ACKED_ID"
                + " ORDER BY M.ID");
      s.setString(1,destinationName); s.setString(2,clientId); s.setString(3,subscriptionName);
      printQuery(s,System.out); }

    public static void dumpTables(java.sql.Connection c) throws SQLException {
        printQuery(c, "SELECT COUNT(*) from ACTIVEMQ_MSGS", System.out);

        //printQuery(c, "SELECT COUNT(*) from ACTIVEMQ_ACKS", System.out);

        //printQuery(c, "Select * from ACTIVEMQ_MSGS ORDER BY ID", System.out);
        //printQuery(c, "Select * from ACTIVEMQ_ACKS", System.out);
    }

    public static void printQuery(java.sql.Connection c, String query, java.io.PrintStream out)
            throws SQLException {
        printQuery(c.prepareStatement(query), out);
    }

    public static void printQuery(java.sql.PreparedStatement s, java.io.PrintStream out)
            throws SQLException {

        ResultSet set = null;
        try {
            set = s.executeQuery();
            java.sql.ResultSetMetaData metaData = set.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (i == 1)
                    out.print("||");
                out.print(metaData.getColumnName(i) + "||");
            }
            out.println();
            while (set.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    if (i == 1)
                        out.print("|");
                    out.print(set.getString(i) + "|");
                }
                out.println();
            }
        } finally {
            try {
                set.close();
            } catch (Throwable ignore) {
            }
            try {
                s.close();
            } catch (Throwable ignore) {
            }
        }
    }

    @Override
    public String limitQuery(String query) {
        return query;
    }
}

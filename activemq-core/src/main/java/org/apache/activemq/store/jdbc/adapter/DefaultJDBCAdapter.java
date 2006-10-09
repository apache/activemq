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

package org.apache.activemq.store.jdbc.adapter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.jdbc.JDBCAdapter;
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
public class DefaultJDBCAdapter implements JDBCAdapter{

    private static final Log log=LogFactory.getLog(DefaultJDBCAdapter.class);
    protected Statements statements;
    protected boolean batchStatments=true;

    protected void setBinaryData(PreparedStatement s,int index,byte data[]) throws SQLException{
        s.setBytes(index,data);
    }

    protected byte[] getBinaryData(ResultSet rs,int index) throws SQLException{
        return rs.getBytes(index);
    }

    public void doCreateTables(TransactionContext c) throws SQLException,IOException{
        Statement s=null;
        try{
            // Check to see if the table already exists. If it does, then don't log warnings during startup.
            // Need to run the scripts anyways since they may contain ALTER statements that upgrade a previous version
            // of the table
            boolean alreadyExists=false;
            ResultSet rs=null;
            try{
                rs=c.getConnection().getMetaData().getTables(null,null,statements.getFullMessageTableName(),
                        new String[] { "TABLE" });
                alreadyExists=rs.next();
            }catch(Throwable ignore){
            }finally{
                close(rs);
            }
            s=c.getConnection().createStatement();
            String[] createStatments=statements.getCreateSchemaStatements();
            for(int i=0;i<createStatments.length;i++){
                // This will fail usually since the tables will be
                // created already.
                try{
                    log.debug("Executing SQL: "+createStatments[i]);
                    boolean rc=s.execute(createStatments[i]);
                }catch(SQLException e){
                    if(alreadyExists){
                        log.debug("Could not create JDBC tables; The message table already existed."+" Failure was: "
                                +createStatments[i]+" Message: "+e.getMessage()+" SQLState: "+e.getSQLState()
                                +" Vendor code: "+e.getErrorCode());
                    }else{
                        log.warn("Could not create JDBC tables; they could already exist."+" Failure was: "
                                +createStatments[i]+" Message: "+e.getMessage()+" SQLState: "+e.getSQLState()
                                +" Vendor code: "+e.getErrorCode());
                        JDBCPersistenceAdapter.log("Failure details: ",e);
                    }
                }
            }
            c.getConnection().commit();
        }finally{
            try{
                s.close();
            }catch(Throwable e){
            }
        }
    }

    public void doDropTables(TransactionContext c) throws SQLException,IOException{
        Statement s=null;
        try{
            s=c.getConnection().createStatement();
            String[] dropStatments=statements.getDropSchemaStatements();
            for(int i=0;i<dropStatments.length;i++){
                // This will fail usually since the tables will be
                // created already.
                try{
                    boolean rc=s.execute(dropStatments[i]);
                }catch(SQLException e){
                    log.warn("Could not drop JDBC tables; they may not exist."+" Failure was: "+dropStatments[i]
                            +" Message: "+e.getMessage()+" SQLState: "+e.getSQLState()+" Vendor code: "
                            +e.getErrorCode());
                    JDBCPersistenceAdapter.log("Failure details: ",e);
                }
            }
            c.getConnection().commit();
        }finally{
            try{
                s.close();
            }catch(Throwable e){
            }
        }
    }

    public long doGetLastMessageBrokerSequenceId(TransactionContext c) throws SQLException,IOException{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getFindLastSequenceIdInMsgsStatement());
            rs=s.executeQuery();
            long seq1=0;
            if(rs.next()){
                seq1=rs.getLong(1);
            }
            rs.close();
            s.close();
            s=c.getConnection().prepareStatement(statements.getFindLastSequenceIdInAcksStatement());
            rs=s.executeQuery();
            long seq2=0;
            if(rs.next()){
                seq2=rs.getLong(1);
            }
            return Math.max(seq1,seq2);
        }finally{
            close(rs);
            close(s);
        }
    }

    public void doAddMessage(TransactionContext c,MessageId messageID,ActiveMQDestination destination,byte[] data,
            long expiration) throws SQLException,IOException{
        PreparedStatement s=c.getAddMessageStatement();
        try{
            if(s==null){
                s=c.getConnection().prepareStatement(statements.getAddMessageStatement());
                if(batchStatments){
                    c.setAddMessageStatement(s);
                }
            }
            s.setLong(1,messageID.getBrokerSequenceId());
            s.setString(2,messageID.getProducerId().toString());
            s.setLong(3,messageID.getProducerSequenceId());
            s.setString(4,destination.getQualifiedName());
            s.setLong(5,expiration);
            setBinaryData(s,6,data);
            if(batchStatments){
                s.addBatch();
            }else if(s.executeUpdate()!=1){
                throw new SQLException("Failed add a message");
            }
        }finally{
            if(!batchStatments){
                s.close();
            }
        }
    }

    public void doAddMessageReference(TransactionContext c,MessageId messageID,ActiveMQDestination destination,
            long expirationTime,String messageRef) throws SQLException,IOException{
        PreparedStatement s=c.getAddMessageStatement();
        try{
            if(s==null){
                s=c.getConnection().prepareStatement(statements.getAddMessageStatement());
                if(batchStatments){
                    c.setAddMessageStatement(s);
                }
            }
            s.setLong(1,messageID.getBrokerSequenceId());
            s.setString(2,messageID.getProducerId().toString());
            s.setLong(3,messageID.getProducerSequenceId());
            s.setString(4,destination.getQualifiedName());
            s.setLong(5,expirationTime);
            s.setString(6,messageRef);
            if(batchStatments){
                s.addBatch();
            }else if(s.executeUpdate()!=1){
                throw new SQLException("Failed add a message");
            }
        }finally{
            if(!batchStatments){
                s.close();
            }
        }
    }

    public long getBrokerSequenceId(TransactionContext c,MessageId messageID) throws SQLException,IOException{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getFindMessageSequenceIdStatement());
            s.setString(1,messageID.getProducerId().toString());
            s.setLong(2,messageID.getProducerSequenceId());
            rs=s.executeQuery();
            if(!rs.next()){
                return 0;
            }
            return rs.getLong(1);
        }finally{
            close(rs);
            close(s);
        }
    }

    public byte[] doGetMessage(TransactionContext c,long seq) throws SQLException,IOException{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getFindMessageStatement());
            s.setLong(1,seq);
            rs=s.executeQuery();
            if(!rs.next()){
                return null;
            }
            return getBinaryData(rs,1);
        }finally{
            close(rs);
            close(s);
        }
    }

    public String doGetMessageReference(TransactionContext c,long seq) throws SQLException,IOException{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getFindMessageStatement());
            s.setLong(1,seq);
            rs=s.executeQuery();
            if(!rs.next()){
                return null;
            }
            return rs.getString(1);
        }finally{
            close(rs);
            close(s);
        }
    }

    public void doRemoveMessage(TransactionContext c,long seq) throws SQLException,IOException{
        PreparedStatement s=c.getRemovedMessageStatement();
        try{
            if(s==null){
                s=c.getConnection().prepareStatement(statements.getRemoveMessageStatment());
                if(batchStatments){
                    c.setRemovedMessageStatement(s);
                }
            }
            s.setLong(1,seq);
            if(batchStatments){
                s.addBatch();
            }else if(s.executeUpdate()!=1){
                throw new SQLException("Failed to remove message");
            }
        }finally{
            if(!batchStatments){
                s.close();
            }
        }
    }

    public void doRecover(TransactionContext c,ActiveMQDestination destination,JDBCMessageRecoveryListener listener)
            throws Exception{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getFindAllMessagesStatement());
            s.setString(1,destination.getQualifiedName());
            rs=s.executeQuery();
            if(statements.isUseExternalMessageReferences()){
                while(rs.next()){
                    listener.recoverMessageReference(rs.getString(2));
                }
            }else{
                while(rs.next()){
                    listener.recoverMessage(rs.getLong(1),getBinaryData(rs,2));
                }
            }
        }finally{
            close(rs);
            close(s);
            listener.finished();
        }
    }

    public void doSetLastAck(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName,long seq) throws SQLException,IOException{
        PreparedStatement s=c.getAddMessageStatement();
        try{
            if(s==null){
                s=c.getConnection().prepareStatement(statements.getUpdateLastAckOfDurableSubStatement());
                if(batchStatments){
                    c.setUpdateLastAckStatement(s);
                }
            }
            s.setLong(1,seq);
            s.setString(2,destination.getQualifiedName());
            s.setString(3,clientId);
            s.setString(4,subscriptionName);
            if(batchStatments){
                s.addBatch();
            }else if(s.executeUpdate()!=1){
                throw new SQLException("Failed add a message");
            }
        }finally{
            if(!batchStatments){
                s.close();
            }
        }
    }

    public void doRecoverSubscription(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName,JDBCMessageRecoveryListener listener) throws Exception{
        // dumpTables(c, destination.getQualifiedName(),clientId,subscriptionName);
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getFindAllDurableSubMessagesStatement());
            s.setString(1,destination.getQualifiedName());
            s.setString(2,clientId);
            s.setString(3,subscriptionName);
            rs=s.executeQuery();
            if(statements.isUseExternalMessageReferences()){
                while(rs.next()){
                    listener.recoverMessageReference(rs.getString(2));
                }
            }else{
                while(rs.next()){
                    listener.recoverMessage(rs.getLong(1),getBinaryData(rs,2));
                }
            }
        }finally{
            close(rs);
            close(s);
            listener.finished();
        }
    }

    public void doRecoverNextMessages(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName,long seq,int maxReturned,JDBCMessageRecoveryListener listener) throws Exception{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getFindDurableSubMessagesStatement());
            s.setString(1,destination.getQualifiedName());
            s.setString(2,clientId);
            s.setString(3,subscriptionName);
            s.setLong(4,seq);
            rs=s.executeQuery();
            int count=0;
            if(statements.isUseExternalMessageReferences()){
                while(rs.next()&&count<maxReturned){
                    listener.recoverMessageReference(rs.getString(1));
                    count++;
                }
            }else{
                while(rs.next()&&count<maxReturned){
                    listener.recoverMessage(rs.getLong(1),getBinaryData(rs,2));
                    count++;
                }
            }
        }finally{
            close(rs);
            close(s);
            listener.finished();
        }
    }

    public int doGetDurableSubscriberMessageCount(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName) throws SQLException,IOException{
        PreparedStatement s=null;
        ResultSet rs=null;
        int result=0;
        try{
            s=c.getConnection().prepareStatement(statements.getDurableSubscriberMessageCountStatement());
            s.setString(1,destination.getQualifiedName());
            s.setString(2,clientId);
            s.setString(3,subscriptionName);
            rs=s.executeQuery();
            if(rs.next()){
                result=rs.getInt(1);
            }
        }finally{
            close(rs);
            close(s);
        }
        return result;
    }

    /**
     * @see org.apache.activemq.store.jdbc.JDBCAdapter#doSetSubscriberEntry(java.sql.Connection, java.lang.Object,
     *      org.apache.activemq.service.SubscriptionInfo)
     */
    public void doSetSubscriberEntry(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName,String selector,boolean retroactive) throws SQLException,IOException{
        // dumpTables(c, destination.getQualifiedName(), clientId, subscriptionName);
        PreparedStatement s=null;
        try{
            long lastMessageId=-1;
            if(!retroactive){
                s=c.getConnection().prepareStatement(statements.getFindLastSequenceIdInMsgsStatement());
                ResultSet rs=null;
                try{
                    rs=s.executeQuery();
                    if(rs.next()){
                        lastMessageId=rs.getLong(1);
                    }
                }finally{
                    close(rs);
                    close(s);
                }
            }
            s=c.getConnection().prepareStatement(statements.getCreateDurableSubStatement());
            s.setString(1,destination.getQualifiedName());
            s.setString(2,clientId);
            s.setString(3,subscriptionName);
            s.setString(4,selector);
            s.setLong(5,lastMessageId);
            if(s.executeUpdate()!=1){
                throw new IOException("Could not create durable subscription for: "+clientId);
            }
        }finally{
            close(s);
        }
    }

    public SubscriptionInfo doGetSubscriberEntry(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName) throws SQLException,IOException{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getFindDurableSubStatement());
            s.setString(1,destination.getQualifiedName());
            s.setString(2,clientId);
            s.setString(3,subscriptionName);
            rs=s.executeQuery();
            if(!rs.next()){
                return null;
            }
            SubscriptionInfo subscription=new SubscriptionInfo();
            subscription.setDestination(destination);
            subscription.setClientId(clientId);
            subscription.setSubcriptionName(subscriptionName);
            subscription.setSelector(rs.getString(1));
            return subscription;
        }finally{
            close(rs);
            close(s);
        }
    }

    public SubscriptionInfo[] doGetAllSubscriptions(TransactionContext c,ActiveMQDestination destination)
            throws SQLException,IOException{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getFindAllDurableSubsStatement());
            s.setString(1,destination.getQualifiedName());
            rs=s.executeQuery();
            ArrayList rc=new ArrayList();
            while(rs.next()){
                SubscriptionInfo subscription=new SubscriptionInfo();
                subscription.setDestination(destination);
                subscription.setSelector(rs.getString(1));
                subscription.setSubcriptionName(rs.getString(2));
                subscription.setClientId(rs.getString(3));
                rc.add(subscription);
            }
            return (SubscriptionInfo[])rc.toArray(new SubscriptionInfo[rc.size()]);
        }finally{
            close(rs);
            close(s);
        }
    }

    public void doRemoveAllMessages(TransactionContext c,ActiveMQDestination destinationName) throws SQLException,
            IOException{
        PreparedStatement s=null;
        try{
            s=c.getConnection().prepareStatement(statements.getRemoveAllMessagesStatement());
            s.setString(1,destinationName.getQualifiedName());
            s.executeUpdate();
            s.close();
            s=c.getConnection().prepareStatement(statements.getRemoveAllSubscriptionsStatement());
            s.setString(1,destinationName.getQualifiedName());
            s.executeUpdate();
        }finally{
            close(s);
        }
    }

    public void doDeleteSubscription(TransactionContext c,ActiveMQDestination destination,String clientId,
            String subscriptionName) throws SQLException,IOException{
        PreparedStatement s=null;
        try{
            s=c.getConnection().prepareStatement(statements.getDeleteSubscriptionStatement());
            s.setString(1,destination.getQualifiedName());
            s.setString(2,clientId);
            s.setString(3,subscriptionName);
            s.executeUpdate();
        }finally{
            close(s);
        }
    }

    public void doDeleteOldMessages(TransactionContext c) throws SQLException,IOException{
        PreparedStatement s=null;
        try{
            log.debug("Executing SQL: "+statements.getDeleteOldMessagesStatement());
            s=c.getConnection().prepareStatement(statements.getDeleteOldMessagesStatement());
            s.setLong(1,System.currentTimeMillis());
            int i=s.executeUpdate();
            log.debug("Deleted "+i+" old message(s).");
        }finally{
            close(s);
        }
    }

    static private void close(PreparedStatement s){
        try{
            s.close();
        }catch(Throwable e){
        }
    }

    static private void close(ResultSet rs){
        try{
            rs.close();
        }catch(Throwable e){
        }
    }

    public Set doGetDestinations(TransactionContext c) throws SQLException,IOException{
        HashSet rc=new HashSet();
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getFindAllDestinationsStatement());
            rs=s.executeQuery();
            while(rs.next()){
                rc.add(ActiveMQDestination.createDestination(rs.getString(1),ActiveMQDestination.QUEUE_TYPE));
            }
        }finally{
            close(rs);
            close(s);
        }
        return rc;
    }

    public boolean isBatchStatments(){
        return batchStatments;
    }

    public void setBatchStatments(boolean batchStatments){
        this.batchStatments=batchStatments;
    }

    public void setUseExternalMessageReferences(boolean useExternalMessageReferences){
        statements.setUseExternalMessageReferences(useExternalMessageReferences);
    }

    public Statements getStatements(){
        return statements;
    }

    public void setStatements(Statements statements){
        this.statements=statements;
    }

    public byte[] doGetNextDurableSubscriberMessageStatement(TransactionContext c,ActiveMQDestination destination,
            String clientId,String subscriberName) throws SQLException,IOException{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getNextDurableSubscriberMessageStatement());
            s.setString(1,destination.getQualifiedName());
            s.setString(2,clientId);
            s.setString(3,subscriberName);
            rs=s.executeQuery();
            if(!rs.next()){
                return null;
            }
            return getBinaryData(rs,1);
        }finally{
            close(rs);
            close(s);
        }
    }

    /**
     * @param c
     * @param destination
     * @param clientId
     * @param subscriberName
     * @param id
     * @return the previous Id
     * @throws Exception 
     * @see org.apache.activemq.store.jdbc.JDBCAdapter#doGetPrevDurableSubscriberMessageStatement(org.apache.activemq.store.jdbc.TransactionContext,
     *      org.apache.activemq.command.ActiveMQDestination, java.lang.String, java.lang.String, java.lang.String)
     */
    public void doGetPrevDurableSubscriberMessageIdStatement(TransactionContext c,ActiveMQDestination destination,
            String clientId,String subscriberName,long id,JDBCMessageRecoveryListener listener) throws Exception{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getPrevDurableSubscriberMessageIdStatement());
            s.setString(1,destination.getQualifiedName());
            s.setLong(2,id);
            rs=s.executeQuery();
            if (rs.next()) {
            listener.recoverMessage(rs.getLong(1),getBinaryData(rs,2));
            }
            listener.finished();
           
        }finally{
            close(rs);
            close(s);
        }
    }

    /**
     * @param c
     * @param destination
     * @param clientId
     * @param subscriberName
     * @param id
     * @return the next id
     * @throws SQLException
     * @throws IOException
     * @see org.apache.activemq.store.jdbc.JDBCAdapter#doGetNextDurableSubscriberMessageIdStatement(org.apache.activemq.store.jdbc.TransactionContext,
     *      org.apache.activemq.command.ActiveMQDestination, java.lang.String, java.lang.String, java.lang.String)
     */
    public void doGetNextDurableSubscriberMessageIdStatement(TransactionContext c,ActiveMQDestination destination,
            String clientId,String subscriberName,long id,JDBCMessageRecoveryListener listener) throws Exception{
        PreparedStatement s=null;
        ResultSet rs=null;
        try{
            s=c.getConnection().prepareStatement(statements.getNextDurableSubscriberMessageIdStatement());
            s.setString(1,destination.getQualifiedName());
            s.setLong(2,id);
            rs=s.executeQuery();
            if (rs.next()) {
            listener.recoverMessage(rs.getLong(1),getBinaryData(rs,2));
            }
            listener.finished();
           
        }finally{
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

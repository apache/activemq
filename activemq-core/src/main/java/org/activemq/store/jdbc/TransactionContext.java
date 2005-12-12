/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
 *
 **/
package org.activemq.store.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.activemq.util.IOExceptionSupport;

/**
 * Helps keep track of the current transaction/JDBC connection.
 * 
 * @version $Revision: 1.2 $
 */
public class TransactionContext {

    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(TransactionContext.class);
    
    private final DataSource dataSource;
    private Connection connection;
    private boolean inTx;
    private PreparedStatement addMessageStatement;
    private PreparedStatement removedMessageStatement;
    private PreparedStatement updateLastAckStatement;

    public TransactionContext(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Connection getConnection() throws IOException {
        if( connection == null ) {
            try {
                connection = dataSource.getConnection();
                connection.setAutoCommit(!inTx);
            } catch (SQLException e) {
                throw IOExceptionSupport.create(e);
            }
            
            try {
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            } catch (Throwable e) {
            }
        }
        return connection;
    }

    public void executeBatch() throws SQLException {
        try {
            executeBatch(addMessageStatement, "Failed add a message");
        } finally {
            addMessageStatement = null;
            try {
                executeBatch(removedMessageStatement, "Failed to remove a message");
            } finally {
                removedMessageStatement=null;
                try {
                    executeBatch(updateLastAckStatement, "Failed to ack a message");
                } finally {
                    updateLastAckStatement=null;
                }
            }
        }
    }

    private void executeBatch(PreparedStatement p, String message) throws SQLException {
        if( p == null )
            return;
        
        try {
            int[] rc = p.executeBatch();
            for (int i = 0; i < rc.length; i++) {
                if ( rc[i]!= 1 ) {
                    throw new SQLException(message);
                }
            }
        } finally {
            try { p.close(); } catch (Throwable e) { }
        }
    }
    
    public void close() throws IOException {
        if( !inTx ) {
            try {
                try{
                    executeBatch();
                } finally {
                    if (connection != null) {
                        connection.commit();
                    }
                }
            } catch (SQLException e) {
                throw IOExceptionSupport.create(e);
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (Throwable e) {
                    log.warn("Close failed: "+e.getMessage(), e);
                } finally {
                    connection=null;
                }
            }
        }
    }

    public void begin() throws IOException {
        if( inTx )
            throw new IOException("Already started.");
        inTx = true;
        connection = getConnection();
    }

    public void commit() throws IOException {
        if( !inTx )
            throw new IOException("Not started.");
        try {
            executeBatch();
            connection.commit();
        } catch (SQLException e) {
            log.info("commit failed: "+e.getMessage(), e);
            while( e.getNextException() !=null ) {
                e = e.getNextException();
                log.info("Nested exception: "+e);
            }
            throw IOExceptionSupport.create(e);
        } finally {
            inTx=false;
            close();
        }
    }
    
    public void rollback() throws IOException {
        if( !inTx )
            throw new IOException("Not started.");
        try {
            if( addMessageStatement != null ) {
                addMessageStatement.close();
                addMessageStatement=null;
            }
            if( removedMessageStatement != null ) {
                removedMessageStatement.close();
                removedMessageStatement=null;
            }
            if( updateLastAckStatement != null ) {
                updateLastAckStatement.close();
                updateLastAckStatement=null;
            }
            connection.rollback();
            
        } catch (SQLException e) {
            throw IOExceptionSupport.create(e);
        } finally {
            inTx=false;
            close();
        }
    }

    public PreparedStatement getAddMessageStatement() {
        return addMessageStatement;
    }
    public void setAddMessageStatement(PreparedStatement addMessageStatement) {
        this.addMessageStatement = addMessageStatement;
    }

    public PreparedStatement getUpdateLastAckStatement() {
        return updateLastAckStatement;
    }
    public void setUpdateLastAckStatement(PreparedStatement ackMessageStatement) {
        this.updateLastAckStatement = ackMessageStatement;
    }

    public PreparedStatement getRemovedMessageStatement() {
        return removedMessageStatement;
    }
    public void setRemovedMessageStatement(PreparedStatement removedMessageStatement) {
        this.removedMessageStatement = removedMessageStatement;
    }

}

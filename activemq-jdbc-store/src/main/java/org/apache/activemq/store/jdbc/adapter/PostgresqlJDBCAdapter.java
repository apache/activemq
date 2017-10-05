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
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.activemq.store.jdbc.Statements;
import org.apache.activemq.store.jdbc.TransactionContext;

/**
 * Implements all the default JDBC operations that are used
 * by the JDBCPersistenceAdapter.
 * <p/>
 * Subclassing is encouraged to override the default
 * implementation of methods to account for differences
 * in JDBC Driver implementations.
 * <p/>
 * The JDBCAdapter inserts and extracts BLOB data using the
 * getBytes()/setBytes() operations.
 * <p/>
 * The databases/JDBC drivers that use this adapter are:
 * <ul>
 * <li></li>
 * </ul>
 *
 * @org.apache.xbean.XBean element="postgresql-jdbc-adapter"
 * 
 */
public class PostgresqlJDBCAdapter extends BytesJDBCAdapter {
    public String acksPkName = "activemq_acks_pkey";

    @Override
    public void setStatements(Statements statements) {
        statements.setBinaryDataType("BYTEA");
        statements.setDropAckPKAlterStatementEnd("DROP CONSTRAINT \"" + getAcksPkName() + "\"");
        super.setStatements(statements);
    }

    private String getAcksPkName() {
        return acksPkName;
    }

    public void setAcksPkName(String acksPkName) {
        this.acksPkName = acksPkName;
    }

    @Override
    public void doCreateTables(TransactionContext transactionContext) throws SQLException, IOException {
        // Check to see if the table already exists. If it does, then don't log warnings during startup.
        // Need to run the scripts anyways since they may contain ALTER statements that upgrade a previous version of the table
        boolean messageTableAlreadyExists = this.messageTableAlreadyExists(transactionContext);

        for (String createStatement : this.statements.getCreateSchemaStatements()) {
            // This will fail usually since the tables will be
            // created already.
            super.executeStatement(transactionContext, createStatement, messageTableAlreadyExists);
        }
    }

    protected boolean messageTableAlreadyExists(TransactionContext transactionContext) {
        boolean alreadyExists = false;
        ResultSet rs = null;
        try {
            rs = transactionContext.getConnection().getMetaData().getTables(null, null, this.statements.getFullMessageTableName().toLowerCase(), new String[] { "TABLE" });
            alreadyExists = rs.next();
        } catch (Throwable ignore) {
        } finally {
            close(rs);
        }
        return alreadyExists;
    }
}

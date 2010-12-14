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

import org.apache.activemq.store.jdbc.Statements;

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
 * @version $Revision: 1.1 $
 */
public class PostgresqlJDBCAdapter extends BytesJDBCAdapter {
    public String acksPkName = "activemq_acks_pkey";

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
}

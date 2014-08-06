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
 * A JDBC Adapter for Transact-SQL based databases such as SQL Server or Sybase
 *
 * @org.apache.xbean.XBean element="transact-jdbc-adapter"
 * 
 * 
 */
public class TransactJDBCAdapter extends ImageBasedJDBCAdaptor {
    @Override
    public void setStatements(Statements statements) {
        String lockCreateStatement = "SELECT * FROM " + statements.getFullLockTableName() + " WITH (UPDLOCK, ROWLOCK)";

        if (statements.isUseLockCreateWhereClause()) {
            lockCreateStatement += " WHERE ID = 1";
        }

        statements.setLockCreateStatement(lockCreateStatement);

        super.setStatements(statements);
    }
}

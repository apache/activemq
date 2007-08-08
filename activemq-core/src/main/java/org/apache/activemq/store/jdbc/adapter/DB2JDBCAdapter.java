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

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @version $Revision: 1.2 $
 * @org.apache.xbean.XBean element="db2JDBCAdapter"
 */
public class DB2JDBCAdapter extends DefaultJDBCAdapter {
    public DB2JDBCAdapter() {
        batchStatments = false;
    }

    public void setStatements(Statements statements) {
        String lockCreateStatement = "LOCK TABLE " + statements.getFullLockTableName() + " IN EXCLUSIVE MODE";
        statements.setLockCreateStatement(lockCreateStatement);

        super.setStatements(statements);
    }

    protected byte[] getBinaryData(ResultSet rs, int index) throws SQLException {
        // Get as a BLOB
        Blob aBlob = rs.getBlob(index);
        return aBlob.getBytes(1, (int) aBlob.length());
    }
}

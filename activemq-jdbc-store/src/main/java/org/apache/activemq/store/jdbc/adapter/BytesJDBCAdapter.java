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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This JDBCAdapter inserts and extracts BLOB data using the
 * setBytes()/getBytes() operations. The databases/JDBC drivers that use this
 * adapter are:
 * 
 * @org.apache.xbean.XBean element="bytesJDBCAdapter"
 * 
 */
public class BytesJDBCAdapter extends DefaultJDBCAdapter {

    /**
     * @see org.apache.activemq.store.jdbc.adapter.DefaultJDBCAdapter#getBinaryData(java.sql.ResultSet,
     *      int)
     */
    @Override
    protected byte[] getBinaryData(ResultSet rs, int index) throws SQLException {
        return rs.getBytes(index);
    }

    /**
     * @see org.apache.activemq.store.jdbc.adapter.DefaultJDBCAdapter#setBinaryData(java.sql.PreparedStatement,
     *      int, byte[])
     */
    @Override
    protected void setBinaryData(PreparedStatement s, int index, byte[] data) throws SQLException {
        s.setBytes(index, data);
    }

}

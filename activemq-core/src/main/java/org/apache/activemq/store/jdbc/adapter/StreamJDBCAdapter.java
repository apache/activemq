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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.activemq.util.ByteArrayInputStream;

/**
 * This JDBCAdapter inserts and extracts BLOB data using the
 * setBinaryStream()/getBinaryStream() operations.
 * 
 * The databases/JDBC drivers that use this adapter are:
 * <ul>
 * <li>Axion</li>
 * </ul>
 * 
 * @org.apache.xbean.XBean element="streamJDBCAdapter"
 * 
 * @version $Revision: 1.2 $
 */
public class StreamJDBCAdapter extends DefaultJDBCAdapter {

    /**
     * @see org.apache.activemq.store.jdbc.adapter.DefaultJDBCAdapter#getBinaryData(java.sql.ResultSet,
     *      int)
     */
    protected byte[] getBinaryData(ResultSet rs, int index) throws SQLException {

        try {
            InputStream is = rs.getBinaryStream(index);
            ByteArrayOutputStream os = new ByteArrayOutputStream(1024 * 4);

            int ch;
            while ((ch = is.read()) >= 0) {
                os.write(ch);
            }
            is.close();
            os.close();

            return os.toByteArray();
        } catch (IOException e) {
            throw (SQLException)new SQLException("Error reading binary parameter: " + index).initCause(e);
        }
    }

    /**
     * @see org.apache.activemq.store.jdbc.adapter.DefaultJDBCAdapter#setBinaryData(java.sql.PreparedStatement,
     *      int, byte[])
     */
    protected void setBinaryData(PreparedStatement s, int index, byte[] data) throws SQLException {
        s.setBinaryStream(index, new ByteArrayInputStream(data), data.length);
    }

}

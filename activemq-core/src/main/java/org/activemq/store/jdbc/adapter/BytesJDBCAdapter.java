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
package org.activemq.store.jdbc.adapter;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.activemq.store.jdbc.StatementProvider;

/**
 * This JDBCAdapter inserts and extracts BLOB data using the 
 * setBytes()/getBytes() operations.
 * 
 * The databases/JDBC drivers that use this adapter are:
 * <ul>
 * <li></li> 
 * </ul>
 * 
 * @version $Revision: 1.2 $
 */
public class BytesJDBCAdapter extends DefaultJDBCAdapter {

	
    public BytesJDBCAdapter() {
        super();
    }

	public BytesJDBCAdapter(StatementProvider provider) {
        super(provider);
    }
    
    /**
     * @see org.activemq.store.jdbc.adapter.DefaultJDBCAdapter#getBinaryData(java.sql.ResultSet, int)
     */
    protected byte[] getBinaryData(ResultSet rs, int index) throws SQLException {
        return rs.getBytes(index);
    }
    
    /**
     * @see org.activemq.store.jdbc.adapter.DefaultJDBCAdapter#setBinaryData(java.sql.PreparedStatement, int, byte[])
     */
    protected void setBinaryData(PreparedStatement s, int index, byte[] data) throws SQLException {
        s.setBytes(index, data);
    }
    
}
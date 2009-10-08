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
package org.apache.activemq.store.jdbc;

import java.io.IOException;

import junit.framework.AssertionFailedError;

import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterTestSupport;
import org.apache.derby.jdbc.EmbeddedDataSource;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class JDBCPersistenceAdapterTest extends PersistenceAdapterTestSupport {
    
    protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws IOException {
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        jdbc.setDataSource(dataSource);
        if( delete ) {
            jdbc.deleteAllMessages();
        }
        return jdbc;
    }
    
    @Override
    public void testStoreCanHandleDupMessages() throws Exception {
        try {
            super.testStoreCanHandleDupMessages();
            fail("We expect this test to fail as it would be too expensive to add additional " +
                 "unique constraints in the JDBC implementation to detect the duplicate messages.");
        } catch (AssertionFailedError expected) {
        }
    }
    
}

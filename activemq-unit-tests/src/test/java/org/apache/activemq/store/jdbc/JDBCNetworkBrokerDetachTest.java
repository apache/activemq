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

import java.io.File;
import java.sql.SQLException;
import java.util.LinkedList;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkBrokerDetachTest;
import org.apache.activemq.util.IOHelper;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.After;
import org.junit.BeforeClass;

public class JDBCNetworkBrokerDetachTest extends NetworkBrokerDetachTest {

    LinkedList<EmbeddedDataSource> dataSources = new LinkedList<>();
    protected void configureBroker(BrokerService broker) throws Exception {
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        try {
            EmbeddedDataSource dataSource = (EmbeddedDataSource) DataSourceServiceSupport.createDataSource(jdbc.getDataDirectoryFile().getCanonicalPath(), broker.getBrokerName());
            dataSource.getConnection().close(); // ensure derby for brokerName is initialized
            jdbc.setDataSource(dataSource);
            dataSources.add(dataSource);
        } catch (SQLException e) {
            e.printStackTrace();
            Exception n = e.getNextException();
            while (n != null) {
                n.printStackTrace();
                if (n instanceof SQLException) {
                    n = ((SQLException) n).getNextException();
                }
            }
            throw e;
        }
        broker.setPersistenceAdapter(jdbc);
        broker.setUseVirtualTopics(false);
    }

    @After
    public void shutdownDataSources() throws Exception {
        for (EmbeddedDataSource ds: dataSources) {
            DataSourceServiceSupport.shutdownDefaultDataSource(ds);
        }
        dataSources.clear();
    }

    @BeforeClass
    public static void ensureDerbyHasCleanDirectory() throws Exception {
        IOHelper.delete(new File(IOHelper.getDefaultDataDirectory()));
    }
}

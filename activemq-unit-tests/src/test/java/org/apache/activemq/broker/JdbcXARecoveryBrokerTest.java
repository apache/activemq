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
package org.apache.activemq.broker;

import java.io.File;
import junit.framework.Test;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.jdbc.DataSourceServiceSupport;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.util.IOHelper;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.derby.jdbc.EmbeddedXADataSource;

public class JdbcXARecoveryBrokerTest extends XARecoveryBrokerTest {

    EmbeddedXADataSource dataSource;

    @Override
    protected void setUp() throws Exception {
        System.setProperty("derby.system.home", new File(IOHelper.getDefaultDataDirectory()).getCanonicalPath());
        dataSource = new EmbeddedXADataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        stopDerby();
    }

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        super.configureBroker(broker);

        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        jdbc.setDataSource(dataSource);
        broker.setPersistenceAdapter(jdbc);
    }

    @Override
    protected void restartBroker() throws Exception {
        broker.stop();
        stopDerby();
        dataSource = new EmbeddedXADataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");

        broker = createRestartedBroker();
        broker.start();
    }

    private void stopDerby() {
        LOG.info("STOPPING DB!@!!!!");
        DataSourceServiceSupport.shutdownDefaultDataSource(dataSource);
    }

    public static Test suite() {
        return suite(JdbcXARecoveryBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected ActiveMQDestination createDestination() {
        return new ActiveMQQueue("test,special");
    }

}

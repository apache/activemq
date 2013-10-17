/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.web;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.util.Wait;
import org.apache.activemq.web.config.SystemPropertiesConfiguration;
import org.junit.Before;
import org.junit.Test;


import static org.apache.activemq.util.Wait.*;
import static org.junit.Assert.assertEquals;

public class RemoteJMXBrokerFailoverTest {

    private BrokerService master;
    private BrokerService slave;
    private LinkedList<JMXConnectorServer> serverList = new LinkedList<JMXConnectorServer>();

    @Before
    public void startUp() throws Exception {


        master = BrokerFactory.createBroker("broker:()/master?useJmx=true");
        configureMBeanServer(master, 1050);

        slave = BrokerFactory.createBroker("broker:()/slave?useJmx=true");
        configureMBeanServer(slave, 1060);
        master.start();
        master.waitUntilStarted();

        final BrokerService slaveToStart = slave;
        Executors.newCachedThreadPool().execute(new Runnable(){
            @Override
            public void run() {
                try {
                    slaveToStart.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }

    private void configureMBeanServer(BrokerService brokerService, int port) throws IOException {
        // shared fs master/slave
        brokerService.getPersistenceAdapter().setDirectory(
                new File(brokerService.getDataDirectoryFile(), "shared"));

        ManagementContext managementContext = brokerService.getManagementContext();

        // have mbean servers remain alive - like in karaf container
        MBeanServer mbeanServer = MBeanServerFactory.createMBeanServer(managementContext.getJmxDomainName());

        Registry registry = LocateRegistry.createRegistry(port + 1);

        JMXConnectorServer connectorServer =
                        JMXConnectorServerFactory.newJMXConnectorServer(
                                new JMXServiceURL(
                                    "service:jmx:rmi://localhost:"  + port + "/jndi/rmi://localhost:" + (port + 1) + "/jmxrmi"),
                                null, mbeanServer);

        connectorServer.start();
        serverList.addFirst(connectorServer);

        managementContext.setMBeanServer(mbeanServer);
        managementContext.setCreateConnector(false);
    }

    @Test
    public void testConnectToMasterFailover() throws Exception {
        String jmxUri = "";
        for (JMXConnectorServer jmxConnectorServer : serverList) {
            if (!jmxUri.isEmpty()) {
                jmxUri += ',';
            }
            jmxUri += jmxConnectorServer.getAddress().toString();
        }
        System.out.println("jmx url: " + jmxUri);
        System.setProperty("webconsole.jmx.url", jmxUri);
        RemoteJMXBrokerFacade brokerFacade = new RemoteJMXBrokerFacade();

        SystemPropertiesConfiguration configuration = new SystemPropertiesConfiguration();
        brokerFacade.setConfiguration(configuration);

        assertEquals("connected to master", master.getBrokerName(), brokerFacade.getBrokerName());

        stopAndRestartMaster();

        assertEquals("connected to slave", slave.getBrokerName(), brokerFacade.getBrokerName());

    }

    private void stopAndRestartMaster() throws Exception {
        master.stop();
        master.waitUntilStopped();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return !slave.isSlave();
            }
        });

        master.start();
        master.waitUntilStarted();
    }
}

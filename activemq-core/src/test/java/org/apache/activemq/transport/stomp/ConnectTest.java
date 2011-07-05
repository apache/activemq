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
package org.apache.activemq.transport.stomp;

import java.util.Vector;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static junit.framework.Assert.assertTrue;

// https://issues.apache.org/jira/browse/AMQ-3393
public class ConnectTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectTest.class);
    BrokerService brokerService;
    Vector<Throwable> exceptions = new Vector<Throwable>();

    @Before
    public void startBroker() throws Exception {
        exceptions.clear();
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.addConnector("stomp://0.0.0.0:61612");
        brokerService.start();
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void testStompConnectLeak() throws Exception {

        Thread t1 = new Thread() {
            StompConnection connection = new StompConnection();

            public void run() {
                try {
                    connection.open("localhost", 61612);
                    connection.connect("system", "manager");
                    connection.disconnect();
                } catch (Exception ex) {
                    LOG.error("unexpected exception on connect/disconnect", ex);
                    exceptions.add(ex);
                }
            }
        };

        int i = 0;
        long done = System.currentTimeMillis() + (60 * 1000 * 2);
        while (System.currentTimeMillis() < done) {
            t1.run();
            if (++i % 5000 == 0) {
                LOG.info("connection count on stomp connector:" + brokerService.getTransportConnectors().get(0).connectionCount());
            }
        }

        assertTrue("no dangling connections", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == brokerService.getTransportConnectors().get(0).connectionCount();
            }
        }));
        assertTrue("no exceptions", exceptions.isEmpty());
    }
}
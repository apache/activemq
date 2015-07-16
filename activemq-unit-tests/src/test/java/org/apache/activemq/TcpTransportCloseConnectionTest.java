/*
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

package org.apache.activemq;

import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.Wait;
import org.apache.log4j.Appender;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.*;

import static org.junit.Assert.assertFalse;

import java.util.concurrent.*;

import org.apache.activemq.broker.BrokerService;

import javax.management.JMException;
import javax.management.ObjectName;

public class TcpTransportCloseConnectionTest {

    static boolean transportConnectionFailed = false;

    private BrokerService broker;

    private final String uri = "tcp://localhost:0?wireFormat.maxInactivityDuration=500";

    static final Appender appender = new DefaultTestAppender() {
        @Override
        public void doAppend(LoggingEvent event) {
            if(event.getMessage().toString().contains("Transport Connection")
                    && event.getMessage().toString().contains("failed")
                    && (event.getMessage().toString().contains("java.net.SocketException") || event.getMessage().toString().contains("java.io.EOFException"))) {
                transportConnectionFailed = true;
            }
        }
    };

    class CustomManagementContext extends ManagementContext {
        @Override
        public void unregisterMBean(ObjectName name) throws JMException {
            try {
                //Sleep for a second to emulate the MBean server being hung up of an unregister MBean call.
                TimeUnit.SECONDS.sleep(1);
            }
            catch(Throwable t) {
            }
            super.unregisterMBean(name);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        org.apache.log4j.Logger.getRootLogger().addAppender(appender);
    }

    @AfterClass
    public static void setDown() throws Exception {
        org.apache.log4j.Logger.getRootLogger().removeAppender(appender);
    }

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.addConnector(uri);
        ManagementContext customManagementContext = new CustomManagementContext();
        broker.setManagementContext(customManagementContext);
        broker.start();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void tesCloseConnectionTest() throws Exception {
        final ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(broker.getDefaultSocketURIString());
        activeMQConnectionFactory.setWatchTopicAdvisories(false);

        ActiveMQConnection connection = (ActiveMQConnection) activeMQConnectionFactory.createConnection();
        connection.start();
        connection.close();

        assertFalse("The Transport has not failed", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return transportConnectionFailed;
            }
        }, 2 * 1000));

    }
}

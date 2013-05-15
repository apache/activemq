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
package org.apache.activemq.bugs;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for simple App.
 */
public class AMQ4531Test extends TestCase {

    private final Logger LOG = LoggerFactory.getLogger(AMQ4531Test.class);

    private String connectionURI;
    private MBeanServer mbeanServer;
    private BrokerService broker;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        broker = new BrokerService();
        connectionURI = broker.addConnector("tcp://0.0.0.0:0?maximumConnections=1").getPublishableConnectString();
        broker.setPersistent(false);
        broker.start();
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    @Override
    protected void tearDown() throws Exception {
        broker.stop();
        super.tearDown();
    }

    /**
     * Create the test case
     *
     * @param testName
     *            name of the test case
     */
    public AMQ4531Test(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AMQ4531Test.class);
    }

    public void testFDSLeak() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionURI);
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.start();

        int connections = 100;
        final long original = openFileDescriptorCount();
        LOG.info("FD count: " + original);
        final CountDownLatch done = new CountDownLatch(connections);
        for (int i = 0; i < connections; i++) {
            new Thread("worker: " + i) {
                @Override
                public void run() {
                    ActiveMQConnection connection = null;
                    try {
                        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionURI);
                        connection = (ActiveMQConnection) factory.createConnection();
                        connection.start();
                    } catch (Exception e) {
                        LOG.debug(getStack(e));
                    } finally {
                        try {
                            connection.close();
                        } catch (Exception e) {
                            LOG.debug(getStack(e));
                        }
                        done.countDown();
                        LOG.debug("Latch count down called.");
                    }
                }
            }.start();
        }

        // Wait for all the clients to finish
        LOG.info("Waiting for latch...");
        done.await();
        LOG.info("Latch complete.");
        LOG.info("FD count: " + openFileDescriptorCount());

        assertTrue("Too many open file descriptors: " + openFileDescriptorCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                long openFDs = openFileDescriptorCount();
                LOG.info("Current FD count [{}], original FD count[{}]", openFDs, original);
                return (openFDs - original) < 10;
            }
        }));
    }

    private long openFileDescriptorCount() throws Exception {
        return ((Long) mbeanServer.getAttribute(new ObjectName("java.lang:type=OperatingSystem"), "OpenFileDescriptorCount")).longValue();
    }

    private String getStack(Throwable aThrowable) {
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        aThrowable.printStackTrace(printWriter);
        return result.toString();
    }
}

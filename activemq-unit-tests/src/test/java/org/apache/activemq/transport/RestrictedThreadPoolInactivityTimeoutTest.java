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
package org.apache.activemq.transport;

import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.util.SocketProxy;
import org.apache.activemq.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import javax.management.JMException;
import javax.management.ObjectName;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

@Category(ParallelTest.class)
public class RestrictedThreadPoolInactivityTimeoutTest extends JmsTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(RestrictedThreadPoolInactivityTimeoutTest.class);

    public String brokerTransportScheme = "tcp";
    public Boolean rejectWork = Boolean.FALSE;

    final int poolSize = 2;
    final int numConnections = 10;
    final CountDownLatch doneOneConnectionAddress = new CountDownLatch(1);
    final CountDownLatch doneConsumers = new CountDownLatch(numConnections);

    protected BrokerService createBroker() throws Exception {

        if (rejectWork) {
            System.setProperty("org.apache.activemq.transport.AbstractInactivityMonitor.workQueueCapacity", Integer.toString(poolSize));
            System.setProperty("org.apache.activemq.transport.AbstractInactivityMonitor.rejectWork", "true");
        }
        System.setProperty("org.apache.activemq.transport.AbstractInactivityMonitor.maximumPoolSize", Integer.toString(poolSize));

        BrokerService broker = super.createBroker();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setManagementContext(new ManagementContext() {
            @Override
            public void unregisterMBean(ObjectName name) throws JMException {
                if (name.getKeyPropertyListString().contains("remoteAddress")) {
                    // a client connection mbean, removed by inactivity monitor task
                    // simulate a slow mbean unregister
                    LOG.info("SLEEP : " + Thread.currentThread() + ": on remoteAddress unregister: " + name);
                    try {
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException ok) {
                    }
                    doneOneConnectionAddress.countDown();
                } else if (name.getKeyPropertyListString().contains("Consumer")) {
                    // consumer removal from asyncStop task, this is blocked on service lock
                    // during inactivity monitor onException
                    LOG.info(Thread.currentThread() + ": on consumer unregister: " + name);
                    doneConsumers.countDown();
                }
                super.unregisterMBean(name);
            }
        });

        broker.addConnector(brokerTransportScheme + "://localhost:0?wireFormat.maxInactivityDuration=1000&wireFormat.maxInactivityDurationInitalDelay=1000");
        return broker;
    }

    public void initCombosForTestThreadsInvolvedInXInactivityTimeouts() {
        addCombinationValues("brokerTransportScheme", new Object[]{"tcp", "nio"});
        addCombinationValues("rejectWork", new Object[] {Boolean.TRUE, Boolean.FALSE});
    }

    public void testThreadsInvolvedInXInactivityTimeouts() throws Exception {

        URI tcpBrokerUri = URISupport.removeQuery(broker.getTransportConnectors().get(0).getConnectUri());

        SocketProxy proxy = new SocketProxy();
        proxy.setTarget(tcpBrokerUri);
        proxy.open();

        // leave the server to do the only inactivity monitoring
        URI clientUri =  URISupport.createURIWithQuery(proxy.getUrl(), "useInactivityMonitor=false");
        LOG.info("using server uri: " + tcpBrokerUri + ", client uri: " + clientUri);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(clientUri);

        for (int i=0; i<numConnections;i++) {
            Connection c = factory.createConnection();
            c.start();
        }

        proxy.pause();

        int before = Thread.currentThread().getThreadGroup().activeCount();
        LOG.info("threads before: " + before);

        // expect inactivity monitor to kick in after 2*timeout

        Thread.yield();

        // after one sleep, unbounded pools will have filled with threads
        doneOneConnectionAddress.await(10, TimeUnit.SECONDS);

        int after = Thread.currentThread().getThreadGroup().activeCount();

        int diff = Math.abs(before - after);
        LOG.info("threads after: " + after + ", diff: " + diff);

        assertTrue("Should be at most inactivity monitor pool size * 2. Diff = " + diff, diff <= 2*poolSize);

        assertTrue("all work complete", doneConsumers.await(30, TimeUnit.SECONDS));
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        System.clearProperty("org.apache.activemq.transport.AbstractInactivityMonitor.workQueueCapacity");
        System.clearProperty("org.apache.activemq.transport.AbstractInactivityMonitor.maximumPoolSize");
        System.clearProperty("org.apache.activemq.transport.AbstractInactivityMonitor.rejectWork");
    }

    public static Test suite() {
        return suite(RestrictedThreadPoolInactivityTimeoutTest.class);
    }
}

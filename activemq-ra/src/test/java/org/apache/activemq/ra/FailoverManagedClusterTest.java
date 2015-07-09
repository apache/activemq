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
package org.apache.activemq.ra;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.resource.ResourceException;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.ExecutionContext;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkListener;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverManagedClusterTest {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverManagedClusterTest.class);

    long txGenerator = System.currentTimeMillis();

    private static final String MASTER_BIND_ADDRESS = "tcp://localhost:0";
    private static final String SLAVE_BIND_ADDRESS = "tcp://localhost:0";
    private static final String KAHADB_DIRECTORY = "target/activemq-data/";

    private String masterConnectionUri;
    private String slaveConnectionUri;

    private String brokerUri;

    private BrokerService master;
    private BrokerService slave;
    private final CountDownLatch slaveThreadStarted = new CountDownLatch(1);

    @Before
    public void setUp() throws Exception {
        createAndStartMaster();
        createAndStartSlave();

        brokerUri = "failover://(" + masterConnectionUri + "," + slaveConnectionUri + ")?randomize=false";
    }

    @After
    public void tearDown() throws Exception {
        if (slave != null) {
            slave.stop();
        }

        if (master != null) {
            master.stop();
        }
    }

    private void createAndStartMaster() throws Exception {
        master = new BrokerService();
        master.setDeleteAllMessagesOnStartup(true);
        master.setUseJmx(false);
        master.setDataDirectory(KAHADB_DIRECTORY);
        master.setBrokerName("BROKER");
        masterConnectionUri = master.addConnector(MASTER_BIND_ADDRESS).getPublishableConnectString();
        master.start();
        master.waitUntilStarted();
    }

    private void createAndStartSlave() throws Exception {
        slave = new BrokerService();
        slave.setUseJmx(false);
        slave.setDataDirectory(KAHADB_DIRECTORY);
        slave.setBrokerName("BROKER");
        slaveConnectionUri = slave.addConnector(SLAVE_BIND_ADDRESS).getPublishableConnectString();

        // Start the slave asynchronously, since this will block
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    slaveThreadStarted.countDown();
                    slave.start();
                    LOG.info("slave has started");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }).start();
    }

    @Test(timeout = 60000)
    public void testFailover() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUri);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQResourceAdapter adapter = new ActiveMQResourceAdapter();
        adapter.setServerUrl(brokerUri);
        adapter.start(new StubBootstrapContext());

        final CountDownLatch messageDelivered = new CountDownLatch(1);

        final StubMessageEndpoint endpoint = new StubMessageEndpoint() {
            @Override
            public void onMessage(Message message) {
                LOG.info("Received message " + message);
                super.onMessage(message);
                messageDelivered.countDown();
            };
        };

        ActiveMQActivationSpec activationSpec = new ActiveMQActivationSpec();
        activationSpec.setDestinationType(Queue.class.getName());
        activationSpec.setDestination("TEST");
        activationSpec.setResourceAdapter(adapter);
        activationSpec.validate();

        MessageEndpointFactory messageEndpointFactory = new MessageEndpointFactory() {
            @Override
            public MessageEndpoint createEndpoint(XAResource resource) throws UnavailableException {
                endpoint.xaresource = resource;
                return endpoint;
            }

            @Override
            public boolean isDeliveryTransacted(Method method) throws NoSuchMethodException {
                return true;
            }
        };

        // Activate an Endpoint
        adapter.endpointActivation(messageEndpointFactory, activationSpec);

        // Give endpoint a moment to setup and register its listeners
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }

        MessageProducer producer = session.createProducer(new ActiveMQQueue("TEST"));
        slaveThreadStarted.await(10, TimeUnit.SECONDS);

        // force a failover before send
        LOG.info("Stopping master to force failover..");
        master.stop();
        master = null;
        assertTrue("slave started ok", slave.waitUntilStarted());

        producer.send(session.createTextMessage("Hello, again!"));

        // Wait for the message to be delivered.
        assertTrue(messageDelivered.await(5000, TimeUnit.MILLISECONDS));
    }

    private static final class StubBootstrapContext implements BootstrapContext {
        @Override
        public WorkManager getWorkManager() {
            return new WorkManager() {
                @Override
                public void doWork(Work work) throws WorkException {
                    new Thread(work).start();
                }

                @Override
                public void doWork(Work work, long arg1, ExecutionContext arg2, WorkListener arg3) throws WorkException {
                    new Thread(work).start();
                }

                @Override
                public long startWork(Work work) throws WorkException {
                    new Thread(work).start();
                    return 0;
                }

                @Override
                public long startWork(Work work, long arg1, ExecutionContext arg2, WorkListener arg3) throws WorkException {
                    new Thread(work).start();
                    return 0;
                }

                @Override
                public void scheduleWork(Work work) throws WorkException {
                    new Thread(work).start();
                }

                @Override
                public void scheduleWork(Work work, long arg1, ExecutionContext arg2, WorkListener arg3) throws WorkException {
                    new Thread(work).start();
                }
            };
        }

        @Override
        public XATerminator getXATerminator() {
            return null;
        }

        @Override
        public Timer createTimer() throws UnavailableException {
            return null;
        }
    }

    public class StubMessageEndpoint implements MessageEndpoint, MessageListener {
        public int messageCount;
        public XAResource xaresource;
        public Xid xid;

        @Override
        public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException {
            try {
                if (xid == null) {
                    xid = createXid();
                }
                xaresource.start(xid, 0);
            } catch (Throwable e) {
                throw new ResourceException(e);
            }
        }

        @Override
        public void afterDelivery() throws ResourceException {
            try {
                xaresource.end(xid, XAResource.TMSUCCESS);
                xaresource.prepare(xid);
                xaresource.commit(xid, false);
            } catch (Throwable e) {
                e.printStackTrace();
                throw new ResourceException(e);
            }
        }

        @Override
        public void release() {
        }

        @Override
        public void onMessage(Message message) {
            messageCount++;
        }
    }

    public Xid createXid() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(++txGenerator);
        os.close();
        final byte[] bs = baos.toByteArray();

        return new Xid() {
            @Override
            public int getFormatId() {
                return 86;
            }

            @Override
            public byte[] getGlobalTransactionId() {
                return bs;
            }

            @Override
            public byte[] getBranchQualifier() {
                return bs;
            }
        };
    }
}

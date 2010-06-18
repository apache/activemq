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

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FailoverManagedClusterTest extends TestCase {
    private static final Log LOG = LogFactory.getLog(FailoverManagedClusterTest.class);
    
    long txGenerator = System.currentTimeMillis();
    
    private static final String MASTER_BIND_ADDRESS = "tcp://0.0.0.0:61616";
    private static final String SLAVE_BIND_ADDRESS = "tcp://0.0.0.0:61617";

    private static final String BROKER_URL = "failover://(" + MASTER_BIND_ADDRESS + "," + SLAVE_BIND_ADDRESS + ")?randomize=false";
    
    private BrokerService master;
    private BrokerService slave;
    private CountDownLatch slaveThreadStarted = new CountDownLatch(1);

    @Override
    protected void setUp() throws Exception {
        createAndStartMaster();
        createAndStartSlave();    
    }
    
    @Override
    protected void tearDown() throws Exception {
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
        master.setBrokerName("BROKER");
        master.addConnector(MASTER_BIND_ADDRESS);
        master.start();
        master.waitUntilStarted();
    }

    private void createAndStartSlave() throws Exception {
        slave = new BrokerService();
        slave.setUseJmx(false);
        slave.setBrokerName("BROKER");
        slave.addConnector(SLAVE_BIND_ADDRESS);

        // Start the slave asynchronously, since this will block
        new Thread(new Runnable() {
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

    public void testFailover() throws Exception {
        
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQResourceAdapter adapter = new ActiveMQResourceAdapter();
        adapter.setServerUrl(BROKER_URL);
        adapter.start(new StubBootstrapContext());

        final CountDownLatch messageDelivered = new CountDownLatch(1);

        final StubMessageEndpoint endpoint = new StubMessageEndpoint() {
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
            public MessageEndpoint createEndpoint(XAResource resource) throws UnavailableException {
                endpoint.xaresource = resource;
                return endpoint;
            }

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
        public WorkManager getWorkManager() {
            return new WorkManager() {
                public void doWork(Work work) throws WorkException {
                    new Thread(work).start();
                }

                public void doWork(Work work, long arg1, ExecutionContext arg2, WorkListener arg3) throws WorkException {
                    new Thread(work).start();
                }

                public long startWork(Work work) throws WorkException {
                    new Thread(work).start();
                    return 0;
                }

                public long startWork(Work work, long arg1, ExecutionContext arg2, WorkListener arg3) throws WorkException {
                    new Thread(work).start();
                    return 0;
                }

                public void scheduleWork(Work work) throws WorkException {
                    new Thread(work).start();
                }

                public void scheduleWork(Work work, long arg1, ExecutionContext arg2, WorkListener arg3) throws WorkException {
                    new Thread(work).start();
                }
            };
        }

        public XATerminator getXATerminator() {
            return null;
        }

        public Timer createTimer() throws UnavailableException {
            return null;
        }
    }

    public class StubMessageEndpoint implements MessageEndpoint, MessageListener {
        public int messageCount;
        public XAResource xaresource;
        public Xid xid;

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

        public void release() {
        }

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
            public int getFormatId() {
                return 86;
            }

            public byte[] getGlobalTransactionId() {
                return bs;
            }

            public byte[] getBranchQualifier() {
                return bs;
            }
        };
    }

}

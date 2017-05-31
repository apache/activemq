/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.ra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.KeyStore;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
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

import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.SslBrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.tcp.SslTransportFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SSLTest {

    private static final String KEYSTORE_TYPE = "jks";
    private static final String PASSWORD = "password";
    private static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    private static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";
    private static final String KAHADB_DIRECTORY = "target/activemq-data/";

    private static final String BIND_ADDRESS = "ssl://0.0.0.0:61616";

    private long txGenerator = System.currentTimeMillis();

    private SslBrokerService broker;
    private TransportConnector connector;

    @Before
    public void setUp() throws Exception {
        createAndStartBroker();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    private void createAndStartBroker() throws Exception {
        broker = new SslBrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setUseJmx(false);
        broker.setBrokerName("BROKER");
        broker.setDataDirectory(KAHADB_DIRECTORY);
        KeyManager[] km = getKeyManager();
        TrustManager[] tm = getTrustManager();
        connector = broker.addSslConnector(BIND_ADDRESS, km, tm, null);
        broker.start();
        broker.waitUntilStarted();     // for client side

        SslTransportFactory sslFactory = new SslTransportFactory();
        SslContext ctx = new SslContext(km, tm, null);
        SslContext.setCurrentSslContext(ctx);
        TransportFactory.registerTransportFactory("ssl", sslFactory);
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
                xaresource.end(xid, 0);
                xaresource.prepare(xid);
                xaresource.commit(xid, false);
            } catch (Throwable e) {
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

    @Test(timeout = 60000)
    public void testMessageDeliveryUsingSSLTruststoreOnly() throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(getKeyManager(), getTrustManager(), null);
        makeSSLConnection(context, null, connector);
        ActiveMQSslConnectionFactory factory = new ActiveMQSslConnectionFactory("ssl://localhost:61616");
        factory.setTrustStore("server.keystore");
        factory.setTrustStorePassword("password");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer advisory = session.createConsumer(AdvisorySupport.getConsumerAdvisoryTopic(new ActiveMQQueue("TEST")));

        ActiveMQResourceAdapter adapter = new ActiveMQResourceAdapter();
        adapter.setServerUrl("ssl://localhost:61616");
        adapter.setTrustStore("server.keystore");
        adapter.setTrustStorePassword("password");
        adapter.setQueuePrefetch(1);
        adapter.start(new StubBootstrapContext());

        final CountDownLatch messageDelivered = new CountDownLatch(1);

        final StubMessageEndpoint endpoint = new StubMessageEndpoint() {
            @Override
            public void onMessage(Message message) {
                super.onMessage(message);
                messageDelivered.countDown();
            }
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

        ActiveMQMessage msg = (ActiveMQMessage) advisory.receive(1000);
        if (msg != null) {
            assertEquals("Prefetch size hasn't been set", 1, ((ConsumerInfo) msg.getDataStructure()).getPrefetchSize());
        } else {
            fail("Consumer hasn't been created");
        }

        // Send the broker a message to that endpoint
        MessageProducer producer = session.createProducer(new ActiveMQQueue("TEST"));
        producer.send(session.createTextMessage("Hello!"));

        connection.close();

        // Wait for the message to be delivered.
        assertTrue(messageDelivered.await(5000, TimeUnit.MILLISECONDS));

        // Shut the Endpoint down.
        adapter.endpointDeactivation(messageEndpointFactory, activationSpec);
        adapter.stop();
    }

    @Test(timeout = 60000)
    public void testMessageDeliveryUsingSSLTruststoreAndKeystore() throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(getKeyManager(), getTrustManager(), null);
        makeSSLConnection(context, null, connector);
        ActiveMQSslConnectionFactory factory = new ActiveMQSslConnectionFactory("ssl://localhost:61616");
        factory.setTrustStore("server.keystore");
        factory.setTrustStorePassword("password");
        factory.setKeyStore("client.keystore");
        factory.setKeyStorePassword("password");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer advisory = session.createConsumer(AdvisorySupport.getConsumerAdvisoryTopic(new ActiveMQQueue("TEST")));

        ActiveMQResourceAdapter adapter = new ActiveMQResourceAdapter();
        adapter.setServerUrl("ssl://localhost:61616");
        adapter.setTrustStore("server.keystore");
        adapter.setTrustStorePassword("password");
        adapter.setKeyStore("client.keystore");
        adapter.setKeyStorePassword("password");
        adapter.setQueuePrefetch(1);
        adapter.start(new StubBootstrapContext());

        final CountDownLatch messageDelivered = new CountDownLatch(1);

        final StubMessageEndpoint endpoint = new StubMessageEndpoint() {
            @Override
            public void onMessage(Message message) {
                super.onMessage(message);
                messageDelivered.countDown();
            }
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

        ActiveMQMessage msg = (ActiveMQMessage) advisory.receive(1000);
        if (msg != null) {
            assertEquals("Prefetch size hasn't been set", 1, ((ConsumerInfo) msg.getDataStructure()).getPrefetchSize());
        } else {
            fail("Consumer hasn't been created");
        }

        // Send the broker a message to that endpoint
        MessageProducer producer = session.createProducer(new ActiveMQQueue("TEST"));
        producer.send(session.createTextMessage("Hello!"));

        connection.close();

        // Wait for the message to be delivered.
        assertTrue(messageDelivered.await(5000, TimeUnit.MILLISECONDS));

        // Shut the Endpoint down.
        adapter.endpointDeactivation(messageEndpointFactory, activationSpec);
        adapter.stop();
    }

    @Test(timeout = 60000)
    public void testMessageDeliveryUsingSSLTruststoreAndKeystoreOverrides() throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(getKeyManager(), getTrustManager(), null);
        makeSSLConnection(context, null, connector);
        ActiveMQSslConnectionFactory factory = new ActiveMQSslConnectionFactory("ssl://localhost:61616");
        factory.setTrustStore("server.keystore");
        factory.setTrustStorePassword("password");
        factory.setKeyStore("client.keystore");
        factory.setKeyStorePassword("password");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer advisory = session.createConsumer(AdvisorySupport.getConsumerAdvisoryTopic(new ActiveMQQueue("TEST")));

        ActiveMQResourceAdapter adapter = new ActiveMQResourceAdapter();
        adapter.setServerUrl("ssl://localhost:61616");
        adapter.setQueuePrefetch(1);
        adapter.start(new StubBootstrapContext());

        final CountDownLatch messageDelivered = new CountDownLatch(1);

        final StubMessageEndpoint endpoint = new StubMessageEndpoint() {
            @Override
            public void onMessage(Message message) {
                super.onMessage(message);
                messageDelivered.countDown();
            }
        };

        ActiveMQActivationSpec activationSpec = new ActiveMQActivationSpec();
        activationSpec.setDestinationType(Queue.class.getName());
        activationSpec.setDestination("TEST");
        activationSpec.setResourceAdapter(adapter);
        activationSpec.setTrustStore("server.keystore");
        activationSpec.setTrustStorePassword("password");
        activationSpec.setKeyStore("client.keystore");
        activationSpec.setKeyStorePassword("password");
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

        ActiveMQMessage msg = (ActiveMQMessage) advisory.receive(1000);
        if (msg != null) {
            assertEquals("Prefetch size hasn't been set", 1, ((ConsumerInfo) msg.getDataStructure()).getPrefetchSize());
        } else {
            fail("Consumer hasn't been created");
        }

        // Send the broker a message to that endpoint
        MessageProducer producer = session.createProducer(new ActiveMQQueue("TEST"));
        producer.send(session.createTextMessage("Hello!"));

        connection.close();

        // Wait for the message to be delivered.
        assertTrue(messageDelivered.await(5000, TimeUnit.MILLISECONDS));

        // Shut the Endpoint down.
        adapter.endpointDeactivation(messageEndpointFactory, activationSpec);
        adapter.stop();
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

    public static TrustManager[] getTrustManager() throws Exception {
        TrustManager[] trustStoreManagers = null;
        KeyStore trustedCertStore = KeyStore.getInstance(KEYSTORE_TYPE);

        trustedCertStore.load(new FileInputStream(TRUST_KEYSTORE), null);
        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        tmf.init(trustedCertStore);
        trustStoreManagers = tmf.getTrustManagers();
        return trustStoreManagers;
    }

    public static KeyManager[] getKeyManager() throws Exception {
        KeyManagerFactory kmf =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance(KEYSTORE_TYPE);
        KeyManager[] keystoreManagers = null;

        byte[] sslCert = loadClientCredential(SERVER_KEYSTORE);

        if (sslCert != null && sslCert.length > 0) {
            ByteArrayInputStream bin = new ByteArrayInputStream(sslCert);
            ks.load(bin, PASSWORD.toCharArray());
            kmf.init(ks, PASSWORD.toCharArray());
            keystoreManagers = kmf.getKeyManagers();
        }
        return keystoreManagers;
    }

    private static byte[] loadClientCredential(String fileName) throws IOException {
        if (fileName == null) {
            return null;
        }

        FileInputStream in = new FileInputStream(fileName);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[512];
        int i = in.read(buf);

        while (i > 0) {
            out.write(buf, 0, i);
            i = in.read(buf);
        }

        in.close();
        return out.toByteArray();
    }

    private void makeSSLConnection(SSLContext context, String enabledSuites[], TransportConnector connector) throws Exception {
        SSLSocket sslSocket = (SSLSocket) context.getSocketFactory().createSocket("localhost", connector.getUri().getPort());

        if (enabledSuites != null) {
            sslSocket.setEnabledCipherSuites(enabledSuites);
        }

        sslSocket.setSoTimeout(5000);
        sslSocket.getSession();
        sslSocket.startHandshake();
    }
}

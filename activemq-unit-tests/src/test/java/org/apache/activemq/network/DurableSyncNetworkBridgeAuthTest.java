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
package org.apache.activemq.network;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerSubscriptionInfo;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class DurableSyncNetworkBridgeAuthTest extends AbstractDurableSyncNetworkBridgeTest {

    protected static final Logger LOG = LoggerFactory.getLogger(DurableSyncNetworkBridgeAuthTest.class);

    @Parameters(name="duplex={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {true},
                {false}
        });
    }

    @Rule
    public Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    static {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
    }

    private static final String USER_PASSWORD = "password";
    private final boolean duplex;
    private final AtomicReference<BrokerSubscriptionInfo> brokerSubInfo = new AtomicReference<>();
    private String ncPassword = USER_PASSWORD;

    public DurableSyncNetworkBridgeAuthTest(boolean duplex) {
        this.duplex = duplex;
    }

    @Before
    public void setUp() throws Exception {
        this.ncPassword = USER_PASSWORD;
        this.brokerSubInfo.set(null);
    }

    @After
    public void tearDown() throws Exception {
        doTearDown();
    }

    @Test
    public void testAuthSuccess() throws Exception {
        doSetUp(true, true, tempFolder.newFolder(), tempFolder.newFolder(),
                TimeUnit.SECONDS.toMillis(15));

        // When the local broker starts the bridge it will send its BrokerSubscriptionInfo list
        // automatically on connect so the remote broker will always receive it. However, the
        // remote broker should only send back its list after the connection is properly authenticated.
        assertTrue(Wait.waitFor(() -> brokerSubInfo.get() != null,5000,10));

        // Simulate a connection exception and reconnect, we should receive again
        brokerSubInfo.set(null);
        localBroker.getNetworkConnectors().get(0).activeBridges().stream()
                .findFirst().orElseThrow().serviceRemoteException(new Exception());
        assertTrue(Wait.waitFor(() -> brokerSubInfo.get() != null,5000,10));
    }

    @Test
    public void testAuthFailure() throws Exception {
        this.ncPassword = "badpassword";
        try {
            // set a shorter wait time, it won't connect with bad password
            doSetUp(true, true, tempFolder.newFolder(), tempFolder.newFolder(),
                    TimeUnit.SECONDS.toMillis(5));
            throw new IllegalStateException("Should have received assertion error with bad password");
        } catch (AssertionError e) {
            // expected
        }

        // Because the local broker was not authenticated by the remote broker, the local broker
        // should not have received back the BrokerSubscriptionInfo
        assertNull(brokerSubInfo.get());
    }

    @Test
    public void testRestartSync() throws Exception {
        doSetUp(true, true, tempFolder.newFolder(), tempFolder.newFolder(),
                TimeUnit.SECONDS.toMillis(15));

        // When the local broker starts the bridge it will send its BrokerSubscriptionInfo list
        // automatically on connect so the remote broker will always receive it. However, the
        // remote broker should only send back its list after the connection is properly authenticated.
        assertTrue(Wait.waitFor(() -> brokerSubInfo.get() != null,5000,10));

        // Restart, should receive again with new connection
        brokerSubInfo.set(null);
        restartRemoteBroker();

        // Wait for the reconnect and receive of BrokerSubInfo
        assertTrue(Wait.waitFor(() -> brokerSubInfo.get() != null,5000,10));
    }

    protected void doSetUp(boolean deleteAllMessages, boolean startNetworkConnector, File localDataDir,
            File remoteDataDir, long waitForStart) throws Exception {
        doSetUpRemoteBroker(deleteAllMessages, remoteDataDir, 0);
        doSetUpLocalBroker(deleteAllMessages, startNetworkConnector, localDataDir);
        //Wait for the bridge to be fully started
        if (startNetworkConnector) {
            waitForBridgeFullyStarted(waitForStart, duplex);
        }
    }

    protected void doSetUpLocalBroker(boolean deleteAllMessages, boolean startNetworkConnector,
            File dataDir) throws Exception {
        localBroker = createLocalBroker(dataDir, startNetworkConnector);
        localBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        localBroker.start();
        localBroker.waitUntilStarted();

        if (startNetworkConnector) {
            // Best-effort wait for the bridge to appear. Do NOT use assertTrue here
            // because some tests restart localBroker before remoteBroker is running,
            // relying on the bridge connecting later when remoteBroker restarts.
            // Tests that need the bridge to be fully started call assertBridgeStarted() explicitly.
            // Keep timeout short (5s) to avoid growing the NC reconnect backoff too much,
            // which would delay bridge formation when the remote broker starts later.
            Wait.waitFor(() -> localBroker.getNetworkConnectors().get(0).activeBridges().size() == 1,
                         TimeUnit.SECONDS.toMillis(5), 500);
        }

    }

    protected void doSetUpRemoteBroker(boolean deleteAllMessages, File dataDir, int port) throws Exception {
        remoteBroker = createRemoteBroker(dataDir, port);
        remoteBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
    }

    protected BrokerService createLocalBroker(File dataDir, boolean startNetworkConnector) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("localBroker");
        brokerService.setDataDirectoryFile(dataDir);
        KahaDBPersistenceAdapter adapter = new KahaDBPersistenceAdapter();
        adapter.setDirectory(dataDir);
        adapter.setJournalDiskSyncStrategy(JournalDiskSyncStrategy.PERIODIC.name());
        brokerService.setPersistenceAdapter(adapter);

        if (startNetworkConnector) {
            brokerService.addNetworkConnector(configureLocalNetworkConnector());
        }

        //Use auto+nio+ssl to test out the transport works with bridging
        brokerService.addConnector("auto+nio+ssl://localhost:0");

        return brokerService;
    }

    @Override
    protected NetworkConnector configureLocalNetworkConnector() throws Exception {
        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI = transportConnectors.get(0).getConnectUri();
        String uri = "static:(" + remoteURI + ")";
        NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri)) {
            @Override
            protected NetworkBridge createBridge(Transport localTransport,
                    Transport remoteTransport, DiscoveryEvent event) {
                // Add a listener so we can capture if the remote broker sends
                // back a BrokerSubscriptionInfo object
                final Transport remoteFilter = new TransportFilter(remoteTransport) {
                    @Override
                    public void onCommand(Object command) {
                        if (command instanceof BrokerSubscriptionInfo) {
                            if (brokerSubInfo.get() != null) {
                                throw new IllegalStateException("Received BrokerSubscriptionInfo more than once.");
                            }
                            brokerSubInfo.set((BrokerSubscriptionInfo) command);
                        }
                        super.onCommand(command);
                    }
                };
                return super.createBridge(localTransport, remoteFilter, event);
            }
        };
        connector.setName("networkConnector");
        connector.setUserName("user1");
        connector.setPassword(ncPassword);
        connector.setDecreaseNetworkConsumerPriority(false);
        connector.setConduitSubscriptions(true);
        connector.setDuplex(duplex);
        connector.setStaticBridge(false);
        connector.setSyncDurableSubs(true);
        connector.setDynamicallyIncludedDestinations(List.of(new ActiveMQTopic("include.test.>")));
        return connector;
    }

    protected BrokerService createRemoteBroker(File dataDir, int port) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("remoteBroker");
        brokerService.setUseJmx(false);
        brokerService.setDataDirectoryFile(dataDir);
        KahaDBPersistenceAdapter adapter = new KahaDBPersistenceAdapter();
        adapter.setDirectory(dataDir);
        adapter.setJournalDiskSyncStrategy(JournalDiskSyncStrategy.PERIODIC.name());
        brokerService.setPersistenceAdapter(adapter);

        // Add authentication to the remote broker
        AuthenticationUser user = new AuthenticationUser("user1", USER_PASSWORD, "group1");
        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin();
        authenticationPlugin.setUsers(List.of(user));
        brokerService.setPlugins(new BrokerPlugin[] {authenticationPlugin});

        brokerService.addConnector("auto+nio+ssl://localhost:" + port);

        return brokerService;
    }

}

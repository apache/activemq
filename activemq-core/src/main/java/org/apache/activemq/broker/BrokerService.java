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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionMetaData;
import org.apache.activemq.ConfigurationException;
import org.apache.activemq.Service;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.broker.cluster.ConnectionSplitBroker;
import org.apache.activemq.broker.ft.MasterConnector;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.ConnectorView;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.FTConnectorView;
import org.apache.activemq.broker.jmx.JmsConnectorView;
import org.apache.activemq.broker.jmx.JobSchedulerView;
import org.apache.activemq.broker.jmx.JobSchedulerViewMBean;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.NetworkConnectorView;
import org.apache.activemq.broker.jmx.NetworkConnectorViewMBean;
import org.apache.activemq.broker.jmx.ProxyConnectorView;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFactory;
import org.apache.activemq.broker.region.DestinationFactoryImpl;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.MirroredQueue;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.broker.scheduler.SchedulerBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.network.ConnectionFilter;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.network.jms.JmsConnector;
import org.apache.activemq.proxy.ProxyConnector;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterFactory;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.plist.PListStore;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.vm.VMTransportFactory;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Manages the lifecycle of an ActiveMQ Broker. A BrokerService consists of a
 * number of transport connectors, network connectors and a bunch of properties
 * which can be used to configure the broker as its lazily created.
 * 
 * @version $Revision: 1.1 $
 * @org.apache.xbean.XBean
 */
public class BrokerService implements Service {
    protected CountDownLatch slaveStartSignal = new CountDownLatch(1);
    public static final String DEFAULT_PORT = "61616";
    public static final String LOCAL_HOST_NAME;
    public static final String DEFAULT_BROKER_NAME = "localhost";
    private static final Logger LOG = LoggerFactory.getLogger(BrokerService.class);
    private static final long serialVersionUID = 7353129142305630237L;
    private boolean useJmx = true;
    private boolean enableStatistics = true;
    private boolean persistent = true;
    private boolean populateJMSXUserID;
    private boolean useShutdownHook = true;
    private boolean useLoggingForShutdownErrors;
    private boolean shutdownOnMasterFailure;
    private boolean shutdownOnSlaveFailure;
    private boolean waitForSlave;
    private long waitForSlaveTimeout = 600000L;
    private boolean passiveSlave;
    private String brokerName = DEFAULT_BROKER_NAME;
    private File dataDirectoryFile;
    private File tmpDataDirectory;
    private Broker broker;
    private BrokerView adminView;
    private ManagementContext managementContext;
    private ObjectName brokerObjectName;
    private TaskRunnerFactory taskRunnerFactory;
    private TaskRunnerFactory persistenceTaskRunnerFactory;
    private SystemUsage systemUsage;
    private SystemUsage producerSystemUsage;
    private SystemUsage consumerSystemUsaage;
    private PersistenceAdapter persistenceAdapter;
    private PersistenceAdapterFactory persistenceFactory;
    protected DestinationFactory destinationFactory;
    private MessageAuthorizationPolicy messageAuthorizationPolicy;
    private final List<TransportConnector> transportConnectors = new CopyOnWriteArrayList<TransportConnector>();
    private final List<NetworkConnector> networkConnectors = new CopyOnWriteArrayList<NetworkConnector>();
    private final List<ProxyConnector> proxyConnectors = new CopyOnWriteArrayList<ProxyConnector>();
    private final List<JmsConnector> jmsConnectors = new CopyOnWriteArrayList<JmsConnector>();
    private final List<Service> services = new ArrayList<Service>();
    private MasterConnector masterConnector;
    private String masterConnectorURI;
    private transient Thread shutdownHook;
    private String[] transportConnectorURIs;
    private String[] networkConnectorURIs;
    private JmsConnector[] jmsBridgeConnectors; // these are Jms to Jms bridges
    // to other jms messaging
    // systems
    private boolean deleteAllMessagesOnStartup;
    private boolean advisorySupport = true;
    private URI vmConnectorURI;
    private String defaultSocketURIString;
    private PolicyMap destinationPolicy;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private BrokerPlugin[] plugins;
    private boolean keepDurableSubsActive = true;
    private boolean useVirtualTopics = true;
    private boolean useMirroredQueues = false;
    private boolean useTempMirroredQueues = true;
    private BrokerId brokerId;
    private DestinationInterceptor[] destinationInterceptors;
    private ActiveMQDestination[] destinations;
    private PListStore tempDataStore;
    private int persistenceThreadPriority = Thread.MAX_PRIORITY;
    private boolean useLocalHostBrokerName;
    private final CountDownLatch stoppedLatch = new CountDownLatch(1);
    private final CountDownLatch startedLatch = new CountDownLatch(1);
    private boolean supportFailOver;
    private Broker regionBroker;
    private int producerSystemUsagePortion = 60;
    private int consumerSystemUsagePortion = 40;
    private boolean splitSystemUsageForProducersConsumers;
    private boolean monitorConnectionSplits = false;
    private int taskRunnerPriority = Thread.NORM_PRIORITY;
    private boolean dedicatedTaskRunner;
    private boolean cacheTempDestinations = false;// useful for failover
    private int timeBeforePurgeTempDestinations = 5000;
    private final List<Runnable> shutdownHooks = new ArrayList<Runnable>();
    private boolean systemExitOnShutdown;
    private int systemExitOnShutdownExitCode;
    private SslContext sslContext;
    private boolean forceStart = false;
    private IOExceptionHandler ioExceptionHandler;
    private boolean schedulerSupport = false;
    private File schedulerDirectoryFile;
    private Scheduler scheduler;
    private ThreadPoolExecutor executor;
    private boolean slave = true;
    private int schedulePeriodForDestinationPurge=5000;
    private BrokerContext brokerContext;
    
    
	static {
        String localHostName = "localhost";
        try {
            localHostName =  InetAddressUtil.getLocalHostName();
        } catch (UnknownHostException e) {
            LOG.error("Failed to resolve localhost");
        }
        LOCAL_HOST_NAME = localHostName;
    }

    @Override
    public String toString() {
        return "BrokerService[" + getBrokerName() + "]";
    }

    /**
     * Adds a new transport connector for the given bind address
     * 
     * @return the newly created and added transport connector
     * @throws Exception
     */
    public TransportConnector addConnector(String bindAddress) throws Exception {
        return addConnector(new URI(bindAddress));
    }

    /**
     * Adds a new transport connector for the given bind address
     * 
     * @return the newly created and added transport connector
     * @throws Exception
     */
    public TransportConnector addConnector(URI bindAddress) throws Exception {
        return addConnector(createTransportConnector(bindAddress));
    }

    /**
     * Adds a new transport connector for the given TransportServer transport
     * 
     * @return the newly created and added transport connector
     * @throws Exception
     */
    public TransportConnector addConnector(TransportServer transport) throws Exception {
        return addConnector(new TransportConnector(transport));
    }

    /**
     * Adds a new transport connector
     * 
     * @return the transport connector
     * @throws Exception
     */
    public TransportConnector addConnector(TransportConnector connector) throws Exception {
        transportConnectors.add(connector);
        return connector;
    }

    /**
     * Stops and removes a transport connector from the broker.
     * 
     * @param connector
     * @return true if the connector has been previously added to the broker
     * @throws Exception
     */
    public boolean removeConnector(TransportConnector connector) throws Exception {
        boolean rc = transportConnectors.remove(connector);
        if (rc) {
            unregisterConnectorMBean(connector);
        }
        return rc;
    }

    /**
     * Adds a new network connector using the given discovery address
     * 
     * @return the newly created and added network connector
     * @throws Exception
     */
    public NetworkConnector addNetworkConnector(String discoveryAddress) throws Exception {
        return addNetworkConnector(new URI(discoveryAddress));
    }

    /**
     * Adds a new proxy connector using the given bind address
     * 
     * @return the newly created and added network connector
     * @throws Exception
     */
    public ProxyConnector addProxyConnector(String bindAddress) throws Exception {
        return addProxyConnector(new URI(bindAddress));
    }

    /**
     * Adds a new network connector using the given discovery address
     * 
     * @return the newly created and added network connector
     * @throws Exception
     */
    public NetworkConnector addNetworkConnector(URI discoveryAddress) throws Exception {
        if (!isAdvisorySupport()) {
            throw new javax.jms.IllegalStateException(
                    "Networks require advisory messages to function - advisories are currently disabled");
        }
        NetworkConnector connector = new DiscoveryNetworkConnector(discoveryAddress);
        return addNetworkConnector(connector);
    }

    /**
     * Adds a new proxy connector using the given bind address
     * 
     * @return the newly created and added network connector
     * @throws Exception
     */
    public ProxyConnector addProxyConnector(URI bindAddress) throws Exception {
        ProxyConnector connector = new ProxyConnector();
        connector.setBind(bindAddress);
        connector.setRemote(new URI("fanout:multicast://default"));
        return addProxyConnector(connector);
    }

    /**
     * Adds a new network connector to connect this broker to a federated
     * network
     */
    public NetworkConnector addNetworkConnector(NetworkConnector connector) throws Exception {
        connector.setBrokerService(this);
        URI uri = getVmConnectorURI();
        Map<String, String> map = new HashMap<String, String>(URISupport.parseParameters(uri));
        map.put("network", "true");
        uri = URISupport.createURIWithQuery(uri, URISupport.createQueryString(map));
        connector.setLocalUri(uri);
        // Set a connection filter so that the connector does not establish loop
        // back connections.
        connector.setConnectionFilter(new ConnectionFilter() {
            public boolean connectTo(URI location) {
                List<TransportConnector> transportConnectors = getTransportConnectors();
                for (Iterator<TransportConnector> iter = transportConnectors.iterator(); iter.hasNext();) {
                    try {
                        TransportConnector tc = iter.next();
                        if (location.equals(tc.getConnectUri())) {
                            return false;
                        }
                    } catch (Throwable e) {
                    }
                }
                return true;
            }
        });
        networkConnectors.add(connector);
        if (isUseJmx()) {
            registerNetworkConnectorMBean(connector);
        }
        return connector;
    }

    /**
     * Removes the given network connector without stopping it. The caller
     * should call {@link NetworkConnector#stop()} to close the connector
     */
    public boolean removeNetworkConnector(NetworkConnector connector) {
        boolean answer = networkConnectors.remove(connector);
        if (answer) {
            unregisterNetworkConnectorMBean(connector);
        }
        return answer;
    }

    public ProxyConnector addProxyConnector(ProxyConnector connector) throws Exception {
        URI uri = getVmConnectorURI();
        connector.setLocalUri(uri);
        proxyConnectors.add(connector);
        if (isUseJmx()) {
            registerProxyConnectorMBean(connector);
        }
        return connector;
    }

    public JmsConnector addJmsConnector(JmsConnector connector) throws Exception {
        connector.setBrokerService(this);
        jmsConnectors.add(connector);
        if (isUseJmx()) {
            registerJmsConnectorMBean(connector);
        }
        return connector;
    }

    public JmsConnector removeJmsConnector(JmsConnector connector) {
        if (jmsConnectors.remove(connector)) {
            return connector;
        }
        return null;
    }

    /**
     * @return Returns the masterConnectorURI.
     */
    public String getMasterConnectorURI() {
        return masterConnectorURI;
    }

    /**
     * @param masterConnectorURI
     *            The masterConnectorURI to set.
     */
    public void setMasterConnectorURI(String masterConnectorURI) {
        this.masterConnectorURI = masterConnectorURI;
    }

    /**
     * @return true if this Broker is a slave to a Master
     */
    public boolean isSlave() {
        return (masterConnector != null && masterConnector.isSlave()) ||
            (masterConnector != null && masterConnector.isStoppedBeforeStart()) ||
            (masterConnector == null && slave);
    }

    public void masterFailed() {
        if (shutdownOnMasterFailure) {
            LOG.error("The Master has failed ... shutting down");
            try {
                stop();
            } catch (Exception e) {
                LOG.error("Failed to stop for master failure", e);
            }
        } else {
            LOG.warn("Master Failed - starting all connectors");
            try {
                startAllConnectors();
                broker.nowMasterBroker();
            } catch (Exception e) {
                LOG.error("Failed to startAllConnectors", e);
            }
        }
    }

    public boolean isStarted() {
        return started.get();
    }

    public void start(boolean force) throws Exception {
        forceStart = force;
        stopped.set(false);
        started.set(false);
        start();
    }

    // Service interface
    // -------------------------------------------------------------------------

    protected boolean shouldAutostart() {
        return true;
    }

    /**
     *
     * @throws Exception
     * @org. apache.xbean.InitMethod
     */
    @PostConstruct
    public void autoStart() throws Exception {
        if(shouldAutostart()) {
            start();
        }
    }

    public void start() throws Exception {
        if (stopped.get() || !started.compareAndSet(false, true)) {
            // lets just ignore redundant start() calls
            // as its way too easy to not be completely sure if start() has been
            // called or not with the gazillion of different configuration
            // mechanisms
            // throw new IllegalStateException("Allready started.");
            return;
        }
        try {
        	if (systemExitOnShutdown && useShutdownHook) {
        		throw new ConfigurationException("'useShutdownHook' property cannot be be used with 'systemExitOnShutdown', please turn it off (useShutdownHook=false)");
        	}
            processHelperProperties();
            if (isUseJmx()) {
            	startManagementContext();
            }
            getPersistenceAdapter().setUsageManager(getProducerSystemUsage());
            getPersistenceAdapter().setBrokerName(getBrokerName());
            LOG.info("Using Persistence Adapter: " + getPersistenceAdapter());
            if (deleteAllMessagesOnStartup) {
                deleteAllMessages();
            }
            getPersistenceAdapter().start();
            slave = false;
            startDestinations();
            addShutdownHook();
            getBroker().start();
            if (isUseJmx()) {
            	if (getManagementContext().isCreateConnector() && !getManagementContext().isConnectorStarted()) {
            		// try to restart management context
            		// typical for slaves that use the same ports as master
            		managementContext.stop();
            		startManagementContext();
            	}
                ManagedRegionBroker managedBroker = (ManagedRegionBroker) regionBroker;
                managedBroker.setContextBroker(broker);
            	adminView.setBroker(managedBroker);
            }
            BrokerRegistry.getInstance().bind(getBrokerName(), this);
            // see if there is a MasterBroker service and if so, configure
            // it and start it.
            for (Service service : services) {
                if (service instanceof MasterConnector) {
                    configureService(service);
                    service.start();
                }
            }
            if (!isSlave() && (this.masterConnector == null || isShutdownOnMasterFailure() == false)) {
                startAllConnectors();
            }
            if (!stopped.get()) {
                if (isUseJmx() && masterConnector != null) {
                    registerFTConnectorMBean(masterConnector);
                }
            }
            if (brokerId == null) {
                brokerId = broker.getBrokerId();
            }
            if (ioExceptionHandler == null) {
            	setIoExceptionHandler(new DefaultIOExceptionHandler());
            }
            LOG.info("ActiveMQ JMS Message Broker (" + getBrokerName() + ", " + brokerId + ") started");
            getBroker().brokerServiceStarted();
            startedLatch.countDown();
        } catch (Exception e) {
            LOG.error("Failed to start ActiveMQ JMS Message Broker. Reason: " + e, e);
            try {
                if (!stopped.get()) {
                    stop();
                }
            } catch (Exception ex) {
                LOG.warn("Failed to stop broker after failure in start ", ex);
            }
            throw e;
        }
    }

    /**
     *
     * @throws Exception
     * @org.apache .xbean.DestroyMethod
     */
    @PreDestroy
    public void stop() throws Exception {
        if (!started.get()) {
            return;
        }
        
        if (systemExitOnShutdown) {
        	new Thread() {
        		@Override
                public void run() {
        			System.exit(systemExitOnShutdownExitCode);
        		}
        	}.start();
        }
        
        LOG.info("ActiveMQ Message Broker (" + getBrokerName() + ", " + brokerId + ") is shutting down");
        removeShutdownHook();
        ServiceStopper stopper = new ServiceStopper();
        if (services != null) {
            for (Service service : services) {
                stopper.stop(service);
            }
        }
        stopAllConnectors(stopper);
        // remove any VMTransports connected
        // this has to be done after services are stopped,
        // to avoid timimg issue with discovery (spinning up a new instance)
        BrokerRegistry.getInstance().unbind(getBrokerName());
        VMTransportFactory.stopped(getBrokerName());
        if (broker != null) {
            stopper.stop(broker);
            broker = null;
        }
        
        if (tempDataStore != null) {
            tempDataStore.stop();
            tempDataStore = null;
        }
        stopper.stop(persistenceAdapter);
        persistenceAdapter = null;
        slave = true;
        if (isUseJmx()) {
            stopper.stop(getManagementContext());
            managementContext = null;
        }
        // Clear SelectorParser cache to free memory
        SelectorParser.clearCache();
        stopped.set(true);
        stoppedLatch.countDown();
        if (masterConnectorURI == null) {
            // master start has not finished yet
            if (slaveStartSignal.getCount() == 1) {
                started.set(false);
                slaveStartSignal.countDown();
            }
        } else {
            for (Service service : services) {
                if (service instanceof MasterConnector) {
                    MasterConnector mConnector = (MasterConnector) service;
                    if (!mConnector.isSlave()) {
                        // means should be slave but not connected to master yet
                        started.set(false);
                        mConnector.stopBeforeConnected();
                    }
                }
            }
        }
        if (this.taskRunnerFactory != null) {
            this.taskRunnerFactory.shutdown();
            this.taskRunnerFactory = null;
        }
        if (this.scheduler != null) {
            this.scheduler.stop();
            this.scheduler = null;
        }
        if (this.executor != null) {
            this.executor.shutdownNow();
            this.executor = null;
        }
        
        this.destinationInterceptors = null;
        this.destinationFactory = null;
        
        LOG.info("ActiveMQ JMS Message Broker (" + getBrokerName() + ", " + brokerId + ") stopped");
        synchronized (shutdownHooks) {
            for (Runnable hook : shutdownHooks) {
                try {
                    hook.run();
                } catch (Throwable e) {
                    stopper.onException(hook, e);
                }
            }
        }
        
        stopper.throwFirstException();
    }
    
    public boolean checkQueueSize(String queueName) {
        long count = 0;
        long queueSize = 0;
        Map<ActiveMQDestination, Destination> destinationMap = regionBroker.getDestinationMap();
        for (Map.Entry<ActiveMQDestination, Destination> entry : destinationMap.entrySet()) {
            if (entry.getKey().isQueue()) {
                if (entry.getValue().getName().matches(queueName)) {
                    queueSize = entry.getValue().getDestinationStatistics().getMessages().getCount();
                    count += queueSize;
                    if (queueSize > 0) {
                        LOG.info("Queue has pending message:" + entry.getValue().getName() + " queueSize is:"
                                + queueSize);
                    }
                }
            }
        }
        return count == 0;
    }

    /**
     * This method (both connectorName and queueName are using regex to match)
     * 1. stop the connector (supposed the user input the connector which the
     * clients connect to) 2. to check whether there is any pending message on
     * the queues defined by queueName 3. supposedly, after stop the connector,
     * client should failover to other broker and pending messages should be
     * forwarded. if no pending messages, the method finally call stop to stop
     * the broker.
     * 
     * @param connectorName
     * @param queueName
     * @param timeout
     * @param pollInterval
     * @throws Exception
     */
    public void stopGracefully(String connectorName, String queueName, long timeout, long pollInterval)
            throws Exception {
        if (isUseJmx()) {
            if (connectorName == null || queueName == null || timeout <= 0) {
                throw new Exception(
                        "connectorName and queueName cannot be null and timeout should be >0 for stopGracefully.");
            }
            if (pollInterval <= 0) {
                pollInterval = 30;
            }
            LOG.info("Stop gracefully with connectorName:" + connectorName + " queueName:" + queueName + " timeout:"
                    + timeout + " pollInterval:" + pollInterval);
            TransportConnector connector;
            for (int i = 0; i < transportConnectors.size(); i++) {
                connector = transportConnectors.get(i);
                if (connector != null && connector.getName() != null && connector.getName().matches(connectorName)) {
                    connector.stop();
                }
            }
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < timeout * 1000) {
                // check quesize until it gets zero
                if (checkQueueSize(queueName)) {
                    stop();
                    break;
                } else {
                    Thread.sleep(pollInterval * 1000);
                }
            }
            if (stopped.get()) {
                LOG.info("Successfully stop the broker.");
            } else {
                LOG.info("There is still pending message on the queue. Please check and stop the broker manually.");
            }
        }
    }

    /**
     * A helper method to block the caller thread until the broker has been
     * stopped
     */
    public void waitUntilStopped() {
        while (isStarted() && !stopped.get()) {
            try {
                stoppedLatch.await();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * A helper method to block the caller thread until the broker has fully started
     * @return boolean true if wait succeeded false if broker was not started or was stopped
     */
    public boolean waitUntilStarted() {
        boolean waitSucceeded = false;
        while (isStarted() && !stopped.get() && !waitSucceeded) {
            try {
                waitSucceeded = startedLatch.await(100L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignore) {
            }
        }
        return waitSucceeded;
    }

    // Properties
    // -------------------------------------------------------------------------
    /**
     * Returns the message broker
     */
    public Broker getBroker() throws Exception {
        if (broker == null) {
            LOG.info("ActiveMQ " + ActiveMQConnectionMetaData.PROVIDER_VERSION + " JMS Message Broker ("
                    + getBrokerName() + ") is starting");
            LOG.info("For help or more information please see: http://activemq.apache.org/");
            broker = createBroker();
        }
        return broker;
    }

    /**
     * Returns the administration view of the broker; used to create and destroy
     * resources such as queues and topics. Note this method returns null if JMX
     * is disabled.
     */
    public BrokerView getAdminView() throws Exception {
        if (adminView == null) {
            // force lazy creation
            getBroker();
        }
        return adminView;
    }

    public void setAdminView(BrokerView adminView) {
        this.adminView = adminView;
    }

    public String getBrokerName() {
        return brokerName;
    }

    /**
     * Sets the name of this broker; which must be unique in the network
     * 
     * @param brokerName
     */
    public void setBrokerName(String brokerName) {
        if (brokerName == null) {
            throw new NullPointerException("The broker name cannot be null");
        }
        String str = brokerName.replaceAll("[^a-zA-Z0-9\\.\\_\\-\\:]", "_");
        if (!str.equals(brokerName)) {
            LOG.error("Broker Name: " + brokerName + " contained illegal characters - replaced with " + str);
        }
        this.brokerName = str.trim();
    }

    public PersistenceAdapterFactory getPersistenceFactory() {
        return persistenceFactory;
    }

    public File getDataDirectoryFile() {
        if (dataDirectoryFile == null) {
            dataDirectoryFile = new File(IOHelper.getDefaultDataDirectory());
        }
        return dataDirectoryFile;
    }

    public File getBrokerDataDirectory() {
        String brokerDir = getBrokerName();
        return new File(getDataDirectoryFile(), brokerDir);
    }

    /**
     * Sets the directory in which the data files will be stored by default for
     * the JDBC and Journal persistence adaptors.
     * 
     * @param dataDirectory
     *            the directory to store data files
     */
    public void setDataDirectory(String dataDirectory) {
        setDataDirectoryFile(new File(dataDirectory));
    }

    /**
     * Sets the directory in which the data files will be stored by default for
     * the JDBC and Journal persistence adaptors.
     * 
     * @param dataDirectoryFile
     *            the directory to store data files
     */
    public void setDataDirectoryFile(File dataDirectoryFile) {
        this.dataDirectoryFile = dataDirectoryFile;
    }

    /**
     * @return the tmpDataDirectory
     */
    public File getTmpDataDirectory() {
        if (tmpDataDirectory == null) {
            tmpDataDirectory = new File(getBrokerDataDirectory(), "tmp_storage");
        }
        return tmpDataDirectory;
    }

    /**
     * @param tmpDataDirectory
     *            the tmpDataDirectory to set
     */
    public void setTmpDataDirectory(File tmpDataDirectory) {
        this.tmpDataDirectory = tmpDataDirectory;
    }

    public void setPersistenceFactory(PersistenceAdapterFactory persistenceFactory) {
        this.persistenceFactory = persistenceFactory;
    }

    public void setDestinationFactory(DestinationFactory destinationFactory) {
        this.destinationFactory = destinationFactory;
    }

    public boolean isPersistent() {
        return persistent;
    }

    /**
     * Sets whether or not persistence is enabled or disabled.
     */
    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public boolean isPopulateJMSXUserID() {
        return populateJMSXUserID;
    }

    /**
     * Sets whether or not the broker should populate the JMSXUserID header.
     */
    public void setPopulateJMSXUserID(boolean populateJMSXUserID) {
        this.populateJMSXUserID = populateJMSXUserID;
    }

    public SystemUsage getSystemUsage() {
        try {
            if (systemUsage == null) {
                systemUsage = new SystemUsage("Main", getPersistenceAdapter(), getTempDataStore());
                systemUsage.setExecutor(getExecutor());
                systemUsage.getMemoryUsage().setLimit(1024 * 1024 * 64); // Default
                                                                         // 64
                                                                         // Meg
                systemUsage.getTempUsage().setLimit(1024L * 1024 * 1024 * 100); // 10
                                                                                // Gb
                systemUsage.getStoreUsage().setLimit(1024L * 1024 * 1024 * 100); // 100
                                                                                 // GB
                addService(this.systemUsage);
            }
            return systemUsage;
        } catch (IOException e) {
            LOG.error("Cannot create SystemUsage", e);
            throw new RuntimeException("Fatally failed to create SystemUsage" + e.getMessage());
        }
    }

    public void setSystemUsage(SystemUsage memoryManager) {
        if (this.systemUsage != null) {
            removeService(this.systemUsage);
        }
        this.systemUsage = memoryManager;
        if (this.systemUsage.getExecutor()==null) {
            this.systemUsage.setExecutor(getExecutor());
        }
        addService(this.systemUsage);
    }

    /**
     * @return the consumerUsageManager
     * @throws IOException
     */
    public SystemUsage getConsumerSystemUsage() throws IOException {
        if (this.consumerSystemUsaage == null) {
            if (splitSystemUsageForProducersConsumers) {
                this.consumerSystemUsaage = new SystemUsage(getSystemUsage(), "Consumer");
                float portion = consumerSystemUsagePortion / 100f;
                this.consumerSystemUsaage.getMemoryUsage().setUsagePortion(portion);
                addService(this.consumerSystemUsaage);
            } else {
                consumerSystemUsaage = getSystemUsage();
            }
        }
        return this.consumerSystemUsaage;
    }

    /**
     * @param consumerSystemUsaage
     *            the storeSystemUsage to set
     */
    public void setConsumerSystemUsage(SystemUsage consumerSystemUsaage) {
        if (this.consumerSystemUsaage != null) {
            removeService(this.consumerSystemUsaage);
        }
        this.consumerSystemUsaage = consumerSystemUsaage;
        addService(this.consumerSystemUsaage);
    }

    /**
     * @return the producerUsageManager
     * @throws IOException
     */
    public SystemUsage getProducerSystemUsage() throws IOException {
        if (producerSystemUsage == null) {
            if (splitSystemUsageForProducersConsumers) {
                producerSystemUsage = new SystemUsage(getSystemUsage(), "Producer");
                float portion = producerSystemUsagePortion / 100f;
                producerSystemUsage.getMemoryUsage().setUsagePortion(portion);
                addService(producerSystemUsage);
            } else {
                producerSystemUsage = getSystemUsage();
            }
        }
        return producerSystemUsage;
    }

    /**
     * @param producerUsageManager
     *            the producerUsageManager to set
     */
    public void setProducerSystemUsage(SystemUsage producerUsageManager) {
        if (this.producerSystemUsage != null) {
            removeService(this.producerSystemUsage);
        }
        this.producerSystemUsage = producerUsageManager;
        addService(this.producerSystemUsage);
    }

    public PersistenceAdapter getPersistenceAdapter() throws IOException {
        if (persistenceAdapter == null) {
            persistenceAdapter = createPersistenceAdapter();
            configureService(persistenceAdapter);
            this.persistenceAdapter = registerPersistenceAdapterMBean(persistenceAdapter);
        }
        return persistenceAdapter;
    }

    /**
     * Sets the persistence adaptor implementation to use for this broker
     * 
     * @throws IOException
     */
    public void setPersistenceAdapter(PersistenceAdapter persistenceAdapter) throws IOException {
        this.persistenceAdapter = persistenceAdapter;
        configureService(this.persistenceAdapter);
        this.persistenceAdapter = registerPersistenceAdapterMBean(persistenceAdapter);
    }

    public TaskRunnerFactory getTaskRunnerFactory() {
        if (this.taskRunnerFactory == null) {
            this.taskRunnerFactory = new TaskRunnerFactory("BrokerService["+getBrokerName()+"] Task", getTaskRunnerPriority(), true, 1000,
                    isDedicatedTaskRunner());
        }
        return this.taskRunnerFactory;
    }

    public void setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory) {
        this.taskRunnerFactory = taskRunnerFactory;
    }

    public TaskRunnerFactory getPersistenceTaskRunnerFactory() {
        if (taskRunnerFactory == null) {
            persistenceTaskRunnerFactory = new TaskRunnerFactory("Persistence Adaptor Task", persistenceThreadPriority,
                    true, 1000, isDedicatedTaskRunner());
        }
        return persistenceTaskRunnerFactory;
    }

    public void setPersistenceTaskRunnerFactory(TaskRunnerFactory persistenceTaskRunnerFactory) {
        this.persistenceTaskRunnerFactory = persistenceTaskRunnerFactory;
    }

    public boolean isUseJmx() {
        return useJmx;
    }

    public boolean isEnableStatistics() {
        return enableStatistics;
    }

    /**
     * Sets whether or not the Broker's services enable statistics or not.
     */
    public void setEnableStatistics(boolean enableStatistics) {
        this.enableStatistics = enableStatistics;
    }

    /**
     * Sets whether or not the Broker's services should be exposed into JMX or
     * not.
     */
    public void setUseJmx(boolean useJmx) {
        this.useJmx = useJmx;
    }

    public ObjectName getBrokerObjectName() throws IOException {
        if (brokerObjectName == null) {
            brokerObjectName = createBrokerObjectName();
        }
        return brokerObjectName;
    }

    /**
     * Sets the JMX ObjectName for this broker
     */
    public void setBrokerObjectName(ObjectName brokerObjectName) {
        this.brokerObjectName = brokerObjectName;
    }

    public ManagementContext getManagementContext() {
        if (managementContext == null) {
            managementContext = new ManagementContext();
        }
        return managementContext;
    }

    public void setManagementContext(ManagementContext managementContext) {
        this.managementContext = managementContext;
    }

    public NetworkConnector getNetworkConnectorByName(String connectorName) {
        for (NetworkConnector connector : networkConnectors) {
            if (connector.getName().equals(connectorName)) {
                return connector;
            }
        }
        return null;
    }

    public String[] getNetworkConnectorURIs() {
        return networkConnectorURIs;
    }

    public void setNetworkConnectorURIs(String[] networkConnectorURIs) {
        this.networkConnectorURIs = networkConnectorURIs;
    }

    public TransportConnector getConnectorByName(String connectorName) {
        for (TransportConnector connector : transportConnectors) {
            if (connector.getName().equals(connectorName)) {
                return connector;
            }
        }
        return null;
    }
    
    public Map<String, String> getTransportConnectorURIsAsMap() {
        Map<String, String> answer = new HashMap<String, String>();
        for (TransportConnector connector : transportConnectors) {
            try {
                URI uri = connector.getConnectUri();
                String scheme = uri.getScheme();
                if (scheme != null) {
                    answer.put(scheme.toLowerCase(), uri.toString());
                }
            } catch (Exception e) {
                LOG.debug("Failed to read URI to build transportURIsAsMap", e);
            }
        }
        return answer;
    }

    public String[] getTransportConnectorURIs() {
        return transportConnectorURIs;
    }

    public void setTransportConnectorURIs(String[] transportConnectorURIs) {
        this.transportConnectorURIs = transportConnectorURIs;
    }

    /**
     * @return Returns the jmsBridgeConnectors.
     */
    public JmsConnector[] getJmsBridgeConnectors() {
        return jmsBridgeConnectors;
    }

    /**
     * @param jmsConnectors
     *            The jmsBridgeConnectors to set.
     */
    public void setJmsBridgeConnectors(JmsConnector[] jmsConnectors) {
        this.jmsBridgeConnectors = jmsConnectors;
    }

    public Service[] getServices() {
        return services.toArray(new Service[0]);
    }

    /**
     * Sets the services associated with this broker such as a
     * {@link MasterConnector}
     */
    public void setServices(Service[] services) {
        this.services.clear();
        if (services != null) {
            for (int i = 0; i < services.length; i++) {
                this.services.add(services[i]);
            }
        }
    }

    /**
     * Adds a new service so that it will be started as part of the broker
     * lifecycle
     */
    public void addService(Service service) {
        services.add(service);
    }

    public void removeService(Service service) {
        services.remove(service);
    }

    public boolean isUseLoggingForShutdownErrors() {
        return useLoggingForShutdownErrors;
    }

    /**
     * Sets whether or not we should use commons-logging when reporting errors
     * when shutting down the broker
     */
    public void setUseLoggingForShutdownErrors(boolean useLoggingForShutdownErrors) {
        this.useLoggingForShutdownErrors = useLoggingForShutdownErrors;
    }

    public boolean isUseShutdownHook() {
        return useShutdownHook;
    }

    /**
     * Sets whether or not we should use a shutdown handler to close down the
     * broker cleanly if the JVM is terminated. It is recommended you leave this
     * enabled.
     */
    public void setUseShutdownHook(boolean useShutdownHook) {
        this.useShutdownHook = useShutdownHook;
    }

    public boolean isAdvisorySupport() {
        return advisorySupport;
    }

    /**
     * Allows the support of advisory messages to be disabled for performance
     * reasons.
     */
    public void setAdvisorySupport(boolean advisorySupport) {
        this.advisorySupport = advisorySupport;
    }

    public List<TransportConnector> getTransportConnectors() {
        return new ArrayList<TransportConnector>(transportConnectors);
    }

    /**
     * Sets the transport connectors which this broker will listen on for new
     * clients
     * 
     * @org.apache.xbean.Property 
     *                            nestedType="org.apache.activemq.broker.TransportConnector"
     */
    public void setTransportConnectors(List<TransportConnector> transportConnectors) throws Exception {
        for (Iterator<TransportConnector> iter = transportConnectors.iterator(); iter.hasNext();) {
            TransportConnector connector = iter.next();
            addConnector(connector);
        }
    }

    public List<NetworkConnector> getNetworkConnectors() {
        return new ArrayList<NetworkConnector>(networkConnectors);
    }

    public List<ProxyConnector> getProxyConnectors() {
        return new ArrayList<ProxyConnector>(proxyConnectors);
    }

    /**
     * Sets the network connectors which this broker will use to connect to
     * other brokers in a federated network
     * 
     * @org.apache.xbean.Property 
     *                            nestedType="org.apache.activemq.network.NetworkConnector"
     */
    public void setNetworkConnectors(List networkConnectors) throws Exception {
        for (Iterator iter = networkConnectors.iterator(); iter.hasNext();) {
            NetworkConnector connector = (NetworkConnector) iter.next();
            addNetworkConnector(connector);
        }
    }

    /**
     * Sets the network connectors which this broker will use to connect to
     * other brokers in a federated network
     */
    public void setProxyConnectors(List proxyConnectors) throws Exception {
        for (Iterator iter = proxyConnectors.iterator(); iter.hasNext();) {
            ProxyConnector connector = (ProxyConnector) iter.next();
            addProxyConnector(connector);
        }
    }

    public PolicyMap getDestinationPolicy() {
        return destinationPolicy;
    }

    /**
     * Sets the destination specific policies available either for exact
     * destinations or for wildcard areas of destinations.
     */
    public void setDestinationPolicy(PolicyMap policyMap) {
        this.destinationPolicy = policyMap;
    }

    public BrokerPlugin[] getPlugins() {
        return plugins;
    }

    /**
     * Sets a number of broker plugins to install such as for security
     * authentication or authorization
     */
    public void setPlugins(BrokerPlugin[] plugins) {
        this.plugins = plugins;
    }

    public MessageAuthorizationPolicy getMessageAuthorizationPolicy() {
        return messageAuthorizationPolicy;
    }

    /**
     * Sets the policy used to decide if the current connection is authorized to
     * consume a given message
     */
    public void setMessageAuthorizationPolicy(MessageAuthorizationPolicy messageAuthorizationPolicy) {
        this.messageAuthorizationPolicy = messageAuthorizationPolicy;
    }

    /**
     * Delete all messages from the persistent store
     * 
     * @throws IOException
     */
    public void deleteAllMessages() throws IOException {
        getPersistenceAdapter().deleteAllMessages();
    }

    public boolean isDeleteAllMessagesOnStartup() {
        return deleteAllMessagesOnStartup;
    }

    /**
     * Sets whether or not all messages are deleted on startup - mostly only
     * useful for testing.
     */
    public void setDeleteAllMessagesOnStartup(boolean deletePersistentMessagesOnStartup) {
        this.deleteAllMessagesOnStartup = deletePersistentMessagesOnStartup;
    }

    public URI getVmConnectorURI() {
        if (vmConnectorURI == null) {
            try {
                vmConnectorURI = new URI("vm://" + getBrokerName().replaceAll("[^a-zA-Z0-9\\.\\_\\-]", "_"));
            } catch (URISyntaxException e) {
                LOG.error("Badly formed URI from " + getBrokerName(), e);
            }
        }
        return vmConnectorURI;
    }

    public void setVmConnectorURI(URI vmConnectorURI) {
        this.vmConnectorURI = vmConnectorURI;
    }
    
    public String getDefaultSocketURIString() {
       
            if (started.get()) {
                if (this.defaultSocketURIString ==null) {
                    for (TransportConnector tc:this.transportConnectors) {
                        String result = null;
                        try {
                            result = tc.getPublishableConnectString();
                        } catch (Exception e) {
                          LOG.warn("Failed to get the ConnectURI for "+tc,e);
                        }
                        if (result != null) {
                            this.defaultSocketURIString =result;
                            break;
                        }
                    }
                }
                return this.defaultSocketURIString;
            }
       return null;
    }

    /**
     * @return Returns the shutdownOnMasterFailure.
     */
    public boolean isShutdownOnMasterFailure() {
        return shutdownOnMasterFailure;
    }

    /**
     * @param shutdownOnMasterFailure
     *            The shutdownOnMasterFailure to set.
     */
    public void setShutdownOnMasterFailure(boolean shutdownOnMasterFailure) {
        this.shutdownOnMasterFailure = shutdownOnMasterFailure;
    }

    public boolean isKeepDurableSubsActive() {
        return keepDurableSubsActive;
    }

    public void setKeepDurableSubsActive(boolean keepDurableSubsActive) {
        this.keepDurableSubsActive = keepDurableSubsActive;
    }

    public boolean isUseVirtualTopics() {
        return useVirtualTopics;
    }

    /**
     * Sets whether or not <a
     * href="http://activemq.apache.org/virtual-destinations.html">Virtual
     * Topics</a> should be supported by default if they have not been
     * explicitly configured.
     */
    public void setUseVirtualTopics(boolean useVirtualTopics) {
        this.useVirtualTopics = useVirtualTopics;
    }

    public DestinationInterceptor[] getDestinationInterceptors() {
        return destinationInterceptors;
    }

    public boolean isUseMirroredQueues() {
        return useMirroredQueues;
    }

    /**
     * Sets whether or not <a
     * href="http://activemq.apache.org/mirrored-queues.html">Mirrored
     * Queues</a> should be supported by default if they have not been
     * explicitly configured.
     */
    public void setUseMirroredQueues(boolean useMirroredQueues) {
        this.useMirroredQueues = useMirroredQueues;
    }

    /**
     * Sets the destination interceptors to use
     */
    public void setDestinationInterceptors(DestinationInterceptor[] destinationInterceptors) {
        this.destinationInterceptors = destinationInterceptors;
    }

    public ActiveMQDestination[] getDestinations() {
        return destinations;
    }

    /**
     * Sets the destinations which should be loaded/created on startup
     */
    public void setDestinations(ActiveMQDestination[] destinations) {
        this.destinations = destinations;
    }

    /**
     * @return the tempDataStore
     */
    public synchronized PListStore getTempDataStore() {
        if (tempDataStore == null) {
            if (!isPersistent()) {
                return null;
            }
            boolean result = true;
            boolean empty = true;
            try {
                File directory = getTmpDataDirectory();
                if (directory.exists() && directory.isDirectory()) {
                    File[] files = directory.listFiles();
                    if (files != null && files.length > 0) {
                        empty = false;
                        for (int i = 0; i < files.length; i++) {
                            File file = files[i];
                            if (!file.isDirectory()) {
                                result &= file.delete();
                            }
                        }
                    }
                }
                if (!empty) {
                    String str = result ? "Successfully deleted" : "Failed to delete";
                    LOG.info(str + " temporary storage");
                }
                this.tempDataStore = new PListStore();
                this.tempDataStore.setDirectory(getTmpDataDirectory());
                this.tempDataStore.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return tempDataStore;
    }

    /**
     * @param tempDataStore
     *            the tempDataStore to set
     */
    public void setTempDataStore(PListStore tempDataStore) {
        this.tempDataStore = tempDataStore;
    }

    public int getPersistenceThreadPriority() {
        return persistenceThreadPriority;
    }

    public void setPersistenceThreadPriority(int persistenceThreadPriority) {
        this.persistenceThreadPriority = persistenceThreadPriority;
    }

    /**
     * @return the useLocalHostBrokerName
     */
    public boolean isUseLocalHostBrokerName() {
        return this.useLocalHostBrokerName;
    }

    /**
     * @param useLocalHostBrokerName
     *            the useLocalHostBrokerName to set
     */
    public void setUseLocalHostBrokerName(boolean useLocalHostBrokerName) {
        this.useLocalHostBrokerName = useLocalHostBrokerName;
        if (useLocalHostBrokerName && !started.get() && brokerName == null || brokerName == DEFAULT_BROKER_NAME) {
            brokerName = LOCAL_HOST_NAME;
        }
    }

    /**
     * @return the supportFailOver
     */
    public boolean isSupportFailOver() {
        return this.supportFailOver;
    }

    /**
     * @param supportFailOver
     *            the supportFailOver to set
     */
    public void setSupportFailOver(boolean supportFailOver) {
        this.supportFailOver = supportFailOver;
    }

    /**
     * Looks up and lazily creates if necessary the destination for the given
     * JMS name
     */
    public Destination getDestination(ActiveMQDestination destination) throws Exception {
        return getBroker().addDestination(getAdminConnectionContext(), destination,false);
    }

    public void removeDestination(ActiveMQDestination destination) throws Exception {
        getBroker().removeDestination(getAdminConnectionContext(), destination, 0);
    }

    public int getProducerSystemUsagePortion() {
        return producerSystemUsagePortion;
    }

    public void setProducerSystemUsagePortion(int producerSystemUsagePortion) {
        this.producerSystemUsagePortion = producerSystemUsagePortion;
    }

    public int getConsumerSystemUsagePortion() {
        return consumerSystemUsagePortion;
    }

    public void setConsumerSystemUsagePortion(int consumerSystemUsagePortion) {
        this.consumerSystemUsagePortion = consumerSystemUsagePortion;
    }

    public boolean isSplitSystemUsageForProducersConsumers() {
        return splitSystemUsageForProducersConsumers;
    }

    public void setSplitSystemUsageForProducersConsumers(boolean splitSystemUsageForProducersConsumers) {
        this.splitSystemUsageForProducersConsumers = splitSystemUsageForProducersConsumers;
    }

    public boolean isMonitorConnectionSplits() {
        return monitorConnectionSplits;
    }

    public void setMonitorConnectionSplits(boolean monitorConnectionSplits) {
        this.monitorConnectionSplits = monitorConnectionSplits;
    }

    public int getTaskRunnerPriority() {
        return taskRunnerPriority;
    }

    public void setTaskRunnerPriority(int taskRunnerPriority) {
        this.taskRunnerPriority = taskRunnerPriority;
    }

    public boolean isDedicatedTaskRunner() {
        return dedicatedTaskRunner;
    }

    public void setDedicatedTaskRunner(boolean dedicatedTaskRunner) {
        this.dedicatedTaskRunner = dedicatedTaskRunner;
    }

    public boolean isCacheTempDestinations() {
        return cacheTempDestinations;
    }

    public void setCacheTempDestinations(boolean cacheTempDestinations) {
        this.cacheTempDestinations = cacheTempDestinations;
    }

    public int getTimeBeforePurgeTempDestinations() {
        return timeBeforePurgeTempDestinations;
    }

    public void setTimeBeforePurgeTempDestinations(int timeBeforePurgeTempDestinations) {
        this.timeBeforePurgeTempDestinations = timeBeforePurgeTempDestinations;
    }

    public boolean isUseTempMirroredQueues() {
        return useTempMirroredQueues;
    }

    public void setUseTempMirroredQueues(boolean useTempMirroredQueues) {
        this.useTempMirroredQueues = useTempMirroredQueues;
    }

    //
    // Implementation methods
    // -------------------------------------------------------------------------
    /**
     * Handles any lazy-creation helper properties which are added to make
     * things easier to configure inside environments such as Spring
     * 
     * @throws Exception
     */
    protected void processHelperProperties() throws Exception {
        boolean masterServiceExists = false;
        if (transportConnectorURIs != null) {
            for (int i = 0; i < transportConnectorURIs.length; i++) {
                String uri = transportConnectorURIs[i];
                addConnector(uri);
            }
        }
        if (networkConnectorURIs != null) {
            for (int i = 0; i < networkConnectorURIs.length; i++) {
                String uri = networkConnectorURIs[i];
                addNetworkConnector(uri);
            }
        }
        if (jmsBridgeConnectors != null) {
            for (int i = 0; i < jmsBridgeConnectors.length; i++) {
                addJmsConnector(jmsBridgeConnectors[i]);
            }
        }
        for (Service service : services) {
            if (service instanceof MasterConnector) {
                masterServiceExists = true;
                break;
            }
        }
        if (masterConnectorURI != null) {
            if (masterServiceExists) {
                throw new IllegalStateException(
                        "Cannot specify masterConnectorURI when a masterConnector is already registered via the services property");
            } else {
                addService(new MasterConnector(masterConnectorURI));
            }
        }
    }

    protected void stopAllConnectors(ServiceStopper stopper) {
        for (Iterator<NetworkConnector> iter = getNetworkConnectors().iterator(); iter.hasNext();) {
            NetworkConnector connector = iter.next();
            unregisterNetworkConnectorMBean(connector);
            stopper.stop(connector);
        }
        for (Iterator<ProxyConnector> iter = getProxyConnectors().iterator(); iter.hasNext();) {
            ProxyConnector connector = iter.next();
            stopper.stop(connector);
        }
        for (Iterator<JmsConnector> iter = jmsConnectors.iterator(); iter.hasNext();) {
            JmsConnector connector = iter.next();
            stopper.stop(connector);
        }
        for (Iterator<TransportConnector> iter = getTransportConnectors().iterator(); iter.hasNext();) {
            TransportConnector connector = iter.next();
            stopper.stop(connector);
        }
    }

    protected TransportConnector registerConnectorMBean(TransportConnector connector) throws IOException {
        try {
            ObjectName objectName = createConnectorObjectName(connector);
            connector = connector.asManagedConnector(getManagementContext(), objectName);
            ConnectorViewMBean view = new ConnectorView(connector);
            AnnotatedMBean.registerMBean(getManagementContext(), view, objectName);
            return connector;
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Transport Connector could not be registered in JMX: " + e.getMessage(), e);
        }
    }

    protected void unregisterConnectorMBean(TransportConnector connector) throws IOException {
        if (isUseJmx()) {
            try {
                ObjectName objectName = createConnectorObjectName(connector);
                getManagementContext().unregisterMBean(objectName);
            } catch (Throwable e) {
                throw IOExceptionSupport.create(
                        "Transport Connector could not be unregistered in JMX: " + e.getMessage(), e);
            }
        }
    }

    protected PersistenceAdapter registerPersistenceAdapterMBean(PersistenceAdapter adaptor) throws IOException {
        return adaptor;
    }

    protected void unregisterPersistenceAdapterMBean(PersistenceAdapter adaptor) throws IOException {
        if (isUseJmx()) {
        }
    }

    private ObjectName createConnectorObjectName(TransportConnector connector) throws MalformedObjectNameException {
        return new ObjectName(getManagementContext().getJmxDomainName() + ":" + "BrokerName="
                + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=Connector," + "ConnectorName="
                + JMXSupport.encodeObjectNamePart(connector.getName()));
    }

    protected void registerNetworkConnectorMBean(NetworkConnector connector) throws IOException {
        NetworkConnectorViewMBean view = new NetworkConnectorView(connector);
        try {
            ObjectName objectName = createNetworkConnectorObjectName(connector);
            connector.setObjectName(objectName);
            AnnotatedMBean.registerMBean(getManagementContext(), view, objectName);
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Network Connector could not be registered in JMX: " + e.getMessage(), e);
        }
    }

    protected ObjectName createNetworkConnectorObjectName(NetworkConnector connector)
            throws MalformedObjectNameException {
        return new ObjectName(getManagementContext().getJmxDomainName() + ":" + "BrokerName="
                + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=NetworkConnector,"
                + "NetworkConnectorName=" + JMXSupport.encodeObjectNamePart(connector.getName()));
    }


    public ObjectName createDuplexNetworkConnectorObjectName(String transport)
            throws MalformedObjectNameException {
        return new ObjectName(getManagementContext().getJmxDomainName() + ":" + "BrokerName="
                + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=NetworkConnector,"
                + "NetworkConnectorName=duplex" + JMXSupport.encodeObjectNamePart(transport));
    }

    protected void unregisterNetworkConnectorMBean(NetworkConnector connector) {
        if (isUseJmx()) {
            try {
                ObjectName objectName = createNetworkConnectorObjectName(connector);
                getManagementContext().unregisterMBean(objectName);
            } catch (Exception e) {
                LOG.error("Network Connector could not be unregistered from JMX: " + e, e);
            }
        }
    }

    protected void registerProxyConnectorMBean(ProxyConnector connector) throws IOException {
        ProxyConnectorView view = new ProxyConnectorView(connector);
        try {
            ObjectName objectName = new ObjectName(getManagementContext().getJmxDomainName() + ":" + "BrokerName="
                    + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=ProxyConnector,"
                    + "ProxyConnectorName=" + JMXSupport.encodeObjectNamePart(connector.getName()));
            AnnotatedMBean.registerMBean(getManagementContext(), view, objectName);
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
        }
    }

    protected void registerFTConnectorMBean(MasterConnector connector) throws IOException {
        FTConnectorView view = new FTConnectorView(connector);
        try {
            ObjectName objectName = new ObjectName(getManagementContext().getJmxDomainName() + ":" + "BrokerName="
                    + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=MasterConnector");
            AnnotatedMBean.registerMBean(getManagementContext(), view, objectName);
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
        }
    }

    protected void registerJmsConnectorMBean(JmsConnector connector) throws IOException {
        JmsConnectorView view = new JmsConnectorView(connector);
        try {
            ObjectName objectName = new ObjectName(getManagementContext().getJmxDomainName() + ":" + "BrokerName="
                    + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=JmsConnector,"
                    + "JmsConnectorName=" + JMXSupport.encodeObjectNamePart(connector.getName()));
            AnnotatedMBean.registerMBean(getManagementContext(), view, objectName);
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
        }
    }

    /**
     * Factory method to create a new broker
     * 
     * @throws Exception
     * @throws
     * @throws
     */
    protected Broker createBroker() throws Exception {
        regionBroker = createRegionBroker();
        Broker broker = addInterceptors(regionBroker);
        // Add a filter that will stop access to the broker once stopped
        broker = new MutableBrokerFilter(broker) {
            Broker old;

            @Override
            public void stop() throws Exception {
                old = this.next.getAndSet(new ErrorBroker("Broker has been stopped: " + this) {
                    // Just ignore additional stop actions.
                    @Override
                    public void stop() throws Exception {
                    }
                });
                old.stop();
            }

            @Override
            public void start() throws Exception {
                if (forceStart && old != null) {
                    this.next.set(old);
                }
                getNext().start();
            }
        };
        return broker;
    }

    /**
     * Factory method to create the core region broker onto which interceptors
     * are added
     * 
     * @throws Exception
     */
    protected Broker createRegionBroker() throws Exception {
        if (destinationInterceptors == null) {
            destinationInterceptors = createDefaultDestinationInterceptor();
        }
        configureServices(destinationInterceptors);
        DestinationInterceptor destinationInterceptor = new CompositeDestinationInterceptor(destinationInterceptors);
        if (destinationFactory == null) {
            destinationFactory = new DestinationFactoryImpl(this, getTaskRunnerFactory(), getPersistenceAdapter());
        }
        return createRegionBroker(destinationInterceptor);
    }

    protected Broker createRegionBroker(DestinationInterceptor destinationInterceptor) throws IOException {
        RegionBroker regionBroker;
        if (isUseJmx()) {
            regionBroker = new ManagedRegionBroker(this, getManagementContext(), getBrokerObjectName(),
                    getTaskRunnerFactory(), getConsumerSystemUsage(), destinationFactory, destinationInterceptor,getScheduler(),getExecutor());
        } else {
            regionBroker = new RegionBroker(this, getTaskRunnerFactory(), getConsumerSystemUsage(), destinationFactory,
                    destinationInterceptor,getScheduler(),getExecutor());
        }
        destinationFactory.setRegionBroker(regionBroker);
        regionBroker.setKeepDurableSubsActive(keepDurableSubsActive);
        regionBroker.setBrokerName(getBrokerName());
        regionBroker.getDestinationStatistics().setEnabled(enableStatistics);
        if (brokerId != null) {
            regionBroker.setBrokerId(brokerId);
        }
        return regionBroker;
    }

    /**
     * Create the default destination interceptor
     */
    protected DestinationInterceptor[] createDefaultDestinationInterceptor() {
        List<DestinationInterceptor> answer = new ArrayList<DestinationInterceptor>();
        if (isUseVirtualTopics()) {
            VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
            VirtualTopic virtualTopic = new VirtualTopic();
            virtualTopic.setName("VirtualTopic.>");
            VirtualDestination[] virtualDestinations = { virtualTopic };
            interceptor.setVirtualDestinations(virtualDestinations);
            answer.add(interceptor);
        }
        if (isUseMirroredQueues()) {
            MirroredQueue interceptor = new MirroredQueue();
            answer.add(interceptor);
        }
        DestinationInterceptor[] array = new DestinationInterceptor[answer.size()];
        answer.toArray(array);
        return array;
    }

    /**
     * Strategy method to add interceptors to the broker
     * 
     * @throws IOException
     */
    protected Broker addInterceptors(Broker broker) throws Exception {
        if (isSchedulerSupport()) {
            SchedulerBroker sb = new SchedulerBroker(broker, getSchedulerDirectoryFile());
            if (isUseJmx()) {
                JobSchedulerViewMBean view = new JobSchedulerView(sb.getJobScheduler());
                try {
                    ObjectName objectName = new ObjectName(getManagementContext().getJmxDomainName() + ":"
                            + "BrokerName=" + JMXSupport.encodeObjectNamePart(getBrokerName()) + ","
                            + "Type=jobScheduler," + "jobSchedulerName=JMS");

                    AnnotatedMBean.registerMBean(getManagementContext(), view, objectName);
                    this.adminView.setJMSJobScheduler(objectName);
                } catch (Throwable e) {
                    throw IOExceptionSupport.create("JobScheduler could not be registered in JMX: "
                            + e.getMessage(), e);
                }

            }
            broker = sb;
        }
        if (isAdvisorySupport()) {
            broker = new AdvisoryBroker(broker);
        }
        broker = new CompositeDestinationBroker(broker);
        broker = new TransactionBroker(broker, getPersistenceAdapter().createTransactionStore());
        if (isPopulateJMSXUserID()) {
            broker = new UserIDBroker(broker);
        }
        if (isMonitorConnectionSplits()) {
            broker = new ConnectionSplitBroker(broker);
        }
        if (plugins != null) {
            for (int i = 0; i < plugins.length; i++) {
                BrokerPlugin plugin = plugins[i];
                broker = plugin.installPlugin(broker);
            }
        }
        return broker;
    }

    protected PersistenceAdapter createPersistenceAdapter() throws IOException {
        if (isPersistent()) {
            PersistenceAdapterFactory fac = getPersistenceFactory();
            if (fac != null) {
                return fac.createPersistenceAdapter();
            }else {
                KahaDBPersistenceAdapter adaptor = new KahaDBPersistenceAdapter();
                File dir = new File(getBrokerDataDirectory(),"KahaDB");
                adaptor.setDirectory(dir);
                return adaptor;
            }
        } else {
            return new MemoryPersistenceAdapter();
        }
    }

    protected ObjectName createBrokerObjectName() throws IOException {
        try {
            return new ObjectName(getManagementContext().getJmxDomainName() + ":" + "BrokerName="
                    + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=Broker");
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Invalid JMX broker name: " + brokerName, e);
        }
    }

    protected TransportConnector createTransportConnector(URI brokerURI) throws Exception {
        TransportServer transport = TransportFactory.bind(this, brokerURI);
        return new TransportConnector(transport);
    }

    /**
     * Extracts the port from the options
     */
    protected Object getPort(Map options) {
        Object port = options.get("port");
        if (port == null) {
            port = DEFAULT_PORT;
            LOG.warn("No port specified so defaulting to: " + port);
        }
        return port;
    }

    protected void addShutdownHook() {
        if (useShutdownHook) {
            shutdownHook = new Thread("ActiveMQ ShutdownHook") {
                @Override
                public void run() {
                    containerShutdown();
                }
            };
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    protected void removeShutdownHook() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (Exception e) {
                LOG.debug("Caught exception, must be shutting down: " + e);
            }
        }
    }

    /**
     * Sets hooks to be executed when broker shut down
     * 
     * @org.apache.xbean.Property
     */
    public void setShutdownHooks(List<Runnable> hooks) throws Exception {
        for (Runnable hook : hooks) {
            addShutdownHook(hook);
        }
    }
    
    /**
     * Causes a clean shutdown of the container when the VM is being shut down
     */
    protected void containerShutdown() {
        try {
            stop();
        } catch (IOException e) {
            Throwable linkedException = e.getCause();
            if (linkedException != null) {
                logError("Failed to shut down: " + e + ". Reason: " + linkedException, linkedException);
            } else {
                logError("Failed to shut down: " + e, e);
            }
            if (!useLoggingForShutdownErrors) {
                e.printStackTrace(System.err);
            }
        } catch (Exception e) {
            logError("Failed to shut down: " + e, e);
        }
    }

    protected void logError(String message, Throwable e) {
        if (useLoggingForShutdownErrors) {
            LOG.error("Failed to shut down: " + e);
        } else {
            System.err.println("Failed to shut down: " + e);
        }
    }

    /**
     * Starts any configured destinations on startup
     */
    protected void startDestinations() throws Exception {
        if (destinations != null) {
            ConnectionContext adminConnectionContext = getAdminConnectionContext();
            for (int i = 0; i < destinations.length; i++) {
                ActiveMQDestination destination = destinations[i];
                getBroker().addDestination(adminConnectionContext, destination,true);
            }
        }
    }

    /**
     * Returns the broker's administration connection context used for
     * configuring the broker at startup
     */
    public ConnectionContext getAdminConnectionContext() throws Exception {
        return BrokerSupport.getConnectionContext(getBroker());
    }

    protected void waitForSlave() {
        try {
            if (!slaveStartSignal.await(waitForSlaveTimeout, TimeUnit.MILLISECONDS)) {
            	throw new IllegalStateException("Gave up waiting for slave to start after " + waitForSlaveTimeout + " milliseconds."); 
            }
        } catch (InterruptedException e) {
            LOG.error("Exception waiting for slave:" + e);
        }
    }

    protected void slaveConnectionEstablished() {
        slaveStartSignal.countDown();
    }
    
    protected void startManagementContext() throws Exception {
        getManagementContext().start();
        adminView = new BrokerView(this, null);
        ObjectName objectName = getBrokerObjectName();
        AnnotatedMBean.registerMBean(getManagementContext(), adminView, objectName);
    }

    /**
     * Start all transport and network connections, proxies and bridges
     * 
     * @throws Exception
     */
    protected void startAllConnectors() throws Exception {
        if (!isSlave()) {
            Set<ActiveMQDestination> durableDestinations = getBroker().getDurableDestinations();
            List<TransportConnector> al = new ArrayList<TransportConnector>();
            for (Iterator<TransportConnector> iter = getTransportConnectors().iterator(); iter.hasNext();) {
                TransportConnector connector = iter.next();
                connector.setBrokerService(this);
                al.add(startTransportConnector(connector));
            }
            if (al.size() > 0) {
                // let's clear the transportConnectors list and replace it with
                // the started transportConnector instances
                this.transportConnectors.clear();
                setTransportConnectors(al);
            }
            URI uri = getVmConnectorURI();
            Map<String, String> map = new HashMap<String, String>(URISupport.parseParameters(uri));
            map.put("network", "true");
            map.put("async", "false");
            uri = URISupport.createURIWithQuery(uri, URISupport.createQueryString(map));
            if (isWaitForSlave()) {
                waitForSlave();
            }
            if (!stopped.get()) {
                for (Iterator<NetworkConnector> iter = getNetworkConnectors().iterator(); iter.hasNext();) {
                    NetworkConnector connector = iter.next();
                    connector.setLocalUri(uri);
                    connector.setBrokerName(getBrokerName());
                    connector.setDurableDestinations(durableDestinations);
                    if (getDefaultSocketURIString() != null) {
                        connector.setBrokerURL(getDefaultSocketURIString());
                    }
                    connector.start();
                }
                for (Iterator<ProxyConnector> iter = getProxyConnectors().iterator(); iter.hasNext();) {
                    ProxyConnector connector = iter.next();
                    connector.start();
                }
                for (Iterator<JmsConnector> iter = jmsConnectors.iterator(); iter.hasNext();) {
                    JmsConnector connector = iter.next();
                    connector.start();
                }
                for (Service service : services) {
                    configureService(service);
                    service.start();
                }
            }
        }
    }

    protected TransportConnector startTransportConnector(TransportConnector connector) throws Exception {
        connector.setTaskRunnerFactory(getTaskRunnerFactory());
        MessageAuthorizationPolicy policy = getMessageAuthorizationPolicy();
        if (policy != null) {
            connector.setMessageAuthorizationPolicy(policy);
        }
        if (isUseJmx()) {
            connector = registerConnectorMBean(connector);
        }
        connector.getStatistics().setEnabled(enableStatistics);
        connector.start();
        return connector;
    }

    /**
     * Perform any custom dependency injection
     */
    protected void configureServices(Object[] services) {
        for (Object service : services) {
            configureService(service);
        }
    }

    /**
     * Perform any custom dependency injection
     */
    protected void configureService(Object service) {
        if (service instanceof BrokerServiceAware) {
            BrokerServiceAware serviceAware = (BrokerServiceAware) service;
            serviceAware.setBrokerService(this);
        }
        if (masterConnector == null) {
            if (service instanceof MasterConnector) {
                masterConnector = (MasterConnector) service;
                supportFailOver = true;
            }
        }
    }
    
    public void handleIOException(IOException exception) {
        if (ioExceptionHandler != null) {
            ioExceptionHandler.handle(exception);
         } else {
            LOG.info("Ignoring IO exception, " + exception, exception);
         }
    }

    /**
     * Starts all destiantions in persistence store. This includes all inactive
     * destinations
     */
    protected void startDestinationsInPersistenceStore(Broker broker) throws Exception {
        Set destinations = destinationFactory.getDestinations();
        if (destinations != null) {
            Iterator iter = destinations.iterator();
            ConnectionContext adminConnectionContext = broker.getAdminConnectionContext();
            if (adminConnectionContext == null) {
                ConnectionContext context = new ConnectionContext();
                context.setBroker(broker);
                adminConnectionContext = context;
                broker.setAdminConnectionContext(adminConnectionContext);
            }
            while (iter.hasNext()) {
                ActiveMQDestination destination = (ActiveMQDestination) iter.next();
                broker.addDestination(adminConnectionContext, destination,false);
            }
        }
    }
    
    protected synchronized ThreadPoolExecutor getExecutor() {
        if (this.executor == null) {
        this.executor = new ThreadPoolExecutor(1, 10, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "Usage Async Task");
                thread.setDaemon(true);
                return thread;
            }
        });
        }
        return this.executor;
    }
    
    protected synchronized Scheduler getScheduler() {
        if (this.scheduler==null) {
            this.scheduler = new Scheduler("ActiveMQ Broker["+getBrokerName()+"] Scheduler");
            try {
                this.scheduler.start();
            } catch (Exception e) {
               LOG.error("Failed to start Scheduler ",e);
            }
        }
        return this.scheduler;
    }

    public Broker getRegionBroker() {
        return regionBroker;
    }

    public void setRegionBroker(Broker regionBroker) {
        this.regionBroker = regionBroker;
    }

    public void addShutdownHook(Runnable hook) {
        synchronized (shutdownHooks) {
            shutdownHooks.add(hook);
        }
    }

    public void removeShutdownHook(Runnable hook) {
        synchronized (shutdownHooks) {
            shutdownHooks.remove(hook);
        }
    }

    public boolean isSystemExitOnShutdown() {
        return systemExitOnShutdown;
    }

    public void setSystemExitOnShutdown(boolean systemExitOnShutdown) {
        this.systemExitOnShutdown = systemExitOnShutdown;
    }

    public int getSystemExitOnShutdownExitCode() {
        return systemExitOnShutdownExitCode;
    }

    public void setSystemExitOnShutdownExitCode(int systemExitOnShutdownExitCode) {
        this.systemExitOnShutdownExitCode = systemExitOnShutdownExitCode;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    public boolean isShutdownOnSlaveFailure() {
        return shutdownOnSlaveFailure;
    }

    public void setShutdownOnSlaveFailure(boolean shutdownOnSlaveFailure) {
        this.shutdownOnSlaveFailure = shutdownOnSlaveFailure;
    }

    public boolean isWaitForSlave() {
        return waitForSlave;
    }

    public void setWaitForSlave(boolean waitForSlave) {
        this.waitForSlave = waitForSlave;
    }
  
    public long getWaitForSlaveTimeout() {
        return this.waitForSlaveTimeout;
    }
    
    public void setWaitForSlaveTimeout(long waitForSlaveTimeout) {
        this.waitForSlaveTimeout = waitForSlaveTimeout;
    }
    
    public CountDownLatch getSlaveStartSignal() {
        return slaveStartSignal;
    }

    /**
     * Get the passiveSlave
     * @return the passiveSlave
     */
    public boolean isPassiveSlave() {
        return this.passiveSlave;
    }

    /**
     * Set the passiveSlave
     * @param passiveSlave the passiveSlave to set
     */
    public void setPassiveSlave(boolean passiveSlave) {
        this.passiveSlave = passiveSlave;
    }
    
    public void setIoExceptionHandler(IOExceptionHandler ioExceptionHandler) {
        ioExceptionHandler.setBrokerService(this);
        this.ioExceptionHandler = ioExceptionHandler;
    }

    /**
     * @return the schedulerSupport
     */
    public boolean isSchedulerSupport() {
        return this.schedulerSupport;
    }

    /**
     * @param schedulerSupport the schedulerSupport to set
     */
    public void setSchedulerSupport(boolean schedulerSupport) {
        this.schedulerSupport = schedulerSupport;
    }

    /**
     * @return the schedulerDirectory
     */
    public File getSchedulerDirectoryFile() {
        if (this.schedulerDirectoryFile == null) {
            this.schedulerDirectoryFile = new File(getBrokerDataDirectory(), "scheduler");
        }
        return schedulerDirectoryFile;
    }

    /**
     * @param schedulerDirectory the schedulerDirectory to set
     */
    public void setSchedulerDirectoryFile(File schedulerDirectory) {
        this.schedulerDirectoryFile = schedulerDirectory;
    }
    
    public void setSchedulerDirectory(String schedulerDirectory) {
        setSchedulerDirectoryFile(new File(schedulerDirectory));
    }

    public int getSchedulePeriodForDestinationPurge() {
        return this.schedulePeriodForDestinationPurge;
    }

    public void setSchedulePeriodForDestinationPurge(int schedulePeriodForDestinationPurge) {
        this.schedulePeriodForDestinationPurge = schedulePeriodForDestinationPurge;
    }

    public BrokerContext getBrokerContext() {
        return brokerContext;
    }

    public void setBrokerContext(BrokerContext brokerContext) {
        this.brokerContext = brokerContext;
    }

    public void setBrokerId(String brokerId) {
        this.brokerId = new BrokerId(brokerId);
    }
}

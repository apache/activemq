/**
 *
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
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.ActiveMQConnectionMetaData;
import org.apache.activemq.Service;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.broker.ft.MasterConnector;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.ConnectorView;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.FTConnectorView;
import org.apache.activemq.broker.jmx.JmsConnectorView;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.NetworkConnectorView;
import org.apache.activemq.broker.jmx.NetworkConnectorViewMBean;
import org.apache.activemq.broker.jmx.ProxyConnectorView;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.DestinationFactory;
import org.apache.activemq.broker.region.DestinationFactoryImpl;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreFactory;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.network.ConnectionFilter;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.network.jms.JmsConnector;
import org.apache.activemq.proxy.ProxyConnector;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterFactory;
import org.apache.activemq.store.amq.AMQPersistenceAdapterFactory;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.vm.VMTransportFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.JMXSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.IOHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manages the lifecycle of an ActiveMQ Broker. A BrokerService consists of a number of transport
 * connectors, network connectors and a bunch of properties which can be used to
 * configure the broker as its lazily created.
 *
 * @version $Revision: 1.1 $
 */
public class BrokerService implements Service {

   

    private static final Log log = LogFactory.getLog(BrokerService.class);
    private static final long serialVersionUID = 7353129142305630237L;
    public static final String DEFAULT_PORT = "61616";
    static final String DEFAULT_BROKER_NAME = "localhost";
    public static final String LOCAL_HOST_NAME;

    private boolean useJmx = true;
    private boolean enableStatistics = true;
    private boolean persistent = true;
    private boolean populateJMSXUserID = false;
    private boolean useShutdownHook = true;
    private boolean useLoggingForShutdownErrors = false;
    private boolean shutdownOnMasterFailure = false;
    private String brokerName = DEFAULT_BROKER_NAME;
    private File dataDirectoryFile;
    private File tmpDataDirectory;
    private Broker broker;
    private BrokerView adminView;
    private ManagementContext managementContext;
    private ObjectName brokerObjectName;
    private TaskRunnerFactory taskRunnerFactory;
    private TaskRunnerFactory persistenceTaskRunnerFactory;
    private UsageManager usageManager;
    private UsageManager producerUsageManager;
    private UsageManager consumerUsageManager;
    private PersistenceAdapter persistenceAdapter;
    private PersistenceAdapterFactory persistenceFactory;
    private DestinationFactory destinationFactory;
    private MessageAuthorizationPolicy messageAuthorizationPolicy;
    private List transportConnectors = new CopyOnWriteArrayList();
    private List networkConnectors = new CopyOnWriteArrayList();
    private List proxyConnectors = new CopyOnWriteArrayList();
    private List registeredMBeanNames = new CopyOnWriteArrayList();
    private List jmsConnectors = new CopyOnWriteArrayList();
    private Service[] services;
    private MasterConnector masterConnector;
    private String masterConnectorURI;
    private transient Thread shutdownHook;
    private String[] transportConnectorURIs;
    private String[] networkConnectorURIs;
    private JmsConnector[] jmsBridgeConnectors; //these are Jms to Jms bridges to other jms messaging systems
    private boolean deleteAllMessagesOnStartup;
    private boolean advisorySupport = true;
    private URI vmConnectorURI;
    private PolicyMap destinationPolicy;
    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean stopped = new AtomicBoolean(false);
    private BrokerPlugin[] plugins;
    private boolean keepDurableSubsActive=true;
    private boolean useVirtualTopics=true;
    private BrokerId brokerId;
    private DestinationInterceptor[] destinationInterceptors;
    private ActiveMQDestination[] destinations;
    private Store tempDataStore;
    private int persistenceThreadPriority = Thread.MAX_PRIORITY;
    private boolean useLocalHostBrokerName = false;
    private CountDownLatch stoppedLatch = new CountDownLatch(1);
    private boolean supportFailOver = false;
    private boolean clustered = false;

    static{
        String localHostName = "localhost";
        try{
            localHostName=java.net.InetAddress.getLocalHost().getHostName();
        }catch(UnknownHostException e){
            log.error("Failed to resolve localhost");
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
        return addConnector(createTransportConnector(getBroker(), bindAddress));
    }

    /**
     * Adds a new transport connector for the given TransportServer transport
     *
     * @return the newly created and added transport connector
     * @throws Exception
     */
    public TransportConnector addConnector(TransportServer transport) throws Exception {
        return addConnector(new TransportConnector(getBroker(), transport));
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
        if( rc ) {
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
    public NetworkConnector addNetworkConnector(URI discoveryAddress) throws Exception{
        NetworkConnector connector=new DiscoveryNetworkConnector(discoveryAddress);
        return addNetworkConnector(connector);
    }

    /**
     * Adds a new proxy connector using the given bind address
     *
     * @return the newly created and added network connector
     * @throws Exception
     */
    public ProxyConnector addProxyConnector(URI bindAddress) throws Exception{
        ProxyConnector connector=new ProxyConnector();
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
        HashMap map = new HashMap(URISupport.parseParamters(uri));
        map.put("network", "true");
        map.put("async","false");
        uri = URISupport.createURIWithQuery(uri, URISupport.createQueryString(map));
        connector.setLocalUri(uri);
        
        // Set a connection filter so that the connector does not establish loop back connections.
        connector.setConnectionFilter(new ConnectionFilter() {
            public boolean connectTo(URI location) {
                List transportConnectors = getTransportConnectors();
                for (Iterator iter = transportConnectors.iterator(); iter.hasNext();) {
                    try {
                        TransportConnector tc = (TransportConnector) iter.next();
                        if( location.equals(tc.getConnectUri()) ) {
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
     * Removes the given network connector without stopping it.
     * The caller should call {@link NetworkConnector#stop()} to close the connector
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
    
    public JmsConnector addJmsConnector(JmsConnector connector) throws Exception{
        connector.setBrokerService(this);
        jmsConnectors.add(connector);
        if (isUseJmx()) {
            registerJmsConnectorMBean(connector);
        }
        return connector;
    }
    
    public JmsConnector removeJmsConnector(JmsConnector connector){
        if (jmsConnectors.remove(connector)){
            return connector;
        }
        return null;
    }
    
    /**
     * @return Returns the masterConnectorURI.
     */
    public String getMasterConnectorURI(){
        return masterConnectorURI;
    }

    /**
     * @param masterConnectorURI The masterConnectorURI to set.
     */
    public void setMasterConnectorURI(String masterConnectorURI){
        this.masterConnectorURI=masterConnectorURI;
    }

    /**
     * @return true if this Broker is a slave to a Master
     */
    public synchronized boolean isSlave(){
        return masterConnector != null && masterConnector.isSlave();
    }
    
    public void masterFailed(){
        if (shutdownOnMasterFailure){
            log.fatal("The Master has failed ... shutting down");
            try {
            stop();
            }catch(Exception e){
                log.error("Failed to stop for master failure",e);
            }
        }else {
            log.warn("Master Failed - starting all connectors");
            try{
                startAllConnectors();
            }catch(Exception e){
               log.error("Failed to startAllConnectors");
            }
        }
    }
    
    public boolean isStarted() {
        return started.get();
    }
    
    // Service interface
    // -------------------------------------------------------------------------
    public void start() throws Exception {
        if (! started.compareAndSet(false, true)) {
            // lets just ignore redundant start() calls
            // as its way too easy to not be completely sure if start() has been 
            // called or not with the gazillion of different configuration mechanisms
            
            //throw new IllegalStateException("Allready started.");
            return;
        }
        
        try {
            processHelperProperties();

            BrokerRegistry.getInstance().bind(getBrokerName(), this);

            startDestinations();
            
            addShutdownHook();
            log.info("Using Persistence Adapter: " + getPersistenceAdapter());
            if (deleteAllMessagesOnStartup) {
                deleteAllMessages();
            }

            if (isUseJmx()) {
                getManagementContext().start();
            }

            getBroker().start();

            /*
            if(isUseJmx()){
                // yes - this is order dependent!
                // register all destination in persistence store including inactive destinations as mbeans
                this.startDestinationsInPersistenceStore(broker);
            }
            */
            startAllConnectors();
            
            if (isUseJmx() && masterConnector != null) {
                registerFTConnectorMBean(masterConnector);
            }
     
            brokerId = broker.getBrokerId();
            log.info("ActiveMQ JMS Message Broker (" + getBrokerName()+", "+brokerId+") started");
            getBroker().brokerServiceStarted();
        }
        catch (Exception e) {
            log.error("Failed to start ActiveMQ JMS Message Broker. Reason: " + e, e);
            throw e;
        }
    }

    
    public void stop() throws Exception{
        if(!started.compareAndSet(true,false)){
            return;
        }
        log.info("ActiveMQ Message Broker ("+getBrokerName()+", "+brokerId+") is shutting down");
        removeShutdownHook();
        ServiceStopper stopper=new ServiceStopper();
        if(services!=null){
            for(int i=0;i<services.length;i++){
                Service service=services[i];
                stopper.stop(service);
            }
        }
        stopAllConnectors(stopper);
        stopper.stop(persistenceAdapter);
        if(broker!=null){
            stopper.stop(broker);
        }
        if(tempDataStore!=null){
            tempDataStore.close();
        }
        if(isUseJmx()){
            MBeanServer mbeanServer=getManagementContext().getMBeanServer();
            if(mbeanServer!=null){
                for(Iterator iter=registeredMBeanNames.iterator();iter.hasNext();){
                    ObjectName name=(ObjectName)iter.next();
                    try{
                        mbeanServer.unregisterMBean(name);
                    }catch(Exception e){
                        stopper.onException(mbeanServer,e);
                    }
                }
            }
            stopper.stop(getManagementContext());
        }
        // remove any VMTransports connected
        // this has to be done after services are stopped,
        // to avoid timimg issue with discovery (spinning up a new instance)
        BrokerRegistry.getInstance().unbind(getBrokerName());
        VMTransportFactory.stopped(getBrokerName());
        stopped.set(true);
        stoppedLatch.countDown();
        log.info("ActiveMQ JMS Message Broker ("+getBrokerName()+", "+brokerId+") stopped");
        stopper.throwFirstException();
    }

    /**
     * A helper method to block the caller thread until the broker has been stopped
     */
    public void waitUntilStopped() {
        while (!stopped.get()) {
            try {
                stoppedLatch.await();
            }
            catch (InterruptedException e) {
                // ignore
            }
        }
    }


    // Properties
    // -------------------------------------------------------------------------
    
    /**
     * Returns the message broker
     */
    public Broker getBroker() throws Exception {
        if (broker == null) {
            log.info("ActiveMQ " + ActiveMQConnectionMetaData.PROVIDER_VERSION + " JMS Message Broker ("
                    + getBrokerName() + ") is starting");
            log.info("For help or more information please see: http://activemq.apache.org/");
            broker = createBroker();
        }
        return broker;
    }

    
    /**
     * Returns the administration view of the broker; used to create and destroy resources such as queues and topics.
     * 
     * Note this method returns null if JMX is disabled.
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
     * @param brokerName 
     */
    public void setBrokerName(String brokerName) {
        if (brokerName == null) {
            throw new NullPointerException("The broker name cannot be null");
        }
        String str = brokerName.replaceAll("[^a-zA-Z0-9\\.\\_\\-\\:]", "_");
        if (!str.equals(brokerName)) {
            log.error("Broker Name: " + brokerName + " contained illegal characters - replaced with " + str);
        }
        this.brokerName = str.trim();
        
    }

    public PersistenceAdapterFactory getPersistenceFactory() {
        if (persistenceFactory == null) {
            persistenceFactory = createPersistenceFactory();
        }
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
    public File getTmpDataDirectory(){
        if (tmpDataDirectory == null) {
            tmpDataDirectory = new File(getBrokerDataDirectory(), "tmp_storage");
        }
        return tmpDataDirectory;
    }

    /**
     * @param tmpDataDirectory the tmpDataDirectory to set
     */
    public void setTmpDataDirectory(File tmpDataDirectory){
        this.tmpDataDirectory=tmpDataDirectory;
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

    public UsageManager getMemoryManager() {
        if (usageManager == null) {
            usageManager = new UsageManager("Main");
            usageManager.setLimit(1024 * 1024 * 64); // Default to 64 Meg
            // limit
        }
        return usageManager;
    }
       

    public void setMemoryManager(UsageManager memoryManager) {
        this.usageManager = memoryManager;
    }
    
    /**
     * @return the consumerUsageManager
     */
    public UsageManager getConsumerUsageManager(){
        if (consumerUsageManager==null) {
            consumerUsageManager = new UsageManager(getMemoryManager(),"Consumer",0.5f);
        }
        return consumerUsageManager;
    }

    
    /**
     * @param consumerUsageManager the consumerUsageManager to set
     */
    public void setConsumerUsageManager(UsageManager consumerUsageManager){
        this.consumerUsageManager=consumerUsageManager;
    }

    
    /**
     * @return the producerUsageManager
     */
    public UsageManager getProducerUsageManager(){
        if (producerUsageManager==null) {
            producerUsageManager = new UsageManager(getMemoryManager(),"Producer",0.45f);
        }
        return producerUsageManager;
    }
    
    /**
     * @param producerUsageManager the producerUsageManager to set
     */
    public void setProducerUsageManager(UsageManager producerUsageManager){
        this.producerUsageManager=producerUsageManager;
    }    

   
    public PersistenceAdapter getPersistenceAdapter() throws IOException {
        if (persistenceAdapter == null) {
            persistenceAdapter = createPersistenceAdapter();
            configureService(persistenceAdapter);
        }
        return persistenceAdapter;
    }

    /**
     * Sets the persistence adaptor implementation to use for this broker
     */
    public void setPersistenceAdapter(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    public TaskRunnerFactory getTaskRunnerFactory() {
        if (taskRunnerFactory == null) {
            taskRunnerFactory = new TaskRunnerFactory();
        }
        return taskRunnerFactory;
    }

    public void setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory) {
        this.taskRunnerFactory = taskRunnerFactory;
    }
    
    
    public TaskRunnerFactory getPersistenceTaskRunnerFactory(){
        if (taskRunnerFactory == null) {
            persistenceTaskRunnerFactory = new TaskRunnerFactory("Persistence Adaptor Task", persistenceThreadPriority, true, 1000);
        }
        return persistenceTaskRunnerFactory;
    }

   
    public void setPersistenceTaskRunnerFactory(TaskRunnerFactory persistenceTaskRunnerFactory){
        this.persistenceTaskRunnerFactory=persistenceTaskRunnerFactory;
    }

    public boolean isUseJmx() {
        return useJmx;
    }
    
    public boolean isEnableStatistics() {
        return enableStatistics;
    }    

    /**
     * Sets whether or not the Broker's services enable statistics or
     * not.
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

    public String[] getNetworkConnectorURIs() {
        return networkConnectorURIs;
    }

    public void setNetworkConnectorURIs(String[] networkConnectorURIs) {
        this.networkConnectorURIs = networkConnectorURIs;
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
    public JmsConnector[] getJmsBridgeConnectors(){
        return jmsBridgeConnectors;
    }

    /**
     * @param jmsConnectors The jmsBridgeConnectors to set.
     */
    public void setJmsBridgeConnectors(JmsConnector[] jmsConnectors){
        this.jmsBridgeConnectors=jmsConnectors;
    }

    public Service[] getServices() {
        return services;
    }

    /**
     * Sets the services associated with this broker such as a {@link MasterConnector}
     */
    public void setServices(Service[] services) {
        this.services = services;
    }

    /**
     * Adds a new service so that it will be started as part of the broker lifecycle
     */
    public void addService(Service service) {
        if (services == null) {
            services = new Service[] { service };
        }
        else {
            int length = services.length;
            Service[] temp = new Service[length + 1];
            System.arraycopy(services, 1, temp, 1, length);
            temp[length] = service;
            services = temp;
        }
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
     * Allows the support of advisory messages to be disabled for performance reasons.
     */
    public void setAdvisorySupport(boolean advisorySupport) {
        this.advisorySupport = advisorySupport;
    }

    public List getTransportConnectors() {
        return new ArrayList(transportConnectors);
    }

    /**
     * Sets the transport connectors which this broker will listen on for new
     * clients
     * 
     * @org.apache.xbean.Property nestedType="org.apache.activemq.broker.TransportConnector"
     */
    public void setTransportConnectors(List transportConnectors) throws Exception {
        for (Iterator iter = transportConnectors.iterator(); iter.hasNext();) {
            TransportConnector connector = (TransportConnector) iter.next();
            addConnector(connector);
        }
    }

    public List getNetworkConnectors() {
        return new ArrayList(networkConnectors);
    }

    public List getProxyConnectors() {
        return new ArrayList(proxyConnectors);
    }

    /**
     * Sets the network connectors which this broker will use to connect to
     * other brokers in a federated network
     * 
     * @org.apache.xbean.Property nestedType="org.apache.activemq.network.NetworkConnector"
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
     * Sets a number of broker plugins to install such as for security authentication or authorization
     */
    public void setPlugins(BrokerPlugin[] plugins) {
        this.plugins = plugins;
    }
    
    public MessageAuthorizationPolicy getMessageAuthorizationPolicy() {
        return messageAuthorizationPolicy;
    }

    /**
     * Sets the policy used to decide if the current connection is authorized to consume
     * a given message
     */
    public void setMessageAuthorizationPolicy(MessageAuthorizationPolicy messageAuthorizationPolicy) {
        this.messageAuthorizationPolicy = messageAuthorizationPolicy;
    }

    /**
     * Delete all messages from the persistent store
     * @throws IOException
     */
    public void deleteAllMessages() throws IOException{
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
            }
            catch (URISyntaxException e) {
                log.error("Badly formed URI from " + getBrokerName(),e);
            }
        }
        return vmConnectorURI;
    }

    public void setVmConnectorURI(URI vmConnectorURI) {
        this.vmConnectorURI = vmConnectorURI;
    }

    /**
     * @return Returns the shutdownOnMasterFailure.
     */
    public boolean isShutdownOnMasterFailure(){
        return shutdownOnMasterFailure;
    }

    /**
     * @param shutdownOnMasterFailure The shutdownOnMasterFailure to set.
     */
    public void setShutdownOnMasterFailure(boolean shutdownOnMasterFailure){
        this.shutdownOnMasterFailure=shutdownOnMasterFailure;
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
     * Sets whether or not
     * <a href="http://activemq.apache.org/virtual-destinations.html">Virtual Topics</a>
     * should be supported by default if they have not been explicitly configured.
     */
    public void setUseVirtualTopics(boolean useVirtualTopics) {
        this.useVirtualTopics = useVirtualTopics;
    }
    
    public DestinationInterceptor[] getDestinationInterceptors() {
        return destinationInterceptors;
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
    public synchronized Store getTempDataStore(){
        if(tempDataStore==null){
            boolean result=true;
            boolean empty=true;
            try{
                File directory=getTmpDataDirectory();
                if(directory.exists()&&directory.isDirectory()){
                    File[] files=directory.listFiles();
                    if(files!=null&&files.length>0){
                        empty=false;
                        for(int i=0;i<files.length;i++){
                            File file=files[i];
                            if(!file.isDirectory()){
                                result&=file.delete();
                            }
                        }
                    }
                }
                if(!empty){
                    String str=result?"Successfully deleted":"Failed to delete";
                    log.info(str+" temporary storage");
                }
                tempDataStore=StoreFactory.open(getTmpDataDirectory().getPath(),"rw");
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }
        return tempDataStore;
    }

    /**
     * @param tempDataStore the tempDataStore to set
     */
    public void setTempDataStore(Store tempDataStore){
        this.tempDataStore=tempDataStore;
    }   
    
    public int getPersistenceThreadPriority(){
        return persistenceThreadPriority;
    }

    public void setPersistenceThreadPriority(int persistenceThreadPriority){
        this.persistenceThreadPriority=persistenceThreadPriority;
    }
        
    /**
     * @return the useLocalHostBrokerName
     */
    public boolean isUseLocalHostBrokerName(){
        return this.useLocalHostBrokerName;
    }

    /**
     * @param useLocalHostBrokerName the useLocalHostBrokerName to set
     */
    public void setUseLocalHostBrokerName(boolean useLocalHostBrokerName){
        this.useLocalHostBrokerName=useLocalHostBrokerName;
        if(useLocalHostBrokerName&&!started.get()&&brokerName==null||brokerName==DEFAULT_BROKER_NAME){
            brokerName=LOCAL_HOST_NAME;
        }
    }
    
    /**
     * @return the supportFailOver
     */
    public boolean isSupportFailOver(){
        return this.supportFailOver;
    }

    /**
     * @param supportFailOver the supportFailOver to set
     */
    public void setSupportFailOver(boolean supportFailOver){
        this.supportFailOver=supportFailOver;
    }    
    
    /**
     * @return the clustered
     */
    public boolean isClustered(){
        return this.clustered;
    }

    /**
     * @param clustered the clustered to set
     */
    public void setClustered(boolean clustered){
        this.clustered=clustered;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    /**
     * Handles any lazy-creation helper properties which are added to make
     * things easier to configure inside environments such as Spring
     *
     * @throws Exception
     */
    protected void processHelperProperties() throws Exception {
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
               
        if (jmsBridgeConnectors != null){
            for (int i = 0; i < jmsBridgeConnectors.length; i++){
                addJmsConnector(jmsBridgeConnectors[i]);
            }
        }
        if (masterConnectorURI != null) {
            if (masterConnector != null) {
                throw new IllegalStateException("Cannot specify masterConnectorURI when a masterConnector is already registered via the services property");
            }
            else {
                addService(new MasterConnector(masterConnectorURI));
            }
        }
    }

    protected void stopAllConnectors(ServiceStopper stopper) {

		for (Iterator iter = getNetworkConnectors().iterator(); iter.hasNext();) {
            NetworkConnector connector = (NetworkConnector) iter.next();
            unregisterNetworkConnectorMBean(connector);
            stopper.stop(connector);
        }

        for (Iterator iter = getProxyConnectors().iterator(); iter.hasNext();) {
            ProxyConnector connector = (ProxyConnector) iter.next();
            stopper.stop(connector);
        }

        for (Iterator iter = jmsConnectors.iterator(); iter.hasNext();) {
            JmsConnector connector = (JmsConnector) iter.next();
            stopper.stop(connector);
        }

        for (Iterator iter = getTransportConnectors().iterator(); iter.hasNext();) {
            TransportConnector connector = (TransportConnector) iter.next();
            stopper.stop(connector);
        }
	}

    protected TransportConnector registerConnectorMBean(TransportConnector connector) throws IOException  {
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        if (mbeanServer != null) {

            try {
                ObjectName objectName = createConnectorObjectName(connector);
                connector = connector.asManagedConnector(getManagementContext().getMBeanServer(), objectName);
                ConnectorViewMBean view = new ConnectorView(connector);            
                mbeanServer.registerMBean(view, objectName);
                registeredMBeanNames.add(objectName);
                return connector;
            }
            catch (Throwable e) {
                throw IOExceptionSupport.create("Transport Connector could not be registered in JMX: " + e.getMessage(), e);
            }
        }
        return connector;
    }
    
    protected void unregisterConnectorMBean(TransportConnector connector) throws IOException {
        if (isUseJmx()) {
	        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
	        if (mbeanServer != null) {
	            try {
	                ObjectName objectName = createConnectorObjectName(connector);
	
	                if( registeredMBeanNames.remove(objectName) ) {
	                       mbeanServer.unregisterMBean(objectName);
	                }
	            }
	            catch (Throwable e) {
	                throw IOExceptionSupport.create("Transport Connector could not be registered in JMX: " + e.getMessage(), e);
	            }
	        }
        }
    }

	private ObjectName createConnectorObjectName(TransportConnector connector) throws MalformedObjectNameException {
		return new ObjectName(
		        managementContext.getJmxDomainName()+":"+
		        "BrokerName="+JMXSupport.encodeObjectNamePart(getBrokerName())+","+
		        "Type=Connector,"+
		        "ConnectorName="+JMXSupport.encodeObjectNamePart(connector.getName())
		        );
	}    

    protected void registerNetworkConnectorMBean(NetworkConnector connector) throws IOException {
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        if (mbeanServer != null) {
            NetworkConnectorViewMBean view = new NetworkConnectorView(connector);
            try {
                ObjectName objectName = createNetworkConnectorObjectName(connector);
                connector.setObjectName(objectName);
                mbeanServer.registerMBean(view, objectName);
                registeredMBeanNames.add(objectName);
            }
            catch (Throwable e) {
                throw IOExceptionSupport.create("Network Connector could not be registered in JMX: " + e.getMessage(), e);
            }
        }
    }

    protected ObjectName createNetworkConnectorObjectName(NetworkConnector connector) throws MalformedObjectNameException {
        return new ObjectName(managementContext.getJmxDomainName() + ":" + "BrokerName="
                + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=NetworkConnector," + "NetworkConnectorName="
                + JMXSupport.encodeObjectNamePart(connector.getName()));
    }

    protected void unregisterNetworkConnectorMBean(NetworkConnector connector) {
        if (isUseJmx()) {
            MBeanServer mbeanServer = getManagementContext().getMBeanServer();
            if (mbeanServer != null) {
                try {
                    ObjectName objectName = createNetworkConnectorObjectName(connector);
                    if (registeredMBeanNames.remove(objectName)) {
                        mbeanServer.unregisterMBean(objectName);
                    }
                }
                catch (Exception e) {
                    log.error("Network Connector could not be unregistered from JMX: " + e, e);
                }
            }
        }
    }
    
    protected void registerProxyConnectorMBean(ProxyConnector connector) throws IOException {
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        if (mbeanServer != null) {
            ProxyConnectorView view = new ProxyConnectorView(connector);
            try {
                ObjectName objectName = new ObjectName(managementContext.getJmxDomainName() + ":" + "BrokerName="
                        + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=ProxyConnector," + "ProxyConnectorName="
                        + JMXSupport.encodeObjectNamePart(connector.getName()));
                mbeanServer.registerMBean(view, objectName);
                registeredMBeanNames.add(objectName);
            }
            catch (Throwable e) {
                throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
            }
        }
    }

    protected void registerFTConnectorMBean(MasterConnector connector) throws IOException {
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        if (mbeanServer != null) {
            FTConnectorView view = new FTConnectorView(connector);
            try {
                ObjectName objectName = new ObjectName(managementContext.getJmxDomainName() + ":" + "BrokerName="
                        + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=MasterConnector");
                mbeanServer.registerMBean(view, objectName);
                registeredMBeanNames.add(objectName);
            }
            catch (Throwable e) {
                throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
            }
        }
    }

    protected void registerJmsConnectorMBean(JmsConnector connector) throws IOException {
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        if (mbeanServer != null) {
            JmsConnectorView view = new JmsConnectorView(connector);
            try {
                ObjectName objectName = new ObjectName(managementContext.getJmxDomainName() + ":" + "BrokerName="
                        + JMXSupport.encodeObjectNamePart(getBrokerName()) + "," + "Type=JmsConnector," + "JmsConnectorName="
                        + JMXSupport.encodeObjectNamePart(connector.getName()));
                mbeanServer.registerMBean(view, objectName);
                registeredMBeanNames.add(objectName);
            }
            catch (Throwable e) {
                throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
            }
        }
    }
    
    /**
     * Factory method to create a new broker
     *
     * @throws Exception
     *
     * @throws
     * @throws
     */
    protected Broker createBroker() throws Exception {
        Broker regionBroker = createRegionBroker();
        Broker broker = addInterceptors(regionBroker);

        // Add a filter that will stop access to the broker once stopped
        broker = new MutableBrokerFilter(broker) {
            public void stop() throws Exception {
                setNext(new ErrorBroker("Broker has been stopped: "+this) {
                    // Just ignore additional stop actions.
                    public void stop() throws Exception {
                    }
                });
                super.stop();
            }
        };
        
        RegionBroker rBroker = (RegionBroker) regionBroker;
        rBroker.getDestinationStatistics().setEnabled(enableStatistics);
        
        

        if (isUseJmx()) {
            ManagedRegionBroker managedBroker = (ManagedRegionBroker) regionBroker;
            managedBroker.setContextBroker(broker);
            adminView = new BrokerView(this, managedBroker);
            MBeanServer mbeanServer = getManagementContext().getMBeanServer();
            if (mbeanServer != null) {
                ObjectName objectName = getBrokerObjectName();
                mbeanServer.registerMBean(adminView, objectName);
                registeredMBeanNames.add(objectName);
            }
        }
        

        return broker;

    }

    /**
     * Factory method to create the core region broker onto which interceptors
     * are added
     *
     * @throws Exception
     */
    protected Broker createRegionBroker() throws Exception {
        // we must start the persistence adaptor before we can create the region
        // broker
        getPersistenceAdapter().setUsageManager(getProducerUsageManager());
        getPersistenceAdapter().setBrokerName(getBrokerName());
        if(this.deleteAllMessagesOnStartup){
            getPersistenceAdapter().deleteAllMessages();
        }
        getPersistenceAdapter().start();
        
        DestinationInterceptor destinationInterceptor = null;
        if (destinationInterceptors != null) {
            destinationInterceptor = new CompositeDestinationInterceptor(destinationInterceptors);
        }
        else {
            destinationInterceptor = createDefaultDestinationInterceptor();
        }
	RegionBroker regionBroker = null;
	if (destinationFactory == null) {
            destinationFactory = new DestinationFactoryImpl(getProducerUsageManager(), getTaskRunnerFactory(), getPersistenceAdapter());
        }
        if (isUseJmx()) {
            MBeanServer mbeanServer = getManagementContext().getMBeanServer();
            regionBroker = new ManagedRegionBroker(this, mbeanServer, getBrokerObjectName(), getTaskRunnerFactory(), getConsumerUsageManager(),
                    destinationFactory, destinationInterceptor);
        }
        else {
            regionBroker = new RegionBroker(this,getTaskRunnerFactory(), getConsumerUsageManager(), destinationFactory, destinationInterceptor);
        }
        destinationFactory.setRegionBroker(regionBroker);
        
        regionBroker.setKeepDurableSubsActive(keepDurableSubsActive);
		regionBroker.setBrokerName(getBrokerName());
		return regionBroker;
	}

    /**
     * Create the default destination interceptor
     */
    protected DestinationInterceptor createDefaultDestinationInterceptor() {
        if (! isUseVirtualTopics()) {
            return null;
        }
        VirtualDestinationInterceptor answer = new VirtualDestinationInterceptor();
        VirtualTopic virtualTopic = new VirtualTopic();
        virtualTopic.setName("VirtualTopic.>");
        VirtualDestination[] virtualDestinations = { virtualTopic };
        answer.setVirtualDestinations(virtualDestinations);
        return answer;
    }

    /**
     * Strategy method to add interceptors to the broker
     *
     * @throws IOException
     */
    protected Broker addInterceptors(Broker broker) throws Exception {
        broker = new TransactionBroker(broker, getPersistenceAdapter().createTransactionStore());
        if (isAdvisorySupport()) {
            broker = new AdvisoryBroker(broker);
        }
        broker = new CompositeDestinationBroker(broker);
        if (isPopulateJMSXUserID()) {
            broker = new UserIDBroker(broker);
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
            return getPersistenceFactory().createPersistenceAdapter();
        }
        else {
            return new MemoryPersistenceAdapter();
        }
    }

    protected AMQPersistenceAdapterFactory createPersistenceFactory() {
        AMQPersistenceAdapterFactory factory = new AMQPersistenceAdapterFactory();
        factory.setDataDirectory(getBrokerDataDirectory());
        factory.setTaskRunnerFactory(getPersistenceTaskRunnerFactory());
        factory.setBrokerName(getBrokerName());
        return factory;
    }

    protected ObjectName createBrokerObjectName() throws IOException {
        try {
            return new ObjectName(
            		getManagementContext().getJmxDomainName()+":"+
            		"BrokerName="+JMXSupport.encodeObjectNamePart(getBrokerName())+","+
            		"Type=Broker"
            		);
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create("Invalid JMX broker name: " + brokerName, e);
        }
    }

    protected TransportConnector createTransportConnector(Broker broker, URI brokerURI) throws Exception {
        TransportServer transport = TransportFactory.bind(getBrokerName(),brokerURI);
        return new TransportConnector(broker, transport);
    }

    /**
     * Extracts the port from the options
     */
    protected Object getPort(Map options) {
        Object port = options.get("port");
        if (port == null) {
            port = DEFAULT_PORT;
            log.warn("No port specified so defaulting to: " + port);
        }
        return port;
    }

    protected void addShutdownHook() {
        if (useShutdownHook) {
            shutdownHook = new Thread("ActiveMQ ShutdownHook") {
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
            }
            catch (Exception e) {
                log.debug("Caught exception, must be shutting down: " + e);
            }
        }
    }

    /**
     * Causes a clean shutdown of the container when the VM is being shut down
     */
    protected void containerShutdown() {
        try {
            stop();
        }
        catch (IOException e) {
            Throwable linkedException = e.getCause();
            if (linkedException != null) {
                logError("Failed to shut down: " + e + ". Reason: " + linkedException, linkedException);
            }
            else {
                logError("Failed to shut down: " + e, e);
            }
            if (!useLoggingForShutdownErrors) {
                e.printStackTrace(System.err);
            }
        }
        catch (Exception e) {
            logError("Failed to shut down: " + e, e);
        }
    }

    protected void logError(String message, Throwable e) {
        if (useLoggingForShutdownErrors) {
            log.error("Failed to shut down: " + e);
        }
        else {
            System.err.println("Failed to shut down: " + e);
        }
    }

    /**
     * Starts any configured destinations on startup
     *
     */
    protected void startDestinations() throws Exception {
        if (destinations != null) {
            ConnectionContext adminConnectionContext = getAdminConnectionContext();
            
            for (int i = 0; i < destinations.length; i++) {
                ActiveMQDestination destination = destinations[i];
                getBroker().addDestination(adminConnectionContext, destination);
            }
        }
    }
    
    /**
     * Returns the broker's administration connection context used for configuring the broker
     * at startup
     */
    public ConnectionContext getAdminConnectionContext() throws Exception {
        ConnectionContext adminConnectionContext = getBroker().getAdminConnectionContext();
        if (adminConnectionContext == null) {
            adminConnectionContext = createAdminConnectionContext();
            getBroker().setAdminConnectionContext(adminConnectionContext);
        }
        return adminConnectionContext;
    }
    
    /**
     * Factory method to create the new administration connection context object.
     * Note this method is here rather than inside a default broker implementation to
     * ensure that the broker reference inside it is the outer most interceptor
     */
    protected ConnectionContext createAdminConnectionContext() throws Exception {
        ConnectionContext context = new ConnectionContext();
        context.setBroker(getBroker());
        context.setSecurityContext(SecurityContext.BROKER_SECURITY_CONTEXT);
        return context;
    }


    
    /**
     * Start all transport and network connections, proxies and bridges
     * @throws Exception
     */
    protected void startAllConnectors() throws Exception{
        if (!isSlave()){
        	
        	ArrayList al = new ArrayList();

            for (Iterator iter = getTransportConnectors().iterator(); iter.hasNext();) {
                TransportConnector connector = (TransportConnector) iter.next();
                al.add(startTransportConnector(connector));
            }
 
            if (al.size()>0) {
            	//let's clear the transportConnectors list and replace it with the started transportConnector instances 
            	this.transportConnectors.clear();
            	setTransportConnectors(al);
            }
            URI uri = getVmConnectorURI();
            HashMap map = new HashMap(URISupport.parseParamters(uri));
            map.put("network", "true");
            map.put("async","false");
            uri = URISupport.createURIWithQuery(uri, URISupport.createQueryString(map));

            for (Iterator iter = getNetworkConnectors().iterator(); iter.hasNext();) {
                NetworkConnector connector = (NetworkConnector) iter.next();
                connector.setLocalUri(uri);
                connector.setBrokerName(getBrokerName());
                connector.setDurableDestinations(getBroker().getDurableDestinations());
                connector.start();
            }
            
            for (Iterator iter = getProxyConnectors().iterator(); iter.hasNext();) {
                ProxyConnector connector = (ProxyConnector) iter.next();
                connector.start();
            }
            
            for (Iterator iter = jmsConnectors.iterator(); iter.hasNext();) {
                JmsConnector connector = (JmsConnector) iter.next();
                connector.start();
            }
            
            if (services != null) {
                for (int i = 0; i < services.length; i++) {
                    Service service = services[i];
                    configureService(service);
                    service.start();
                }
            }
        }
    }

    protected TransportConnector startTransportConnector(TransportConnector connector) throws Exception {
        connector.setBroker(getBroker());
        connector.setBrokerName(getBrokerName());
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
    protected void configureService(Object service) {
        if (service instanceof BrokerServiceAware) {
            BrokerServiceAware serviceAware = (BrokerServiceAware) service;
            serviceAware.setBrokerService(this);
        }
        if (service instanceof MasterConnector) {
            masterConnector = (MasterConnector) service;
            supportFailOver=true;
        }
    }

   
    /**
     * Starts all destiantions in persistence store. This includes all inactive destinations
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
                broker.addDestination(adminConnectionContext, destination);
            }
        }
    }   
}

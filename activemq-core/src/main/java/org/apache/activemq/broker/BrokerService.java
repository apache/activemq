/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionMetaData;
import org.apache.activemq.Service;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.broker.ft.MasterConnector;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorView;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.FTConnectorView;
import org.apache.activemq.broker.jmx.JmsConnectorView;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.NetworkConnectorView;
import org.apache.activemq.broker.jmx.NetworkConnectorViewMBean;
import org.apache.activemq.broker.jmx.ProxyConnectorView;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.network.jms.JmsConnector;
import org.apache.activemq.proxy.ProxyConnector;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.store.DefaultPersistenceAdapterFactory;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.vm.VMTransportFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.JMXSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.URISupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a running broker service which consists of a number of transport
 * connectors, network connectors and a bunch of properties which can be used to
 * configure the broker as its lazily created.
 *
 * @version $Revision: 1.1 $
 */
public class BrokerService implements Service {
    public static final String DEFAULT_PORT = "61616";

    private static final Log log = LogFactory.getLog(BrokerService.class);

    private boolean useJmx = false;
    private boolean persistent = true;
    private boolean populateJMSXUserID = false;
    private boolean useShutdownHook = true;
    private boolean useLoggingForShutdownErrors = false;
    private boolean shutdownOnMasterFailure = false;
    private String brokerName = "localhost";
    private File dataDirectory;
    private Broker broker;
    private ManagementContext managementContext;
    private ObjectName brokerObjectName;
    private TaskRunnerFactory taskRunnerFactory;
    private UsageManager memoryManager;
    private PersistenceAdapter persistenceAdapter;
    private DefaultPersistenceAdapterFactory persistenceFactory;
    private MessageAuthorizationPolicy messageAuthorizationPolicy;
    private List transportConnectors = new CopyOnWriteArrayList();
    private List networkConnectors = new CopyOnWriteArrayList();
    private List proxyConnectors = new CopyOnWriteArrayList();
    private List registeredMBeanNames = new CopyOnWriteArrayList();
    private List jmsConnectors = new CopyOnWriteArrayList();
    private MasterConnector masterConnector;
    private Thread shutdownHook;
    private String[] transportConnectorURIs;
    private String[] networkConnectorURIs;
    private String[] proxyConnectorURIs;
    private String masterConnectorURI;
    private JmsConnector[] jmsBridgeConnectors; //these are Jms to Jms bridges to other jms messaging systems
    private boolean deleteAllMessagesOnStartup;
    private URI vmConnectorURI;
    private PolicyMap destinationPolicy;
    private AtomicBoolean started = new AtomicBoolean(false);
    private BrokerPlugin[] plugins;

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
        
        connector.setBroker(getBroker());
        connector.setBrokerName(getBrokerName());
        connector.setTaskRunnerFactory(getTaskRunnerFactory());
        MessageAuthorizationPolicy policy = getMessageAuthorizationPolicy();
        if (policy != null) {
            connector.setMessageAuthorizationPolicy(policy);
        }
        
        if (isUseJmx()) {
            connector = connector.asManagedConnector(getManagementContext().getMBeanServer(), getBrokerObjectName());
            registerConnectorMBean(connector);
        }        
        transportConnectors.add(connector);

        return connector;
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
        NetworkConnector connector=new NetworkConnector();
        // add the broker name to the parameters if not set
        connector.setUri(discoveryAddress);
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
        URI uri = getVmConnectorURI();
        HashMap map = new HashMap(URISupport.parseParamters(uri));
        map.put("network", "true");
        uri = URISupport.createURIWithQuery(uri, URISupport.createQueryString(map));
        connector.setLocalUri(uri);
        networkConnectors.add(connector);
        if (isUseJmx()) {
            registerNetworkConnectorMBean(connector);
        }
        return connector;
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
    
    public void initializeMasterConnector(URI remoteURI) throws Exception {
        if (masterConnector != null){
            throw new IllegalStateException("Can only be the Slave to one Master");
        }
        URI localURI = getVmConnectorURI();
        TransportConnector connector = null;
        if (!transportConnectors.isEmpty()){
            connector = (TransportConnector)transportConnectors.get(0);
        }
        masterConnector = new MasterConnector(this,connector);
        masterConnector.setLocalURI(localURI);
        masterConnector.setRemoteURI(remoteURI);
        
        if (isUseJmx()) {
            registerFTConnectorMBean(masterConnector);
        }
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
    public boolean isSlave(){
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
        
        processHelperProperties();

        BrokerRegistry.getInstance().bind(getBrokerName(), this);

        addShutdownHook();
        if (deleteAllMessagesOnStartup) {
            deleteAllMessages();
        }

        if (isUseJmx()) {
            getManagementContext().start();
        }

        getBroker().start();
        if (masterConnectorURI!=null){
            initializeMasterConnector(new URI(masterConnectorURI));
            if (masterConnector!=null){
                masterConnector.start();
            }
        }
        
        startAllConnectors();
        

        log.info("ActiveMQ JMS Message Broker (" + getBrokerName() + ") started");
    }

    public void stop() throws Exception {
        if (! started.compareAndSet(true, false)) {
            return;
        }
        log.info("ActiveMQ Message Broker (" + getBrokerName() + ") is shutting down");
        BrokerRegistry.getInstance().unbind(getBrokerName());
        
        removeShutdownHook();

        ServiceStopper stopper = new ServiceStopper();
        if (masterConnector != null){
            masterConnector.stop();
        }

        
        for (Iterator iter = getNetworkConnectors().iterator(); iter.hasNext();) {
            NetworkConnector connector = (NetworkConnector) iter.next();
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

        
        //remove any VMTransports connected
        VMTransportFactory.stopped(getBrokerName());



        stopper.stop(getPersistenceAdapter());

        if (broker != null) {
            stopper.stop(broker);
        }

        if (isUseJmx()) {
            MBeanServer mbeanServer = getManagementContext().getMBeanServer();
            for (Iterator iter = registeredMBeanNames.iterator(); iter.hasNext();) {
                ObjectName name = (ObjectName) iter.next();
                try {
                    mbeanServer.unregisterMBean(name);
                }
                catch (Exception e) {
                    stopper.onException(mbeanServer, e);
                }
            }
            stopper.stop(getManagementContext());
        }

        log.info("ActiveMQ JMS Message Broker (" + getBrokerName() + ") stopped: "+broker);

        stopper.throwFirstException();
    }

    // Properties
    // -------------------------------------------------------------------------
    public Broker getBroker() throws Exception {
        if (broker == null) {
            log.info("ActiveMQ " + ActiveMQConnectionMetaData.PROVIDER_VERSION + " JMS Message Broker ("
                    + getBrokerName() + ") is starting");
            log.info("For help or more information please see: http://www.logicblaze.com");
            broker = createBroker();
        }
        return broker;
    }

    public String getBrokerName() {
        return brokerName;
    }

    /**
     * Sets the name of this broker; which must be unique in the network
     */
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public DefaultPersistenceAdapterFactory getPersistenceFactory() {
        if (persistenceFactory == null) {
            persistenceFactory = createPersistenceFactory();
        }
        return persistenceFactory;
    }

    public File getDataDirectory() {
        if (dataDirectory == null) {
            dataDirectory = new File(new File("activemq-data"), getBrokerName()
                    .replaceAll("[^a-zA-Z0-9\\.\\_\\-]", "_"));
        }
        return dataDirectory;
    }

    /**
     * Sets the directory in which the data files will be stored by default for
     * the JDBC and Journal persistence adaptors.
     *
     * @param dataDirectory
     *            the directory to store data files
     */
    public void setDataDirectory(File dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    public void setPersistenceFactory(DefaultPersistenceAdapterFactory persistenceFactory) {
        this.persistenceFactory = persistenceFactory;
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
        if (memoryManager == null) {
            memoryManager = new UsageManager();
            memoryManager.setLimit(1024 * 1024 * 20); // Default to 20 Meg
            // limit
        }
        return memoryManager;
    }

    public void setMemoryManager(UsageManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    public PersistenceAdapter getPersistenceAdapter() throws IOException {
        if (persistenceAdapter == null) {
            persistenceAdapter = createPersistenceAdapter();
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

    public boolean isUseJmx() {
        return useJmx;
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
     * @param jmsBridgeConnectors The jmsBridgeConnectors to set.
     */
    public void setJmsBridgeConnectors(JmsConnector[] jmsConnectors){
        this.jmsBridgeConnectors=jmsConnectors;
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

    public List getTransportConnectors() {
        return new ArrayList(transportConnectors);
    }

    /**
     * Sets the transport connectors which this broker will listen on for new
     * clients
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
        if (proxyConnectorURIs != null) {
            for (int i = 0; i < proxyConnectorURIs.length; i++) {
                String uri = proxyConnectorURIs[i];
                addProxyConnector(uri);
            }
        }
        
        if (jmsBridgeConnectors != null){
            for (int i = 0; i < jmsBridgeConnectors.length; i++){
                addJmsConnector(jmsBridgeConnectors[i]);
            }
        }
        
    }

    protected void registerConnectorMBean(TransportConnector connector) throws IOException, URISyntaxException {
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        ConnectorViewMBean view = new ConnectorView(connector);
        Hashtable map = new Hashtable();
        map.put("Type", "Connector");
        map.put("BrokerName", JMXSupport.encodeObjectNamePart(getBrokerName()));
        map.put("ConnectorName", JMXSupport.encodeObjectNamePart(connector.getName()));
        try {
            ObjectName objectName = new ObjectName("org.apache.activemq", map);
            mbeanServer.registerMBean(view, objectName);
            registeredMBeanNames.add(objectName);
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
        }
    }

    protected void registerNetworkConnectorMBean(NetworkConnector connector) throws IOException {
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        NetworkConnectorViewMBean view = new NetworkConnectorView(connector);
        Hashtable map = new Hashtable();
        map.put("Type", "NetworkConnector");
        map.put("BrokerName", JMXSupport.encodeObjectNamePart(getBrokerName()));
        // map.put("ConnectorName",
        // JMXSupport.encodeObjectNamePart(connector.()));
        try {
            ObjectName objectName = new ObjectName("org.apache.activemq", map);
            mbeanServer.registerMBean(view, objectName);
            registeredMBeanNames.add(objectName);
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
        }
    }

    protected void registerProxyConnectorMBean(ProxyConnector connector) throws IOException {
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        ProxyConnectorView view = new ProxyConnectorView(connector);
        Hashtable map = new Hashtable();
        map.put("Type", "ProxyConnector");
        map.put("BrokerName", JMXSupport.encodeObjectNamePart(getBrokerName()));
        // map.put("ConnectorName",
        // JMXSupport.encodeObjectNamePart(connector.()));
        try {
            ObjectName objectName = new ObjectName("org.apache.activemq", map);
            mbeanServer.registerMBean(view, objectName);
            registeredMBeanNames.add(objectName);
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
        }
    }
    
    protected void registerFTConnectorMBean(MasterConnector connector) throws IOException {
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        FTConnectorView view = new FTConnectorView(connector);
        Hashtable map = new Hashtable();
        map.put("Type", "MasterConnector");
        map.put("BrokerName", JMXSupport.encodeObjectNamePart(getBrokerName()));
        // map.put("ConnectorName",
        // JMXSupport.encodeObjectNamePart(connector.()));
        try {
            ObjectName objectName = new ObjectName("org.apache.activemq", map);
            mbeanServer.registerMBean(view, objectName);
            registeredMBeanNames.add(objectName);
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
        }
    }
    
    protected void registerJmsConnectorMBean(JmsConnector connector) throws IOException {
        MBeanServer mbeanServer = getManagementContext().getMBeanServer();
        JmsConnectorView view = new JmsConnectorView(connector);
        Hashtable map = new Hashtable();
        map.put("Type", "JmsConnector");
        map.put("BrokerName", JMXSupport.encodeObjectNamePart(getBrokerName()));
        // map.put("ConnectorName",
        // JMXSupport.encodeObjectNamePart(connector.()));
        try {
            ObjectName objectName = new ObjectName("org.apache.activemq", map);
            mbeanServer.registerMBean(view, objectName);
            registeredMBeanNames.add(objectName);
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create("Broker could not be registered in JMX: " + e.getMessage(), e);
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
                super.stop();
                setNext(new ErrorBroker("Broker has been stopped: "+this) {
                    // Just ignore additional stop actions.
                    public void stop() throws Exception {
                    }
                });
            }
        };

        if (isUseJmx()) {
            ManagedRegionBroker managedBroker = (ManagedRegionBroker) regionBroker;
            managedBroker.setContextBroker(broker);
            BrokerViewMBean view = new BrokerView(managedBroker, getMemoryManager());
            MBeanServer mbeanServer = getManagementContext().getMBeanServer();
            ObjectName objectName = getBrokerObjectName();
            mbeanServer.registerMBean(view, objectName);
            registeredMBeanNames.add(objectName);
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
        getPersistenceAdapter().start();
		RegionBroker regionBroker = null;
        if (isUseJmx()) {
            MBeanServer mbeanServer = getManagementContext().getMBeanServer();
            regionBroker = new ManagedRegionBroker(this,mbeanServer, getBrokerObjectName(),
                    getTaskRunnerFactory(), getMemoryManager(), getPersistenceAdapter(), getDestinationPolicy());
        }
        else {
			regionBroker = new RegionBroker(this,getTaskRunnerFactory(), getMemoryManager(), getPersistenceAdapter(),
                    getDestinationPolicy());
        }
		regionBroker.setBrokerName(getBrokerName());
		return regionBroker;
	}

    /**
     * Strategy method to add interceptors to the broker
     *
     * @throws IOException
     */
    protected Broker addInterceptors(Broker broker) throws IOException {
        broker = new TransactionBroker(broker, getPersistenceAdapter().createTransactionStore());
        broker = new AdvisoryBroker(broker);
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

    protected DefaultPersistenceAdapterFactory createPersistenceFactory() {
        DefaultPersistenceAdapterFactory factory = new DefaultPersistenceAdapterFactory();
        factory.setMemManager(getMemoryManager());
        factory.setDataDirectory(getDataDirectory());
        factory.setTaskRunnerFactory(getTaskRunnerFactory());
        return factory;
    }

    protected ObjectName createBrokerObjectName() throws IOException {
        try {
            Hashtable map = new Hashtable();
            map.put("Type", "Broker");
            map.put("BrokerName", JMXSupport.encodeObjectNamePart(getBrokerName()));
            return new ObjectName(getManagementContext().getJmxDomainName(), map);
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
     * Start all transport and network connections, proxies and bridges
     * @throws Exception
     */
    protected void startAllConnectors() throws Exception{
        if (!isSlave()){
            for (Iterator iter = getTransportConnectors().iterator(); iter.hasNext();) {
                TransportConnector connector = (TransportConnector) iter.next();
                connector.start();
            }

            for (Iterator iter = getNetworkConnectors().iterator(); iter.hasNext();) {
                NetworkConnector connector = (NetworkConnector) iter.next();
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
            }
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
                vmConnectorURI = new URI("vm://" + getBrokerName());
            }
            catch (URISyntaxException e) {
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
}

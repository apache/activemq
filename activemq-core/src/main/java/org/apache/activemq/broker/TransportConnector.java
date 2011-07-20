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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import javax.management.ObjectName;
import org.apache.activemq.broker.jmx.ManagedTransportConnector;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.region.ConnectorStatistics;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.thread.DefaultThreadPools;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @org.apache.xbean.XBean
 * 
 */
public class TransportConnector implements Connector, BrokerServiceAware {

    final Logger LOG = LoggerFactory.getLogger(TransportConnector.class);

    protected CopyOnWriteArrayList<TransportConnection> connections = new CopyOnWriteArrayList<TransportConnection>();
    protected TransportStatusDetector statusDector;
    private BrokerService brokerService;
    private TransportServer server;
    private URI uri;
    private BrokerInfo brokerInfo = new BrokerInfo();
    private TaskRunnerFactory taskRunnerFactory;
    private MessageAuthorizationPolicy messageAuthorizationPolicy;
    private DiscoveryAgent discoveryAgent;
    private final ConnectorStatistics statistics = new ConnectorStatistics();
    private URI discoveryUri;
    private URI connectUri;
    private String name;
    private boolean disableAsyncDispatch;
    private boolean enableStatusMonitor = false;
    private Broker broker;
    private boolean updateClusterClients = false;
    private boolean rebalanceClusterClients;
    private boolean updateClusterClientsOnRemove = false;
    private String updateClusterFilter;

    public TransportConnector() {
    }

    public TransportConnector(TransportServer server) {
        this();
        setServer(server);
        if (server != null && server.getConnectURI() != null) {
            URI uri = server.getConnectURI();
            if (uri != null && uri.getScheme().equals("vm")) {
                setEnableStatusMonitor(false);
            }
        }

    }

    /**
     * @return Returns the connections.
     */
    public CopyOnWriteArrayList<TransportConnection> getConnections() {
        return connections;
    }

    /**
     * Factory method to create a JMX managed version of this transport
     * connector
     */
    public ManagedTransportConnector asManagedConnector(ManagementContext context, ObjectName connectorName)
            throws IOException, URISyntaxException {
        ManagedTransportConnector rc = new ManagedTransportConnector(context, connectorName, getServer());
        rc.setBrokerInfo(getBrokerInfo());
        rc.setConnectUri(getConnectUri());
        rc.setDisableAsyncDispatch(isDisableAsyncDispatch());
        rc.setDiscoveryAgent(getDiscoveryAgent());
        rc.setDiscoveryUri(getDiscoveryUri());
        rc.setEnableStatusMonitor(isEnableStatusMonitor());
        rc.setMessageAuthorizationPolicy(getMessageAuthorizationPolicy());
        rc.setName(getName());
        rc.setTaskRunnerFactory(getTaskRunnerFactory());
        rc.setUri(getUri());
        rc.setBrokerService(brokerService);
        rc.setUpdateClusterClients(isUpdateClusterClients());
        rc.setRebalanceClusterClients(isRebalanceClusterClients());
        rc.setUpdateClusterFilter(getUpdateClusterFilter());
        rc.setUpdateClusterClientsOnRemove(isUpdateClusterClientsOnRemove());
        return rc;
    }

    public BrokerInfo getBrokerInfo() {
        return brokerInfo;
    }

    public void setBrokerInfo(BrokerInfo brokerInfo) {
        this.brokerInfo = brokerInfo;
    }

    /**
     * 
     * @deprecated use the {@link #setBrokerService(BrokerService)} method
     *             instead.
     */
    @Deprecated
    public void setBrokerName(String name) {
        if (this.brokerInfo == null) {
            this.brokerInfo = new BrokerInfo();
        }
        this.brokerInfo.setBrokerName(name);
    }

    public TransportServer getServer() throws IOException, URISyntaxException {
        if (server == null) {
            setServer(createTransportServer());
        }
        return server;
    }

    public void setServer(TransportServer server) {
        this.server = server;
    }

    public URI getUri() {
        if (uri == null) {
            try {
                uri = getConnectUri();
            } catch (Throwable e) {
            }
        }
        return uri;
    }

    /**
     * Sets the server transport URI to use if there is not a
     * {@link TransportServer} configured via the
     * {@link #setServer(TransportServer)} method. This value is used to lazy
     * create a {@link TransportServer} instance
     * 
     * @param uri
     */
    public void setUri(URI uri) {
        this.uri = uri;
    }

    public TaskRunnerFactory getTaskRunnerFactory() {
        return taskRunnerFactory;
    }

    public void setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory) {
        this.taskRunnerFactory = taskRunnerFactory;
    }

    /**
     * @return the statistics for this connector
     */
    public ConnectorStatistics getStatistics() {
        return statistics;
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

    public void start() throws Exception {
        broker = brokerService.getBroker();
        brokerInfo.setBrokerName(broker.getBrokerName());
        brokerInfo.setBrokerId(broker.getBrokerId());
        brokerInfo.setPeerBrokerInfos(broker.getPeerBrokerInfos());
        brokerInfo.setFaultTolerantConfiguration(broker.isFaultTolerantConfiguration());
        brokerInfo.setBrokerURL(getServer().getConnectURI().toString());
        getServer().setAcceptListener(new TransportAcceptListener() {
            public void onAccept(final Transport transport) {
                try {
                    DefaultThreadPools.getDefaultTaskRunnerFactory().execute(new Runnable() {
                        public void run() {
                            try {
                                Connection connection = createConnection(transport);
                                connection.start();
                            } catch (Exception e) {
                                ServiceSupport.dispose(transport);
                                onAcceptError(e);
                            }
                        }
                    });
                } catch (Exception e) {
                    String remoteHost = transport.getRemoteAddress();
                    ServiceSupport.dispose(transport);
                    onAcceptError(e, remoteHost);
                }
            }

            public void onAcceptError(Exception error) {
                onAcceptError(error, null);
            }

            private void onAcceptError(Exception error, String remoteHost) {
                LOG.error("Could not accept connection " + (remoteHost == null ? "" : "from " + remoteHost) + ": "
                        + error);
                LOG.debug("Reason: " + error, error);
            }
        });
        getServer().setBrokerInfo(brokerInfo);
        getServer().start();

        DiscoveryAgent da = getDiscoveryAgent();
        if (da != null) {
            da.registerService(getPublishableConnectString());
            da.start();
        }
        if (enableStatusMonitor) {
            this.statusDector = new TransportStatusDetector(this);
            this.statusDector.start();
        }

        LOG.info("Connector " + getName() + " Started");
    }

    public String getPublishableConnectString() throws Exception {
        String publishableConnectString = null;
        URI theConnectURI = getConnectUri();
        if (theConnectURI != null) {
            publishableConnectString = theConnectURI.toString();
            // strip off server side query parameters which may not be compatible to
            // clients
            if (theConnectURI.getRawQuery() != null) {
                publishableConnectString = publishableConnectString.substring(0, publishableConnectString
                        .indexOf(theConnectURI.getRawQuery()) - 1);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Publishing: " + publishableConnectString + " for broker transport URI: " + theConnectURI);
        }
        return publishableConnectString;
    }

    public void stop() throws Exception {
        ServiceStopper ss = new ServiceStopper();
        if (discoveryAgent != null) {
            ss.stop(discoveryAgent);
        }
        if (server != null) {
            ss.stop(server);
            server = null;
        }
        if (this.statusDector != null) {
            this.statusDector.stop();
        }

        for (Iterator<TransportConnection> iter = connections.iterator(); iter.hasNext();) {
            TransportConnection c = iter.next();
            ss.stop(c);
        }
        ss.throwFirstException();
        LOG.info("Connector " + getName() + " Stopped");
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Connection createConnection(Transport transport) throws IOException {
        TransportConnection answer = new TransportConnection(this, transport, broker, disableAsyncDispatch ? null
                : taskRunnerFactory);
        boolean statEnabled = this.getStatistics().isEnabled();
        answer.getStatistics().setEnabled(statEnabled);
        answer.setMessageAuthorizationPolicy(messageAuthorizationPolicy);
        return answer;
    }

    protected TransportServer createTransportServer() throws IOException, URISyntaxException {
        if (uri == null) {
            throw new IllegalArgumentException("You must specify either a server or uri property");
        }
        if (brokerService == null) {
            throw new IllegalArgumentException(
                    "You must specify the brokerService property. Maybe this connector should be added to a broker?");
        }
        return TransportFactory.bind(brokerService, uri);
    }

    public DiscoveryAgent getDiscoveryAgent() throws IOException {
        if (discoveryAgent == null) {
            discoveryAgent = createDiscoveryAgent();
        }
        return discoveryAgent;
    }

    protected DiscoveryAgent createDiscoveryAgent() throws IOException {
        if (discoveryUri != null) {
            DiscoveryAgent agent = DiscoveryAgentFactory.createDiscoveryAgent(discoveryUri);

            if( agent!=null && agent instanceof BrokerServiceAware ) {
                ((BrokerServiceAware)agent).setBrokerService(brokerService);
            }

            return agent;
        }
        return null;
    }

    public void setDiscoveryAgent(DiscoveryAgent discoveryAgent) {
        this.discoveryAgent = discoveryAgent;
    }

    public URI getDiscoveryUri() {
        return discoveryUri;
    }

    public void setDiscoveryUri(URI discoveryUri) {
        this.discoveryUri = discoveryUri;
    }

    public URI getConnectUri() throws IOException, URISyntaxException {
        if (connectUri == null) {
            if (server != null) {
                connectUri = server.getConnectURI();
            }
        }
        return connectUri;
    }

    public void setConnectUri(URI transportUri) {
        this.connectUri = transportUri;
    }

    public void onStarted(TransportConnection connection) {
        connections.add(connection);
    }

    public void onStopped(TransportConnection connection) {
        connections.remove(connection);
    }

    public String getName() {
        if (name == null) {
            uri = getUri();
            if (uri != null) {
                name = uri.toString();
            }
        }
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        String rc = getName();
        if (rc == null) {
            rc = super.toString();
        }
        return rc;
    }

    protected ConnectionControl getConnectionControl() {
        boolean rebalance = isRebalanceClusterClients();
        String connectedBrokers = "";
        String self = "";

        if (isUpdateClusterClients()) {
            if (brokerService.getDefaultSocketURIString() != null) {
                self += brokerService.getDefaultSocketURIString();
                self += ",";
            }
            if (rebalance == false) {
                connectedBrokers += self;
            }
            if (this.broker.getPeerBrokerInfos() != null) {
                for (BrokerInfo info : this.broker.getPeerBrokerInfos()) {
                    if (isMatchesClusterFilter(info.getBrokerName())) {
                        connectedBrokers += info.getBrokerURL();
                        connectedBrokers += ",";
                    }
                }
            }
            if (rebalance) {
                connectedBrokers += self;
            }
        }

        ConnectionControl control = new ConnectionControl();
        control.setConnectedBrokers(connectedBrokers);
        control.setRebalanceConnection(rebalance);
        return control;

    }

    public void updateClientClusterInfo() {
        if (isRebalanceClusterClients() || isUpdateClusterClients()) {
            ConnectionControl control = getConnectionControl();
            for (Connection c : this.connections) {
                c.updateClient(control);
            }
        }
    }

    private boolean isMatchesClusterFilter(String brokerName) {
        boolean result = true;
        String filter = getUpdateClusterFilter();
        if (filter != null) {
            filter = filter.trim();
            if (filter.length() > 0) {
                StringTokenizer tokenizer = new StringTokenizer(filter, ",");
                while (result && tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();
                    result = isMatchesClusterFilter(brokerName, token);
                }
            }
        }
        return result;
    }

    private boolean isMatchesClusterFilter(String brokerName, String match) {
        boolean result = true;
        if (brokerName != null && match != null && brokerName.length() > 0 && match.length() > 0) {
            result = Pattern.matches(match, brokerName);
        }
        return result;
    }

    public boolean isDisableAsyncDispatch() {
        return disableAsyncDispatch;
    }

    public void setDisableAsyncDispatch(boolean disableAsyncDispatch) {
        this.disableAsyncDispatch = disableAsyncDispatch;
    }

    /**
     * @return the enableStatusMonitor
     */
    public boolean isEnableStatusMonitor() {
        return enableStatusMonitor;
    }

    /**
     * @param enableStatusMonitor
     *            the enableStatusMonitor to set
     */
    public void setEnableStatusMonitor(boolean enableStatusMonitor) {
        this.enableStatusMonitor = enableStatusMonitor;
    }

    /**
     * This is called by the BrokerService right before it starts the transport.
     */
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    public Broker getBroker() {
        return broker;
    }

    public BrokerService getBrokerService() {
        return brokerService;
    }

    /**
     * @return the updateClusterClients
     */
    public boolean isUpdateClusterClients() {
        return this.updateClusterClients;
    }

    /**
     * @param updateClusterClients
     *            the updateClusterClients to set
     */
    public void setUpdateClusterClients(boolean updateClusterClients) {
        this.updateClusterClients = updateClusterClients;
    }

    /**
     * @return the rebalanceClusterClients
     */
    public boolean isRebalanceClusterClients() {
        return this.rebalanceClusterClients;
    }

    /**
     * @param rebalanceClusterClients
     *            the rebalanceClusterClients to set
     */
    public void setRebalanceClusterClients(boolean rebalanceClusterClients) {
        this.rebalanceClusterClients = rebalanceClusterClients;
    }
 
    /**
     * @return the updateClusterClientsOnRemove
     */
    public boolean isUpdateClusterClientsOnRemove() {
        return this.updateClusterClientsOnRemove;
    }

    /**
     * @param updateClusterClientsOnRemove the updateClusterClientsOnRemove to set
     */
    public void setUpdateClusterClientsOnRemove(boolean updateClusterClientsOnRemove) {
        this.updateClusterClientsOnRemove = updateClusterClientsOnRemove;
    }
    
    /**
     * @return the updateClusterFilter
     */
    public String getUpdateClusterFilter() {
        return this.updateClusterFilter;
    }

    /**
     * @param updateClusterFilter
     *            the updateClusterFilter to set
     */
    public void setUpdateClusterFilter(String updateClusterFilter) {
        this.updateClusterFilter = updateClusterFilter;
    }

    public int connectionCount() {
        return connections.size();
    }
}
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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactorySupport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @org.apache.xbean.XBean
 */
public class TransportConnector implements Connector, BrokerServiceAware {

    final Logger LOG = LoggerFactory.getLogger(TransportConnector.class);
//IC see: https://issues.apache.org/jira/browse/AMQ-3177

    protected final CopyOnWriteArrayList<TransportConnection> connections = new CopyOnWriteArrayList<TransportConnection>();
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
    private String name;
    private boolean disableAsyncDispatch;
    private boolean enableStatusMonitor = false;
    private Broker broker;
    private boolean updateClusterClients = false;
    private boolean rebalanceClusterClients;
    private boolean updateClusterClientsOnRemove = false;
    private String updateClusterFilter;
    private boolean auditNetworkProducers = false;
    private int maximumProducersAllowedPerConnection = Integer.MAX_VALUE;
    private int maximumConsumersAllowedPerConnection  = Integer.MAX_VALUE;
    private PublishedAddressPolicy publishedAddressPolicy = new PublishedAddressPolicy();
    private boolean warnOnRemoteClose = false;

    LinkedList<String> peerBrokers = new LinkedList<String>();
//IC see: https://issues.apache.org/jira/browse/AMQ-3706

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
    public ManagedTransportConnector asManagedConnector(ManagementContext context, ObjectName connectorName) throws IOException, URISyntaxException {
        ManagedTransportConnector rc = new ManagedTransportConnector(context, connectorName, getServer());
        rc.setBrokerInfo(getBrokerInfo());
        rc.setDisableAsyncDispatch(isDisableAsyncDispatch());
        rc.setDiscoveryAgent(getDiscoveryAgent());
        rc.setDiscoveryUri(getDiscoveryUri());
        rc.setEnableStatusMonitor(isEnableStatusMonitor());
        rc.setMessageAuthorizationPolicy(getMessageAuthorizationPolicy());
        rc.setName(getName());
        rc.setTaskRunnerFactory(getTaskRunnerFactory());
        rc.setUri(getUri());
//IC see: https://issues.apache.org/jira/browse/AMQ-1670
        rc.setBrokerService(brokerService);
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
        rc.setUpdateClusterClients(isUpdateClusterClients());
        rc.setRebalanceClusterClients(isRebalanceClusterClients());
//IC see: https://issues.apache.org/jira/browse/AMQ-3125
        rc.setUpdateClusterFilter(getUpdateClusterFilter());
        rc.setUpdateClusterClientsOnRemove(isUpdateClusterClientsOnRemove());
//IC see: https://issues.apache.org/jira/browse/AMQ-3576
        rc.setAuditNetworkProducers(isAuditNetworkProducers());
//IC see: https://issues.apache.org/jira/browse/AMQ-3813
        rc.setMaximumConsumersAllowedPerConnection(getMaximumConsumersAllowedPerConnection());
        rc.setMaximumProducersAllowedPerConnection(getMaximumProducersAllowedPerConnection());
//IC see: https://issues.apache.org/jira/browse/AMQ-3757
//IC see: https://issues.apache.org/jira/browse/AMQ-3707
//IC see: https://issues.apache.org/jira/browse/AMQ-4024
        rc.setPublishedAddressPolicy(getPublishedAddressPolicy());
//IC see: https://issues.apache.org/jira/browse/AMQ-7057
        rc.setWarnOnRemoteClose(isWarnOnRemoteClose());
        return rc;
    }

    @Override
    public BrokerInfo getBrokerInfo() {
        return brokerInfo;
    }

    public void setBrokerInfo(BrokerInfo brokerInfo) {
        this.brokerInfo = brokerInfo;
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
    @Override
    public ConnectorStatistics getStatistics() {
        return statistics;
    }

    public MessageAuthorizationPolicy getMessageAuthorizationPolicy() {
//IC see: https://issues.apache.org/jira/browse/AMQ-591
        return messageAuthorizationPolicy;
    }

    /**
     * Sets the policy used to decide if the current connection is authorized to
     * consume a given message
     */
    public void setMessageAuthorizationPolicy(MessageAuthorizationPolicy messageAuthorizationPolicy) {
        this.messageAuthorizationPolicy = messageAuthorizationPolicy;
    }

    @Override
    public void start() throws Exception {
        broker = brokerService.getBroker();
        brokerInfo.setBrokerName(broker.getBrokerName());
        brokerInfo.setBrokerId(broker.getBrokerId());
        brokerInfo.setPeerBrokerInfos(broker.getPeerBrokerInfos());
        brokerInfo.setFaultTolerantConfiguration(broker.isFaultTolerantConfiguration());
//IC see: https://issues.apache.org/jira/browse/AMQ-3685
        brokerInfo.setBrokerURL(broker.getBrokerService().getDefaultSocketURIString());
        getServer().setAcceptListener(new TransportAcceptListener() {
            @Override
            public void onAccept(final Transport transport) {
//IC see: https://issues.apache.org/jira/browse/AMQ-7167
                final String remoteHost = transport.getRemoteAddress();
                try {
//IC see: https://issues.apache.org/jira/browse/AMQ-3451
                    brokerService.getTaskRunnerFactory().execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
//IC see: https://issues.apache.org/jira/browse/AMQ-5070
                                if (!brokerService.isStopping()) {
                                    Connection connection = createConnection(transport);
                                    connection.start();
                                } else {
                                    throw new BrokerStoppedException("Broker " + brokerService + " is being stopped");
                                }
                            } catch (Exception e) {
                                ServiceSupport.dispose(transport);
                                onAcceptError(e, remoteHost);
                            }
                        }
                    });
                } catch (Exception e) {
                    ServiceSupport.dispose(transport);
                    onAcceptError(e, remoteHost);
                }
            }

            @Override
            public void onAcceptError(Exception error) {
                onAcceptError(error, null);
            }

            private void onAcceptError(Exception error, String remoteHost) {
                if (brokerService != null && brokerService.isStopping()) {
                    LOG.info("Could not accept connection during shutdown {} : {} ({})", (remoteHost == null ? "" : "from " + remoteHost), error.getLocalizedMessage(), getRootCause(error).getMessage());
                } else {
                    LOG.warn("Could not accept connection {}: {} ({})", (remoteHost == null ? "" : "from " + remoteHost), error.getMessage(), getRootCause(error).getMessage());
                    LOG.debug("Reason: " + error.getMessage(), error);
                }
            }
        });
        getServer().setBrokerInfo(brokerInfo);
        getServer().start();

        DiscoveryAgent da = getDiscoveryAgent();
        if (da != null) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2283
            da.registerService(getPublishableConnectString());
            da.start();
        }
        if (enableStatusMonitor) {
//IC see: https://issues.apache.org/jira/browse/AMQ-872
            this.statusDector = new TransportStatusDetector(this);
            this.statusDector.start();
        }

        LOG.info("Connector {} started", getName());
    }

    static Throwable getRootCause(final Throwable throwable) {
//IC see: https://issues.apache.org/jira/browse/AMQ-7301
        final List<Throwable> list = getThrowableList(throwable);
        return list.isEmpty() ? null : list.get(list.size() - 1);
    }

    static List<Throwable> getThrowableList(Throwable throwable) {
        final List<Throwable> list = new ArrayList<>();
        while (throwable != null && !list.contains(throwable)) {
            list.add(throwable);
            throwable = throwable.getCause();
        }
        return list;
    }

    public String getPublishableConnectString() throws Exception {
        String publishableConnectString = publishedAddressPolicy.getPublishableConnectString(this);
        LOG.debug("Publishing: {} for broker transport URI: {}", publishableConnectString, getConnectUri());
        return publishableConnectString;
    }

    public URI getPublishableConnectURI() throws Exception {
        return publishedAddressPolicy.getPublishableConnectURI(this);
    }

    @Override
    public void stop() throws Exception {
        ServiceStopper ss = new ServiceStopper();
        if (discoveryAgent != null) {
            ss.stop(discoveryAgent);
        }
        if (server != null) {
            ss.stop(server);
        }
        if (this.statusDector != null) {
            this.statusDector.stop();
        }

//IC see: https://issues.apache.org/jira/browse/AMQ-4066
        for (TransportConnection connection : connections) {
            ss.stop(connection);
        }
//IC see: https://issues.apache.org/jira/browse/AMQ-3599
        server = null;
        ss.throwFirstException();
        LOG.info("Connector {} stopped", getName());
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Connection createConnection(Transport transport) throws IOException {
        // prefer to use task runner from broker service as stop task runner, as we can then
        // tie it to the lifecycle of the broker service
        TransportConnection answer = new TransportConnection(this, transport, broker, disableAsyncDispatch ? null
//IC see: https://issues.apache.org/jira/browse/AMQ-3451
                : taskRunnerFactory, brokerService.getTaskRunnerFactory());
        boolean statEnabled = this.getStatistics().isEnabled();
//IC see: https://issues.apache.org/jira/browse/AMQ-894
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
        return TransportFactorySupport.bind(brokerService, uri);
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

//IC see: https://issues.apache.org/jira/browse/AMQ-4066
            if (agent != null && agent instanceof BrokerServiceAware) {
                ((BrokerServiceAware) agent).setBrokerService(brokerService);
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
//IC see: https://issues.apache.org/jira/browse/AMQ-3918
        if (server != null) {
            return server.getConnectURI();
        } else {
            return uri;
        }
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
        String separator = "";

        if (isUpdateClusterClients()) {
            synchronized (peerBrokers) {
                for (String uri : getPeerBrokers()) {
                    connectedBrokers += separator + uri;
                    separator = ",";
                }

                if (rebalance) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3706
                    String shuffle = peerBrokers.removeFirst();
                    peerBrokers.addLast(shuffle);
                }
            }
        }
        ConnectionControl control = new ConnectionControl();
        control.setConnectedBrokers(connectedBrokers);
        control.setRebalanceConnection(rebalance);
        return control;
    }

    public void addPeerBroker(BrokerInfo info) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3706
        if (isMatchesClusterFilter(info.getBrokerName())) {
            synchronized (peerBrokers) {
                getPeerBrokers().addLast(info.getBrokerURL());
            }
        }
    }

    public void removePeerBroker(BrokerInfo info) {
        synchronized (peerBrokers) {
            getPeerBrokers().remove(info.getBrokerURL());
        }
    }

    public LinkedList<String> getPeerBrokers() {
        synchronized (peerBrokers) {
            if (peerBrokers.isEmpty()) {
                peerBrokers.add(brokerService.getDefaultSocketURIString());
            }
            return peerBrokers;
        }
    }

    @Override
    public void updateClientClusterInfo() {
        if (isRebalanceClusterClients() || isUpdateClusterClients()) {
            ConnectionControl control = getConnectionControl();
            for (Connection c : this.connections) {
                c.updateClient(control);
//IC see: https://issues.apache.org/jira/browse/AMQ-3706
                if (isRebalanceClusterClients()) {
                    control = getConnectionControl();
                }
            }
        }
    }

    private boolean isMatchesClusterFilter(String brokerName) {
        boolean result = true;
        String filter = getUpdateClusterFilter();
        if (filter != null) {
            filter = filter.trim();
            if (filter.length() > 0) {
//IC see: https://issues.apache.org/jira/browse/AMQ-6880
                result = false;
                StringTokenizer tokenizer = new StringTokenizer(filter, ",");
                while (!result && tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();
                    result = isMatchesClusterFilter(brokerName, token);
                }
            }
        }

        return result;
    }

    private boolean isMatchesClusterFilter(String brokerName, String match) {
        boolean result = false;
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
    @Override
    public void setBrokerService(BrokerService brokerService) {
//IC see: https://issues.apache.org/jira/browse/AMQ-1567
//IC see: https://issues.apache.org/jira/browse/AMQ-1670
        this.brokerService = brokerService;
    }

    public Broker getBroker() {
        return broker;
    }

    public BrokerService getBrokerService() {
//IC see: https://issues.apache.org/jira/browse/AMQ-596
        return brokerService;
    }

    /**
     * @return the updateClusterClients
     */
    @Override
    public boolean isUpdateClusterClients() {
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
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
    @Override
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
    @Override
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
    @Override
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

    @Override
    public int connectionCount() {
        return connections.size();
    }

    @Override
    public boolean isAllowLinkStealing() {
//IC see: https://issues.apache.org/jira/browse/AMQ-4719
        return server.isAllowLinkStealing();
    }

    public boolean isAuditNetworkProducers() {
//IC see: https://issues.apache.org/jira/browse/AMQ-3576
        return auditNetworkProducers;
    }

    /**
     * Enable a producer audit on network connections, Traps the case of a missing send reply and resend.
     * Note: does not work with conduit=false, networked composite destinations or networked virtual topics
     * @param auditNetworkProducers
     */
    public void setAuditNetworkProducers(boolean auditNetworkProducers) {
        this.auditNetworkProducers = auditNetworkProducers;
    }

    public int getMaximumProducersAllowedPerConnection() {
//IC see: https://issues.apache.org/jira/browse/AMQ-3813
        return maximumProducersAllowedPerConnection;
    }

    public void setMaximumProducersAllowedPerConnection(int maximumProducersAllowedPerConnection) {
        this.maximumProducersAllowedPerConnection = maximumProducersAllowedPerConnection;
    }

    public int getMaximumConsumersAllowedPerConnection() {
        return maximumConsumersAllowedPerConnection;
    }

    public void setMaximumConsumersAllowedPerConnection(int maximumConsumersAllowedPerConnection) {
        this.maximumConsumersAllowedPerConnection = maximumConsumersAllowedPerConnection;
    }

    /**
     * Gets the currently configured policy for creating the published connection address of this
     * TransportConnector.
     *
     * @return the publishedAddressPolicy
     */
    public PublishedAddressPolicy getPublishedAddressPolicy() {
//IC see: https://issues.apache.org/jira/browse/AMQ-3757
//IC see: https://issues.apache.org/jira/browse/AMQ-3707
//IC see: https://issues.apache.org/jira/browse/AMQ-4024
        return publishedAddressPolicy;
    }

    /**
     * Sets the configured policy for creating the published connection address of this
     * TransportConnector.
     *
     * @return the publishedAddressPolicy
     */
    public void setPublishedAddressPolicy(PublishedAddressPolicy publishedAddressPolicy) {
        this.publishedAddressPolicy = publishedAddressPolicy;
    }

    public boolean isWarnOnRemoteClose() {
//IC see: https://issues.apache.org/jira/browse/AMQ-7057
        return warnOnRemoteClose;
    }

    public void setWarnOnRemoteClose(boolean warnOnRemoteClose) {
        this.warnOnRemoteClose = warnOnRemoteClose;
    }
}

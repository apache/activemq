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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.activemq.broker.jmx.ManagedTransportConnector;
import org.apache.activemq.broker.region.ConnectorStatistics;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;

/**
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 1.6 $
 */
public class TransportConnector implements Connector {

    private static final Log log = LogFactory.getLog(TransportConnector.class);

    private Broker broker;
    private TransportServer server;
    private URI uri;
    private BrokerInfo brokerInfo = new BrokerInfo();
    private TaskRunnerFactory taskRunnerFactory = null;
    private MessageAuthorizationPolicy messageAuthorizationPolicy;
    private DiscoveryAgent discoveryAgent;
    protected CopyOnWriteArrayList connections = new CopyOnWriteArrayList();
    protected TransportStatusDetector statusDector;
    private ConnectorStatistics statistics = new ConnectorStatistics();
    private URI discoveryUri;
    private URI connectUri;


    /**
     * @return Returns the connections.
     */
    public CopyOnWriteArrayList getConnections(){
        return connections;
    }

    public TransportConnector() {
        this.statusDector = new TransportStatusDetector(this);
    }

    public TransportConnector(Broker broker, TransportServer server) {
        this();
        setBroker(broker);
        setServer(server);
    }

    /**
     * Factory method to create a JMX managed version of this transport connector
     */
    public ManagedTransportConnector asManagedConnector(MBeanServer mbeanServer, ObjectName connectorName) throws IOException, URISyntaxException {
        ManagedTransportConnector rc = new ManagedTransportConnector(mbeanServer,connectorName,  getBroker(), getServer());
        rc.setTaskRunnerFactory(getTaskRunnerFactory());
        rc.setUri(uri);
        rc.setConnectUri(connectUri);
        rc.setDiscoveryAgent(discoveryAgent);
        rc.setDiscoveryUri(discoveryUri);
        return rc;
    }
    
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

    public String getName() throws IOException, URISyntaxException {
        return getServer().getConnectURI().toString();
    }

    public Broker getBroker() {
        return broker;
    }

    public void setBroker(Broker broker) {
        this.broker = broker;
        brokerInfo.setBrokerId(broker.getBrokerId());
        brokerInfo.setPeerBrokerInfos(broker.getPeerBrokerInfos());
    }
	
    public void setBrokerName(String brokerName) {
        brokerInfo.setBrokerName(brokerName);
    }

    public void setServer(TransportServer server) {
        this.server = server;
        this.brokerInfo.setBrokerURL(server.getConnectURI().toString());
        this.server.setAcceptListener(new TransportAcceptListener() {
            public void onAccept(Transport transport) {
                try {
                    Connection connection = createConnection(transport);
                    connection.start();
                }
                catch (Exception e) {
                	ServiceSupport.dispose(transport);
                    onAcceptError(e);
                }
            }

            public void onAcceptError(Exception error) {
                log.error("Could not accept connection: " + error, error);
            }
        });
        this.server.setBrokerInfo(brokerInfo);
    }

    public URI getUri() {
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
     * Sets the policy used to decide if the current connection is authorized to consume
     * a given message
     */
    public void setMessageAuthorizationPolicy(MessageAuthorizationPolicy messageAuthorizationPolicy) {
        this.messageAuthorizationPolicy = messageAuthorizationPolicy;
    }

    public void start() throws Exception {
        getServer().start();
        log.info("Accepting connection on: "+getServer().getConnectURI());

        DiscoveryAgent da = getDiscoveryAgent();
        if( da!=null ) {
            da.registerService(getConnectUri().toString());
            da.start();
        }

        this.statusDector.start();
    }

    public void stop() throws Exception {
        ServiceStopper ss = new ServiceStopper();
        if( discoveryAgent!=null ) {
            ss.stop(discoveryAgent);
        }
        if (server != null) {
            ss.stop(server);
        }
        this.statusDector.stop();
        for (Iterator iter = connections.iterator(); iter.hasNext();) {
            TransportConnection c = (TransportConnection) iter.next();
            ss.stop(c);
        }
        ss.throwFirstException();
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Connection createConnection(Transport transport) throws IOException {
        TransportConnection answer = new TransportConnection(this, transport, broker, taskRunnerFactory);
        answer.setMessageAuthorizationPolicy(messageAuthorizationPolicy);
        return answer;
    }

    protected TransportServer createTransportServer() throws IOException, URISyntaxException {
        if (uri == null) {
            throw new IllegalArgumentException("You must specify either a server or uri property");
        }
        if (broker == null) {
            throw new IllegalArgumentException("You must specify the broker property. Maybe this connector should be added to a broker?");
        }
        return TransportFactory.bind(broker.getBrokerId().getValue(),uri);
    }
    
    public DiscoveryAgent getDiscoveryAgent() throws IOException {
        if( discoveryAgent==null ) {
            discoveryAgent = createDiscoveryAgent();
        }
        return discoveryAgent;
    }

    protected DiscoveryAgent createDiscoveryAgent() throws IOException {
        if( discoveryUri!=null ) {
            return DiscoveryAgentFactory.createDiscoveryAgent(discoveryUri);
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
        if( connectUri==null ) {
            if( getServer().getConnectURI()==null ) {
                throw new IllegalStateException("The transportConnector has not been started.");
            }
            connectUri = getServer().getConnectURI();
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

}

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
package org.apache.activemq.transport.amqp.client;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.activemq.transport.amqp.client.transport.NettyTransport;
import org.apache.activemq.transport.amqp.client.transport.NettyTransportFactory;
import org.apache.qpid.proton.amqp.Symbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection instance used to connect to the Broker using Proton as
 * the AMQP protocol handler.
 */
public class AmqpClient {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpClient.class);

    private final String username;
    private final String password;
    private final URI remoteURI;
    private String authzid;
    private String mechanismRestriction;
    private boolean traceFrames;

    private AmqpValidator stateInspector = new AmqpValidator();
    private List<Symbol> offeredCapabilities = Collections.emptyList();
    private Map<Symbol, Object> offeredProperties = Collections.emptyMap();

    /**
     * Creates an AmqpClient instance which can be used as a factory for connections.
     *
     * @param remoteURI
     *        The address of the remote peer to connect to.
     * @param username
     *	      The user name to use when authenticating the client.
     * @param password
     *		  The password to use when authenticating the client.
     */
    public AmqpClient(URI remoteURI, String username, String password) {
        this.remoteURI = remoteURI;
        this.password = password;
        this.username = username;
    }

    /**
     * Creates a connection with the broker at the given location, this method initiates a
     * connect attempt immediately and will fail if the remote peer cannot be reached.
     *
     * @returns a new connection object used to interact with the connected peer.
     *
     * @throws Exception if an error occurs attempting to connect to the Broker.
     */
    public AmqpConnection connect() throws Exception {

        AmqpConnection connection = createConnection();

        LOG.debug("Attempting to create new connection to peer: {}", remoteURI);
        connection.connect();

        return connection;
    }

    /**
     * Creates a connection object using the configured values for user, password, remote URI
     * etc.  This method does not immediately initiate a connection to the remote leaving that
     * to the caller which provides a connection object that can have additional configuration
     * changes applied before the <code>connect</code> method is invoked.
     *
     * @returns a new connection object used to interact with the connected peer.
     *
     * @throws Exception if an error occurs attempting to connect to the Broker.
     */
    public AmqpConnection createConnection() throws Exception {
        if (username == null && password != null) {
            throw new IllegalArgumentException("Password must be null if user name value is null");
        }

        NettyTransport transport = NettyTransportFactory.createTransport(remoteURI);
        AmqpConnection connection = new AmqpConnection(transport, username, password);

        connection.setMechanismRestriction(mechanismRestriction);
        connection.setAuthzid(authzid);

        connection.setOfferedCapabilities(getOfferedCapabilities());
        connection.setOfferedProperties(getOfferedProperties());
        connection.setStateInspector(getStateInspector());
        connection.setTraceFrames(isTraceFrames());

        return connection;
    }

    /**
     * @return the user name value given when constructed.
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the password value given when constructed.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param authzid
     *        The authzid used when authenticating (currently only with PLAIN)
     */
    public void setAuthzid(String authzid) {
        this.authzid = authzid;
    }

    public String getAuthzid() {
        return authzid;
    }

    /**
     * @param mechanismRestriction
     *        The mechanism to use when authenticating (if offered by the server)
     */
    public void setMechanismRestriction(String mechanismRestriction) {
        this.mechanismRestriction = mechanismRestriction;
    }

    public String getMechanismRestriction() {
        return mechanismRestriction;
    }

    /**
     * @return the currently set address to use to connect to the AMQP peer.
     */
    public URI getRemoteURI() {
        return remoteURI;
    }

    /**
     * Sets the offered capabilities that should be used when a new connection attempt
     * is made.
     *
     * @param offeredCapabilities
     *        the list of capabilities to offer when connecting.
     */
    public void setOfferedCapabilities(List<Symbol> offeredCapabilities) {
        if (offeredCapabilities != null) {
            offeredCapabilities = Collections.emptyList();
        }

        this.offeredCapabilities = offeredCapabilities;
    }

    /**
     * @return an unmodifiable view of the currently set offered capabilities
     */
    public List<Symbol> getOfferedCapabilities() {
        return Collections.unmodifiableList(offeredCapabilities);
    }

    /**
     * Sets the offered connection properties that should be used when a new connection
     * attempt is made.
     *
     * @param connectionProperties
     *        the map of properties to offer when connecting.
     */
    public void setOfferedProperties(Map<Symbol, Object> offeredProperties) {
        if (offeredProperties != null) {
            offeredProperties = Collections.emptyMap();
        }

        this.offeredProperties = offeredProperties;
    }

    /**
     * @return an unmodifiable view of the currently set connection properties.
     */
    public Map<Symbol, Object> getOfferedProperties() {
        return Collections.unmodifiableMap(offeredProperties);
    }

    /**
     * @return the currently set state inspector used to check state after various events.
     */
    public AmqpValidator getStateInspector() {
        return stateInspector;
    }

    /**
     * Sets the state inspector used to check that the AMQP resource is valid after
     * specific lifecycle events such as open and close.
     *
     * @param stateInspector
     *        the new state inspector to use.
     */
    public void setValidator(AmqpValidator stateInspector) {
        if (stateInspector == null) {
            stateInspector = new AmqpValidator();
        }

        this.stateInspector = stateInspector;
    }

    /**
     * @return the traceFrames setting for the client, true indicates frame tracing is on.
     */
    public boolean isTraceFrames() {
        return traceFrames;
    }

    /**
     * Controls whether connections created from this client object will log AMQP
     * frames to a trace level logger or not.
     *
     * @param traceFrames
     *      configure the trace frames option for the client created connections.
     */
    public void setTraceFrames(boolean traceFrames) {
        this.traceFrames = traceFrames;
    }

    @Override
    public String toString() {
        return "AmqpClient: " + getRemoteURI().getHost() + ":" + getRemoteURI().getPort();
    }

    /**
     * Creates an anonymous connection with the broker at the given location.
     *
     * @param broker
     *        the address of the remote broker instance.
     *
     * @returns a new connection object used to interact with the connected peer.
     *
     * @throws Exception if an error occurs attempting to connect to the Broker.
     */
    public static AmqpConnection connect(URI broker) throws Exception {
        return connect(broker, null, null);
    }

    /**
     * Creates a connection with the broker at the given location.
     *
     * @param broker
     *        the address of the remote broker instance.
     * @param username
     *        the user name to use to connect to the broker or null for anonymous.
     * @param password
     *        the password to use to connect to the broker, must be null if user name is null.
     *
     * @returns a new connection object used to interact with the connected peer.
     *
     * @throws Exception if an error occurs attempting to connect to the Broker.
     */
    public static AmqpConnection connect(URI broker, String username, String password) throws Exception {
        if (username == null && password != null) {
            throw new IllegalArgumentException("Password must be null if user name value is null");
        }

        AmqpClient client = new AmqpClient(broker, username, password);

        return client.connect();
    }
}

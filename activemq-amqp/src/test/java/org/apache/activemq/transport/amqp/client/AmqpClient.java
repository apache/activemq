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

import org.apache.activemq.transport.amqp.client.util.ClientTcpTransport;
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

        ClientTcpTransport transport = new ClientTcpTransport(remoteURI);
        AmqpConnection connection = new AmqpConnection(transport, username, password);

        connection.setOfferedCapabilities(getOfferedCapabilities());
        connection.setOfferedProperties(getOfferedProperties());
        connection.setStateInspector(getStateInspector());

        return connection;
    }

    /**
     * @return the user name value given when connect was called, always null before connect.
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the password value given when connect was called, always null before connect.
     */
    public String getPassword() {
        return password;
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

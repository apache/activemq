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
package org.apache.activemq.console.command;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;

public abstract class AbstractAmqCommand extends AbstractCommand {
    private URI brokerUrl;
    private ConnectionFactory factory;
    private String factoryClassString;
    private String username;
    private String password;
    private PasswordFactory passwordFactory;
    private final List<Connection> connections = new ArrayList<Connection>();
    private String passwordFactoryClassString;

    /**
     * Establishes a connection to the remote broker specified by the broker
     * url.
     * 
     * @return - connection to the broker
     * @throws JMSException
     */
    protected Connection createConnection() throws JMSException {
        return createConnection(getUsername(), getPassword());
    }

    /**
     * Establishes a connection to the remote broker specified by the broker
     * url.
     * 
     * @param username - username for the connection
     * @param password - password for the connection
     * @return - connection to the broker
     * @throws JMSException
     */
    protected Connection createConnection(String username, String password) throws JMSException {
        if (getBrokerUrl() == null) {
            context
                .printException(new IllegalStateException(
                                                          "You must specify a broker URL to connect to using the --amqurl option."));
            return null;
        }

        ConnectionFactory factory = getConnectionFactory();
        Connection conn;

        if (null == username && null == password)
            conn = factory.createConnection();
        else
            conn = factory.createConnection(username, password);

        connections.add(conn);
        conn.start();

        return conn;
    }

    /**
     * Close all created connections.
     */
    protected void closeAllConnections() {
        for (Iterator<Connection> i = connections.iterator(); i.hasNext();) {
            try {
                i.next().close();
            } catch (Exception e) {
            }
        }

        connections.clear();
    }

    /**
     * Handle the --amqurl option.
     * 
     * @param token - current option
     * @param tokens - succeeding list of arguments
     * @throws Exception
     */
    protected void handleOption(String token, List tokens) throws Exception {
        // Try to handle the options first
        if (token.equals("--amqurl")) {
            // If no broker url specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("Broker URL not specified."));
                tokens.clear();
                return;
            }

            // If broker url already specified
            if (getBrokerUrl() != null) {
                context
                    .printException(new IllegalArgumentException("Multiple broker URL cannot be specified."));
                tokens.clear();
                return;
            }

            String strBrokerUrl = (String)tokens.remove(0);

            try {
                setBrokerUrl(new URI(strBrokerUrl));
            } catch (URISyntaxException e) {
                context.printException(e);
                tokens.clear();
                return;
            }
        } else if (token.equals("--factory")) {
            factoryClassString = (String) tokens.remove(0);
        } else if (token.equals("--passwordFactory")) {
            passwordFactoryClassString = (String) tokens.remove(0);
        } else if (token.equals("--password")) {
            password = (String) tokens.remove(0);
        } else if (token.equals("--user")) {
            username = (String) tokens.remove(0);
        } else {
            // Let the super class handle the option
            super.handleOption(token, tokens);
        }
    }

    /**
     * Set the broker url.
     * 
     * @param brokerUrl - new broker url
     */
    protected void setBrokerUrl(URI brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    /**
     * Set the broker url.
     * 
     * @param address - address of the new broker url
     * @throws URISyntaxException
     */
    protected void setBrokerUrl(String address) throws URISyntaxException {
        this.brokerUrl = new URI(address);
    }

    /**
     * Get the current broker url.
     * 
     * @return current broker url
     */
    protected URI getBrokerUrl() {
        return brokerUrl;
    }

    /**
     * @return the factory
     */
    @SuppressWarnings("unchecked")
    public ConnectionFactory getConnectionFactory() {
        if (factory == null && factoryClassString != null) {
            try {
                Class klass = Class.forName(factoryClassString);

                if (getUsername() != null || getPassword() != null) {
                    factory = (ConnectionFactory) klass.getConstructor(
                            String.class, String.class, URI.class).newInstance(
                            getUsername(), getPassword(), getBrokerUrl());
                } else {
                    factory = (ConnectionFactory) klass.getConstructor(
                            URI.class).newInstance(getBrokerUrl());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (factory == null) {
            if (getUsername() != null || getPassword() != null) {
                factory = new ActiveMQConnectionFactory(getUsername(),
                        getPassword(), getBrokerUrl());
            } else {
                factory = new ActiveMQConnectionFactory(getBrokerUrl());
            }
        }

        return factory;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param factory the factory to set
     */
    public void setFactory(ConnectionFactory factory) {
        this.factory = factory;
    }

    /**
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        if (null == password)
            return null;

        return getPasswordFactory().getPassword(password);
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the passwordFactory
     */
    @SuppressWarnings("unchecked")
    public PasswordFactory getPasswordFactory() {
        if (passwordFactory == null && passwordFactoryClassString != null) {
            try {
                Class klass = Class.forName(passwordFactoryClassString);
                passwordFactory = (PasswordFactory) klass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Preserve the fallback case, if someone did specify a bad class, let them realize when things don't work.
        if (passwordFactory == null) {
            passwordFactory = DefaultPasswordFactory.factory;
        }

        return passwordFactory;
    }

    /**
     * @param passwordFactory the passwordFactory to set
     */
    public void setPasswordFactory(PasswordFactory passwordFactory) {
        this.passwordFactory = passwordFactory;
    }
}

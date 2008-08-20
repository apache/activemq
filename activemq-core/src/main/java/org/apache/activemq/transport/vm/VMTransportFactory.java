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
package org.apache.activemq.transport.vm;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerFactoryHandler;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.MarshallingTransportFilter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.URISupport.CompositeData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class VMTransportFactory extends TransportFactory {
    
    public static final ConcurrentHashMap<String, BrokerService> BROKERS = new ConcurrentHashMap<String, BrokerService>();
    public static final ConcurrentHashMap<String, TransportConnector> CONNECTORS = new ConcurrentHashMap<String, TransportConnector>();
    public static final ConcurrentHashMap<String, VMTransportServer> SERVERS = new ConcurrentHashMap<String, VMTransportServer>();
    private static final Log LOG = LogFactory.getLog(VMTransportFactory.class);
    
    BrokerFactoryHandler brokerFactoryHandler;

    public Transport doConnect(URI location) throws Exception {
        return VMTransportServer.configure(doCompositeConnect(location));
    }

    public Transport doCompositeConnect(URI location) throws Exception {
        URI brokerURI;
        String host;
        Map<String, String> options;
        boolean create = true;
        int waitForStart = -1;
        CompositeData data = URISupport.parseComposite(location);
        if (data.getComponents().length == 1 && "broker".equals(data.getComponents()[0].getScheme())) {
            brokerURI = data.getComponents()[0];
            CompositeData brokerData = URISupport.parseComposite(brokerURI);
            host = (String)brokerData.getParameters().get("brokerName");
            if (host == null) {
                host = "localhost";
            }
            if (brokerData.getPath() != null) {
                host = data.getPath();
            }
            options = data.getParameters();
            location = new URI("vm://" + host);
        } else {
            // If using the less complex vm://localhost?broker.persistent=true
            // form
            try {
                host = location.getHost();
                options = URISupport.parseParamters(location);
                String config = (String)options.remove("brokerConfig");
                if (config != null) {
                    brokerURI = new URI(config);
                } else {
                    Map brokerOptions = IntrospectionSupport.extractProperties(options, "broker.");
                    brokerURI = new URI("broker://()/" + host + "?"
                                        + URISupport.createQueryString(brokerOptions));
                }
                if ("false".equals(options.remove("create"))) {
                    create = false;
                }
                String waitForStartString = options.remove("waitForStart");
                if (waitForStartString != null) {
                    waitForStart = Integer.parseInt(waitForStartString);
                }
            } catch (URISyntaxException e1) {
                throw IOExceptionSupport.create(e1);
            }
            location = new URI("vm://" + host);
        }
        if (host == null) {
            host = "localhost";
        }
        VMTransportServer server = SERVERS.get(host);
        // validate the broker is still active
        if (!validateBroker(host) || server == null) {
            BrokerService broker = null;
            // Synchronize on the registry so that multiple concurrent threads
            // doing this do not think that the broker has not been created and
            // cause multiple brokers to be started.
            synchronized (BrokerRegistry.getInstance().getRegistryMutext()) {
                broker = lookupBroker(BrokerRegistry.getInstance(), host, waitForStart);
                if (broker == null) {
                    if (!create) {
                        throw new IOException("Broker named '" + host + "' does not exist.");
                    }
                    try {
                        if (brokerFactoryHandler != null) {
                            broker = brokerFactoryHandler.createBroker(brokerURI);
                        } else {
                            broker = BrokerFactory.createBroker(brokerURI);
                        }
                        broker.start();
                    } catch (URISyntaxException e) {
                        throw IOExceptionSupport.create(e);
                    }
                    BROKERS.put(host, broker);
                    BrokerRegistry.getInstance().getRegistryMutext().notifyAll();
                }

                server = SERVERS.get(host);
                if (server == null) {
                    server = (VMTransportServer)bind(location, true);
                    TransportConnector connector = new TransportConnector(server);
                    connector.setBrokerService(broker);
                    connector.setUri(location);
                    connector.setTaskRunnerFactory(broker.getTaskRunnerFactory());
                    connector.start();
                    CONNECTORS.put(host, connector);
                }

            }
        }

        VMTransport vmtransport = server.connect();
        IntrospectionSupport.setProperties(vmtransport.peer, new HashMap<String,String>(options));
        IntrospectionSupport.setProperties(vmtransport, options);
        Transport transport = vmtransport;
        if (vmtransport.isMarshal()) {
            Map<String, String> optionsCopy = new HashMap<String, String>(options);
            transport = new MarshallingTransportFilter(transport, createWireFormat(options),
                                                       createWireFormat(optionsCopy));
        }
        if (!options.isEmpty()) {
            throw new IllegalArgumentException("Invalid connect parameters: " + options);
        }
        return transport;
    }

   /**
    * @param registry
    * @param brokerName
    * @param waitForStart - time in milliseconds to wait for a broker to appear
    * @return
    */
    private BrokerService lookupBroker(final BrokerRegistry registry, final String brokerName, int waitForStart) {
        BrokerService broker = null;
        synchronized(registry.getRegistryMutext()) {
            broker = registry.lookup(brokerName);
            if (broker == null && waitForStart > 0) {
                final long expiry = System.currentTimeMillis() + waitForStart;
                while (broker == null  && expiry > System.currentTimeMillis()) {
                    long timeout = Math.max(0, expiry - System.currentTimeMillis());
                    try {
                        LOG.debug("waiting for broker named: " + brokerName + " to start");
                        registry.getRegistryMutext().wait(timeout);
                    } catch (InterruptedException ignored) {
                    }
                    broker = registry.lookup(brokerName);
                }
            }
        }
        return broker;
    }

    public TransportServer doBind(URI location) throws IOException {
        return bind(location, false);
    }

    /**
     * @param location
     * @return the TransportServer
     * @throws IOException
     */
    private TransportServer bind(URI location, boolean dispose) throws IOException {
        String host = location.getHost();
        LOG.debug("binding to broker: " + host);
        VMTransportServer server = new VMTransportServer(location, dispose);
        Object currentBoundValue = SERVERS.get(host);
        if (currentBoundValue != null) {
            throw new IOException("VMTransportServer already bound at: " + location);
        }
        SERVERS.put(host, server);
        return server;
    }

    public static void stopped(VMTransportServer server) {
        String host = server.getBindURI().getHost();
        SERVERS.remove(host);
        TransportConnector connector = CONNECTORS.remove(host);
        if (connector != null) {
            LOG.debug("Shutting down VM connectors for broker: " + host);
            ServiceSupport.dispose(connector);
            BrokerService broker = BROKERS.remove(host);
            if (broker != null) {
                ServiceSupport.dispose(broker);
            }
        }
    }

    public static void stopped(String host) {
        SERVERS.remove(host);
        TransportConnector connector = CONNECTORS.remove(host);
        if (connector != null) {
            LOG.debug("Shutting down VM connectors for broker: " + host);
            ServiceSupport.dispose(connector);
            BrokerService broker = BROKERS.remove(host);
            if (broker != null) {
                ServiceSupport.dispose(broker);
            }
        }
    }

    public BrokerFactoryHandler getBrokerFactoryHandler() {
        return brokerFactoryHandler;
    }

    public void setBrokerFactoryHandler(BrokerFactoryHandler brokerFactoryHandler) {
        this.brokerFactoryHandler = brokerFactoryHandler;
    }

    private boolean validateBroker(String host) {
        boolean result = true;
        if (BROKERS.containsKey(host) || SERVERS.containsKey(host) || CONNECTORS.containsKey(host)) {
            // check the broker is still in the BrokerRegistry
            TransportConnector connector = CONNECTORS.get(host);
            if (BrokerRegistry.getInstance().lookup(host) == null
                || (connector != null && connector.getBroker().isStopped())) {
                result = false;
                // clean-up
                BROKERS.remove(host);
                SERVERS.remove(host);
                if (connector != null) {
                    CONNECTORS.remove(host);
                    if (connector != null) {
                        ServiceSupport.dispose(connector);
                    }
                }
            }
        }
        return result;
    }
}

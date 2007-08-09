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
package org.apache.activemq.transport.peer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.BrokerFactoryHandler;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.vm.VMTransportFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;

public class PeerTransportFactory extends TransportFactory {

    final public static ConcurrentHashMap brokers = new ConcurrentHashMap();

    final public static ConcurrentHashMap connectors = new ConcurrentHashMap();

    final public static ConcurrentHashMap servers = new ConcurrentHashMap();

    private IdGenerator idGenerator = new IdGenerator("peer-");

    public Transport doConnect(URI location) throws Exception {
        VMTransportFactory vmTransportFactory = createTransportFactory(location);
        return vmTransportFactory.doConnect(location);
    }

    public Transport doCompositeConnect(URI location) throws Exception {
        VMTransportFactory vmTransportFactory = createTransportFactory(location);
        return vmTransportFactory.doCompositeConnect(location);
    }

    /**
     * @param location
     * @return the converted URI
     * @throws URISyntaxException
     */
    private VMTransportFactory createTransportFactory(URI location) throws IOException {
        try {
            String group = location.getHost();
            String broker = URISupport.stripPrefix(location.getPath(), "/");

            if (group == null) {
                group = "default";
            }
            if (broker == null || broker.length() == 0) {
                broker = idGenerator.generateSanitizedId();
            }

            final Map brokerOptions = new HashMap(URISupport.parseParamters(location));
            if (!brokerOptions.containsKey("persistent")) {
                brokerOptions.put("persistent", "false");
            }

            final URI finalLocation = new URI("vm://" + broker);
            final String finalBroker = broker;
            final String finalGroup = group;
            VMTransportFactory rc = new VMTransportFactory() {
                public Transport doConnect(URI ignore) throws Exception {
                    return super.doConnect(finalLocation);
                };

                public Transport doCompositeConnect(URI ignore) throws Exception {
                    return super.doCompositeConnect(finalLocation);
                };
            };
            rc.setBrokerFactoryHandler(new BrokerFactoryHandler() {
                public BrokerService createBroker(URI brokerURI) throws Exception {
                    BrokerService service = new BrokerService();
                    IntrospectionSupport.setProperties(service, brokerOptions);
                    service.setBrokerName(finalBroker);
                    TransportConnector c = service.addConnector("tcp://localhost:0");
                    c.setDiscoveryUri(new URI("multicast://" + finalGroup));
                    service.addNetworkConnector("multicast://" + finalGroup);
                    return service;
                }
            });
            return rc;

        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public TransportServer doBind(String brokerId, URI location) throws IOException {
        throw new IOException("This protocol does not support being bound.");
    }

}

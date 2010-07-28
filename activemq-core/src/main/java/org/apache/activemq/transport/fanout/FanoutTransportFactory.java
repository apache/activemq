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
package org.apache.activemq.transport.fanout;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.discovery.DiscoveryTransport;
import org.apache.activemq.transport.discovery.DiscoveryTransportFactory;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.URISupport.CompositeData;

public class FanoutTransportFactory extends TransportFactory {

    public Transport doConnect(URI location) throws IOException {
        try {
            Transport transport = createTransport(location);
            transport = new MutexTransport(transport);
            transport = new ResponseCorrelator(transport);
            return transport;
        } catch (URISyntaxException e) {
            throw new IOException("Invalid location: " + location);
        }
    }

    public Transport doCompositeConnect(URI location) throws IOException {
        try {
            return createTransport(location);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid location: " + location);
        }
    }

    /**
     * @param location
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    public Transport createTransport(URI location) throws IOException, URISyntaxException {
        CompositeData compositeData = URISupport.parseComposite(location);
        Map<String, String> parameters = compositeData.getParameters();
        FanoutTransport fanoutTransport = createTransport(parameters);        
        DiscoveryTransport discoveryTransport = DiscoveryTransportFactory.createTransport(fanoutTransport, compositeData);        
        return discoveryTransport;
    }

    public FanoutTransport createTransport(Map<String,String> parameters) throws IOException {
        FanoutTransport transport = new FanoutTransport();
        IntrospectionSupport.setProperties(transport, parameters);
        return transport;
    }

    public TransportServer doBind(URI location) throws IOException {
        throw new IOException("Invalid server URI: " + location);
    }

}

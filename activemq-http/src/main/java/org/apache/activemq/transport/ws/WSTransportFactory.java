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

package org.apache.activemq.transport.ws;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;

/**
 * Factory for WebSocket (ws) transport
 */
public class WSTransportFactory extends TransportFactory implements BrokerServiceAware {

    private BrokerService brokerService;

    @Override
    public TransportServer doBind(URI location) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            WSTransportServer result = new WSTransportServer(location);
            Map<String, Object> httpOptions = IntrospectionSupport.extractProperties(options, "http.");
            Map<String, Object> transportOptions = IntrospectionSupport.extractProperties(options, "");
            IntrospectionSupport.setProperties(result, transportOptions);
            result.setBrokerService(brokerService);
            result.setTransportOption(transportOptions);
            result.setHttpOptions(httpOptions);
            return result;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }
}

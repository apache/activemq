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
package org.apache.activemq.transport.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.URISupport.CompositeData;

public class MockTransportFactory extends TransportFactory {

    @Override
    public Transport doConnect(URI location) throws URISyntaxException, Exception {
        Transport transport = createTransport(URISupport.parseComposite(location));
        transport = new MutexTransport(transport);
        transport = new ResponseCorrelator(transport);
        return transport;
    }

    @Override
    public Transport doCompositeConnect(URI location) throws URISyntaxException, Exception {
        return createTransport(URISupport.parseComposite(location));
    }

    /**
     * @param location
     * @return a new Transport instance.
     * @throws Exception
     */
    public Transport createTransport(CompositeData compositData) throws Exception {
        MockTransport transport = new MockTransport(TransportFactory.compositeConnect(compositData.getComponents()[0]));
        IntrospectionSupport.setProperties(transport, compositData.getParameters());
        return transport;
    }

    @Override
    public TransportServer doBind(URI location) throws IOException {
        throw new IOException("This protocol does not support being bound.");
    }
}

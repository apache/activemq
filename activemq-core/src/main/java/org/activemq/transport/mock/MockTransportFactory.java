/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activemq.transport.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.activemq.transport.MutexTransport;
import org.activemq.transport.ResponseCorrelator;
import org.activemq.transport.Transport;
import org.activemq.transport.TransportFactory;
import org.activemq.transport.TransportServer;
import org.activemq.util.IntrospectionSupport;
import org.activemq.util.URISupport;
import org.activemq.util.URISupport.CompositeData;

public class MockTransportFactory extends TransportFactory {

    public Transport doConnect(URI location) throws URISyntaxException, Exception {
        Transport transport = createTransport(URISupport.parseComposite(location));
        transport =  new MutexTransport(transport);
        transport = new ResponseCorrelator(transport);
        return transport;
    }
    
    public Transport doCompositeConnect(URI location) throws URISyntaxException, Exception {
        return createTransport(URISupport.parseComposite(location));
    }
    
    /**
     * @param location
     * @return 
     * @throws Exception 
     */
    public Transport createTransport(CompositeData compositData) throws Exception {
        MockTransport transport = new MockTransport( TransportFactory.compositeConnect(compositData.getComponents()[0]) );
        IntrospectionSupport.setProperties(transport, compositData.getParameters());
        return transport;
    }

    public TransportServer doBind(String brokerId,URI location) throws IOException {
        throw new IOException("This protocol does not support being bound.");
    }

}

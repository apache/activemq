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
package org.apache.activemq.transport.https;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.HashMap;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.http.HttpTransportFactory;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Factory of HTTPS based transports
 * 
 * 
 */
public class HttpsTransportFactory extends HttpTransportFactory {
    
    public TransportServer doBind(String brokerId, URI location) throws IOException {
        return doBind(location);
    }
    
    public TransportServer doBind(URI location) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            HttpsTransportServer result = new HttpsTransportServer(location, this);
            Map<String, Object> transportOptions = IntrospectionSupport.extractProperties(options, "transport.");
            result.setTransportOption(transportOptions);
            return result;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
        
    }

    protected Transport createTransport(URI location, WireFormat wf) throws MalformedURLException {
        return new HttpsClientTransport(asTextWireFormat(wf), location);
    }

}

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
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.http.HttpTransportFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Factory of HTTPS based transports
 */
public class HttpsTransportFactory extends HttpTransportFactory {

    public TransportServer doBind(String brokerId, URI location) throws IOException {
        return doBind(location);
    }

    @Override
    public TransportServer doBind(URI location) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            HttpsTransportServer result = new HttpsTransportServer(location, this, SslContext.getCurrentSslContext());
            Map<String, Object> httpOptions = IntrospectionSupport.extractProperties(options, "http.");
            Map<String, Object> transportOptions = IntrospectionSupport.extractProperties(options, "transport.");
            result.setTransportOption(transportOptions);
            result.setHttpOptions(httpOptions);
            return result;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    protected Transport createTransport(URI location, WireFormat wf) throws IOException {
        // need to remove options from uri
        try {
            URI uri = URISupport.removeQuery(location);

            Map<String, String> options = new HashMap<>(URISupport.parseParameters(location));
            Map<String, Object> transportOptions = IntrospectionSupport.extractProperties(options, "transport.");
            boolean verifyHostName = true;
            if (transportOptions.containsKey("verifyHostName")) {
                verifyHostName = Boolean.parseBoolean(transportOptions.get("verifyHostName").toString());
            }

            HttpsClientTransport clientTransport = new HttpsClientTransport(asTextWireFormat(wf), uri);
            clientTransport.setVerifyHostName(verifyHostName);
            return clientTransport;
        } catch (URISyntaxException e) {
            MalformedURLException cause = new MalformedURLException("Error removing query on " + location);
            cause.initCause(e);
            throw cause;
        }
    }

    // TODO Not sure if there is a better way of removing transport.verifyHostName here?
    @Override
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        options.remove("transport.verifyHostName");
        return super.compositeConfigure(transport, format, options);
    }
}

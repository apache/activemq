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
package org.apache.activemq.transport.http;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportLoggerFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.transport.xstream.XStreamWireFormat;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpTransportFactory extends TransportFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HttpTransportFactory.class);

    @Override
    public TransportServer doBind(URI location) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            HttpTransportServer result = new HttpTransportServer(location, this);
            Map<String, Object> httpOptions = IntrospectionSupport.extractProperties(options, "http.");
            Map<String, Object> transportOptions = IntrospectionSupport.extractProperties(options, "transport.");
            result.setTransportOption(transportOptions);
            result.setHttpOptions(httpOptions);
            return result;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    protected TextWireFormat asTextWireFormat(WireFormat wireFormat) {
        if (wireFormat instanceof TextWireFormat) {
            return (TextWireFormat)wireFormat;
        }
        LOG.trace("Not created with a TextWireFormat: {}", wireFormat);
        return new XStreamWireFormat();
    }

    @Override
    protected String getDefaultWireFormatType() {
        return "xstream";
    }

    @Override
    protected Transport createTransport(URI location, WireFormat wf) throws IOException {
        TextWireFormat textWireFormat = asTextWireFormat(wf);
        // need to remove options from uri
        URI uri;
        try {
            uri = URISupport.removeQuery(location);
        } catch (URISyntaxException e) {
            MalformedURLException cause = new MalformedURLException("Error removing query on " + location);
            cause.initCause(e);
            throw cause;
        }
        return new HttpClientTransport(textWireFormat, uri);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Transport serverConfigure(Transport transport, WireFormat format, HashMap options) throws Exception {
        return compositeConfigure(transport, format, options);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        transport = super.compositeConfigure(transport, format, options);
        HttpClientTransport httpTransport = transport.narrow(HttpClientTransport.class);
        if (httpTransport != null && httpTransport.isTrace()) {
            try {
                transport = TransportLoggerFactory.getInstance().createTransportLogger(transport);
            } catch (Throwable e) {
                LOG.error("Could not create TransportLogger object for: " + TransportLoggerFactory.defaultLogWriterName + ", reason: " + e, e);
            }
        }
        boolean useInactivityMonitor = "true".equals(getOption(options, "useInactivityMonitor", "true"));
        if (useInactivityMonitor) {
            transport = new HttpInactivityMonitor(transport);
            IntrospectionSupport.setProperties(transport, options);
        }

        return transport;
    }
}

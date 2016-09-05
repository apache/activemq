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

import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportLoggerFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.http.marshallers.HttpTransportMarshaller;
import org.apache.activemq.transport.http.marshallers.HttpWireFormatMarshaller;
import org.apache.activemq.transport.http.marshallers.TextWireFormatMarshallers;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpTransportFactory extends TransportFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HttpTransportFactory.class);
    private static final String WIRE_FORMAT_XSTREAM = "xstream";
    private final String defaultWireFormatType;

    public HttpTransportFactory() {
        defaultWireFormatType = WIRE_FORMAT_XSTREAM;
    }

    public HttpTransportFactory(final String defaultWireFormatType) {
        this.defaultWireFormatType = defaultWireFormatType;
    }

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

    protected WireFormat processWireFormat(final WireFormat wireFormat) {
        return wireFormat;
    }

    @Override
    protected String getDefaultWireFormatType() {
        return defaultWireFormatType;
    }

    @Override
    protected Transport createTransport(URI location, WireFormat wf) throws IOException {
        final WireFormat wireFormat = processWireFormat(wf);
        // need to remove options from uri
        URI uri;
        try {
            uri = URISupport.removeQuery(location);
        } catch (URISyntaxException e) {
            MalformedURLException cause = new MalformedURLException("Error removing query on " + location);
            cause.initCause(e);
            throw cause;
        }
        return new HttpClientTransport(createMarshaller(wireFormat), uri);
    }

    protected HttpTransportMarshaller createMarshaller(final WireFormat wireFormat)
    {
        return wireFormat instanceof TextWireFormat ?
                TextWireFormatMarshallers.newTransportMarshaller((TextWireFormat)wireFormat) :
                new HttpWireFormatMarshaller(wireFormat);
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
        HttpClientTransport httpTransport = (HttpClientTransport)transport.narrow(HttpClientTransport.class);
        if(httpTransport != null && httpTransport.isTrace() ) {
            try {
                transport = TransportLoggerFactory.getInstance().createTransportLogger(transport);
            } catch (Throwable e) {
                LOG.error("Could not create TransportLogger object for: " + TransportLoggerFactory.defaultLogWriterName + ", reason: " + e, e);
            }
        }
        boolean useInactivityMonitor = "true".equals(getOption(options, "useInactivityMonitor", "true"));
        if (useInactivityMonitor) {
            transport = new InactivityMonitor(transport, null /* ignore wire format as no negotiation over http */);
            IntrospectionSupport.setProperties(transport, options);
        }

        return transport;
    }
}

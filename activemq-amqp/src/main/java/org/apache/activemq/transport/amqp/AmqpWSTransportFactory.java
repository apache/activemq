/*
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
package org.apache.activemq.transport.amqp;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Factory for creating WebSocket aware AMQP Transports.
 */
public class AmqpWSTransportFactory extends TransportFactory implements BrokerServiceAware {

    private BrokerService brokerService = null;

    @Override
    protected String getDefaultWireFormatType() {
        return "amqp";
    }

    @Override
    public TransportServer doBind(URI location) throws IOException {
        throw new IOException("doBind() method not implemented! No Server over WS implemented.");
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        AmqpTransportFilter amqpTransport = new AmqpTransportFilter(transport, format, brokerService);

        Map<String, Object> wireFormatOptions = IntrospectionSupport.extractProperties(options, "wireFormat.");

        IntrospectionSupport.setProperties(amqpTransport, options);
        IntrospectionSupport.setProperties(amqpTransport.getWireFormat(), wireFormatOptions);

        // Now wrap the filter with the monitor
        transport = createInactivityMonitor(amqpTransport, format);
        IntrospectionSupport.setProperties(transport, options);

        return super.compositeConfigure(transport, format, options);
    }

    /**
     * Factory method to create a new transport
     *
     * @throws IOException
     * @throws UnknownHostException
     */
    @Override
    protected Transport createTransport(URI location, WireFormat wireFormat) throws MalformedURLException, UnknownHostException, IOException {
        return new AmqpWSTransport(location, wireFormat);
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    protected Transport createInactivityMonitor(AmqpTransportFilter transport, WireFormat format) {
        AmqpInactivityMonitor monitor = new AmqpInactivityMonitor(transport, format);
        transport.setInactivityMonitor(monitor);
        return monitor;
    }
}

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
package org.apache.activemq.transport.mqtt;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLServerSocketFactory;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.SslTransportFactory;
import org.apache.activemq.transport.tcp.SslTransportServer;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * A <a href="http://mqtt.org/">MQTT</a> over SSL transport factory
 */
public class MQTTSslTransportFactory extends SslTransportFactory implements BrokerServiceAware {

    private BrokerService brokerService = null;

    protected String getDefaultWireFormatType() {
        return "mqtt";
    }

    @SuppressWarnings("rawtypes")

    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        transport = new MQTTTransportFilter(transport, format, brokerService);
        IntrospectionSupport.setProperties(transport, options);
        return super.compositeConfigure(transport, format, options);
    }

    @Override
    protected SslTransportServer createSslTransportServer(URI location, SSLServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        final SslTransportServer server = super.createSslTransportServer(location, serverSocketFactory);
        server.setAllowLinkStealing(true);
        return server;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Transport serverConfigure(Transport transport, WireFormat format, HashMap options) throws Exception {
        transport = super.serverConfigure(transport, format, options);

        MutexTransport mutex = transport.narrow(MutexTransport.class);
        if (mutex != null) {
            mutex.setSyncOnCommand(true);
        }
        return transport;
    }

    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    protected Transport createInactivityMonitor(Transport transport, WireFormat format) {
        MQTTInactivityMonitor monitor = new MQTTInactivityMonitor(transport, format);

        MQTTTransportFilter filter = transport.narrow(MQTTTransportFilter.class);
        filter.setInactivityMonitor(monitor);

        return monitor;
    }

}

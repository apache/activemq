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
package org.apache.activemq.transport.stomp;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.SslTransportFactory;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * A <a href="http://stomp.codehaus.org/">STOMP</a> over SSL transport factory
 *
 *
 */
public class StompSslTransportFactory extends SslTransportFactory implements BrokerServiceAware {

    private BrokerContext brokerContext = null;

    protected String getDefaultWireFormatType() {
        return "stomp";
    }

    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        transport = new StompTransportFilter(transport, format, brokerContext);
        IntrospectionSupport.setProperties(transport, options);
        return super.compositeConfigure(transport, format, options);
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
        this.brokerContext = brokerService.getBrokerContext();
    }

}

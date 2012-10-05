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
package org.apache.activemq.transport.amqp;

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * A <a href="http://amqp.org/">AMQP</a> transport factory
 */
public class AmqpTransportFactory extends TcpTransportFactory implements BrokerServiceAware {

    private BrokerContext brokerContext = null;

    protected String getDefaultWireFormatType() {
        return "amqp";
    }

    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        transport = new AmqpTransportFilter(transport, format, brokerContext);
        IntrospectionSupport.setProperties(transport, options);
        return super.compositeConfigure(transport, format, options);
    }

    public void setBrokerService(BrokerService brokerService) {
        this.brokerContext = brokerService.getBrokerContext();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Transport serverConfigure(Transport transport, WireFormat format, HashMap options) throws Exception {
        transport = super.serverConfigure(transport, format, options);

        // strip off the mutex transport.
        if( transport instanceof MutexTransport ) {
            transport = ((MutexTransport)transport).getNext();
        }
//        MutexTransport mutex = transport.narrow(MutexTransport.class);
//        if (mutex != null) {
//            mutex.setSyncOnCommand(true);
//        }
        return transport;
    }

//    @Override
//    protected Transport createInactivityMonitor(Transport transport, WireFormat format) {
//        AmqpInactivityMonitor monitor = new AmqpInactivityMonitor(transport, format);
//
//        AmqpTransportFilter filter = transport.narrow(AmqpTransportFilter.class);
//        filter.setInactivityMonitor(monitor);
//
//        return monitor;
//    }

    @Override
    protected boolean isUseInactivityMonitor(Transport transport) {
        return false;
    }
}

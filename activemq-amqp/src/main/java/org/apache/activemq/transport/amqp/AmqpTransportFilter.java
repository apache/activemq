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
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportListener;
import org.apache.qpid.proton.jms.InboundTransformer;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.X509Certificate;

/**
 * The AMQPTransportFilter normally sits on top of a TcpTransport that has been
 * configured with the StompWireFormat and is used to convert AMQP commands to
 * ActiveMQ commands. All of the conversion work is done by delegating to the
 * AMQPProtocolConverter
 */
public class AmqpTransportFilter extends TransportFilter implements AmqpTransport {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransportFilter.class);
    private static final Logger TRACE = LoggerFactory.getLogger(AmqpTransportFilter.class.getPackage().getName() + ".AMQPIO");
    private final AmqpProtocolConverter protocolConverter;
//    private AmqpInactivityMonitor monitor;
    private AmqpWireFormat wireFormat;

    private boolean trace;
    private String transformer = InboundTransformer.TRANSFORMER_NATIVE;

    public AmqpTransportFilter(Transport next, WireFormat wireFormat, BrokerContext brokerContext) {
        super(next);
        this.protocolConverter = new AmqpProtocolConverter(this, brokerContext);

        if (wireFormat instanceof AmqpWireFormat) {
            this.wireFormat = (AmqpWireFormat) wireFormat;
        }
    }

    public void oneway(Object o) throws IOException {
        try {
            final Command command = (Command) o;
            protocolConverter.lock.lock();
            try {
                protocolConverter.onActiveMQCommand(command);
            } finally {
                protocolConverter.lock.unlock();
            }
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public void onException(IOException error) {
        protocolConverter.lock.lock();
        try {
            protocolConverter.onAMQPException(error);
        } finally {
            protocolConverter.lock.unlock();
        }
    }

    public void sendToActiveMQ(IOException error) {
        super.onException(error);
    }

    public void onCommand(Object command) {
        try {
            if (trace) {
                TRACE.trace("Received: \n" + command);
            }
            protocolConverter.lock.lock();
            try {
                protocolConverter.onAMQPData(command);
            } finally {
                protocolConverter.lock.unlock();
            }
        } catch (IOException e) {
            handleException(e);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    public void sendToActiveMQ(Command command) {
        assert protocolConverter.lock.isHeldByCurrentThread();
        TransportListener l = transportListener;
        if (l != null) {
            l.onCommand(command);
        }
    }

    public void sendToAmqp(Object command) throws IOException {
        assert protocolConverter.lock.isHeldByCurrentThread();
        if (trace) {
            TRACE.trace("Sending: \n" + command);
        }
        Transport n = next;
        if (n != null) {
            n.oneway(command);
        }
    }

    public X509Certificate[] getPeerCertificates() {
        if (next instanceof SslTransport) {
            X509Certificate[] peerCerts = ((SslTransport) next).getPeerCertificates();
            if (trace && peerCerts != null) {
                LOG.debug("Peer Identity has been verified\n");
            }
            return peerCerts;
        }
        return null;
    }

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

//    @Override
//    public AmqpInactivityMonitor getInactivityMonitor() {
//        return monitor;
//    }
//
//    public void setInactivityMonitor(AmqpInactivityMonitor monitor) {
//        this.monitor = monitor;
//    }

    @Override
    public AmqpWireFormat getWireFormat() {
        return this.wireFormat;
    }



    public void handleException(IOException e) {
        super.onException(e);
    }


    public String getTransformer() {
        return transformer;
    }

    public void setTransformer(String transformer) {
        this.transformer = transformer;
    }
}

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

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AMQPTransportFilter normally sits on top of a TcpTransport that has been
 * configured with the AmqpWireFormat and is used to convert AMQP commands to
 * ActiveMQ commands. All of the conversion work is done by delegating to the
 * AMQPProtocolConverter
 */
public class AmqpTransportFilter extends TransportFilter implements AmqpTransport {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransportFilter.class);
    static final Logger TRACE_BYTES = LoggerFactory.getLogger(AmqpTransportFilter.class.getPackage().getName() + ".BYTES");
    public static final Logger TRACE_FRAMES = LoggerFactory.getLogger(AmqpTransportFilter.class.getPackage().getName() + ".FRAMES");
    private AmqpProtocolConverter protocolConverter;
    private AmqpWireFormat wireFormat;
    private AmqpInactivityMonitor monitor;

    private boolean trace;
    private final ReentrantLock lock = new ReentrantLock();

    public AmqpTransportFilter(Transport next, WireFormat wireFormat, BrokerService brokerService) {
        super(next);
        this.protocolConverter = new AmqpProtocolDiscriminator(this, brokerService);
        if (wireFormat instanceof AmqpWireFormat) {
            this.wireFormat = (AmqpWireFormat) wireFormat;
        }
    }

    @Override
    public void start() throws Exception {
        if (monitor != null) {
            monitor.setProtocolConverter(protocolConverter);
            monitor.startConnectChecker(getConnectAttemptTimeout());
        }
        super.start();
    }

    @Override
    public void oneway(Object o) throws IOException {
        try {
            final Command command = (Command) o;
            lock.lock();
            try {
                protocolConverter.onActiveMQCommand(command);
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public void onException(IOException error) {
        lock.lock();
        try {
            protocolConverter.onAMQPException(error);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void sendToActiveMQ(IOException error) {
        super.onException(error);
    }

    @Override
    public void onCommand(Object command) {
        try {
            if (trace) {
                TRACE_BYTES.trace("Received: \n{}", command);
            }
            lock.lock();
            try {
                protocolConverter.onAMQPData(command);
            } finally {
                lock.unlock();
            }
        } catch (IOException e) {
            handleException(e);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    @Override
    public void sendToActiveMQ(Command command) {
        assert lock.isHeldByCurrentThread();
        TransportListener l = transportListener;
        if (l != null) {
            l.onCommand(command);
        }
    }

    @Override
    public void sendToAmqp(Object command) throws IOException {
        assert lock.isHeldByCurrentThread();
        if (trace) {
            TRACE_BYTES.trace("Sending: \n{}", command);
        }
        Transport n = next;
        if (n != null) {
            n.oneway(command);
        }
    }

    @Override
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

    @Override
    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
        this.protocolConverter.updateTracer();
    }

    @Override
    public AmqpWireFormat getWireFormat() {
        return this.wireFormat;
    }

    public void handleException(IOException e) {
        super.onException(e);
    }

    @Override
    public String getTransformer() {
        return wireFormat.getTransformer();
    }

    public void setTransformer(String transformer) {
        wireFormat.setTransformer(transformer);
    }

    @Override
    public AmqpProtocolConverter getProtocolConverter() {
        return protocolConverter;
    }

    @Override
    public void setProtocolConverter(AmqpProtocolConverter protocolConverter) {
        this.protocolConverter = protocolConverter;
    }

    /**
     * @deprecated AMQP receiver configures it's prefetch via flow, remove on next release.
     */
    @Deprecated
    public void setPrefetch(int prefetch) {
    }

    public void setProducerCredit(int producerCredit) {
        wireFormat.setProducerCredit(producerCredit);
    }

    public int getProducerCredit() {
        return wireFormat.getProducerCredit();
    }

    @Override
    public void setInactivityMonitor(AmqpInactivityMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public AmqpInactivityMonitor getInactivityMonitor() {
        return monitor;
    }

    public long getConnectAttemptTimeout() {
        return wireFormat.getConnectAttemptTimeout();
    }

    public void setConnectAttemptTimeout(long connectAttemptTimeout) {
        wireFormat.setConnectAttemptTimeout(connectAttemptTimeout);
    }
}

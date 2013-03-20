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
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MQTTTransportFilter normally sits on top of a TcpTransport that has been
 * configured with the StompWireFormat and is used to convert MQTT commands to
 * ActiveMQ commands. All of the conversion work is done by delegating to the
 * MQTTProtocolConverter
 */
public class MQTTTransportFilter extends TransportFilter implements MQTTTransport {
    private static final Logger LOG = LoggerFactory.getLogger(MQTTTransportFilter.class);
    private static final Logger TRACE = LoggerFactory.getLogger(MQTTTransportFilter.class.getPackage().getName() + ".MQTTIO");
    private final MQTTProtocolConverter protocolConverter;
    private MQTTInactivityMonitor monitor;
    private MQTTWireFormat wireFormat;
    private final AtomicBoolean stopped = new AtomicBoolean();

    private boolean trace;

    public MQTTTransportFilter(Transport next, WireFormat wireFormat, BrokerContext brokerContext) {
        super(next);
        this.protocolConverter = new MQTTProtocolConverter(this, brokerContext);

        if (wireFormat instanceof MQTTWireFormat) {
            this.wireFormat = (MQTTWireFormat) wireFormat;
        }
    }

    @Override
    public void oneway(Object o) throws IOException {
        try {
            final Command command = (Command) o;
            protocolConverter.onActiveMQCommand(command);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public void onCommand(Object command) {
        try {
            if (trace) {
                TRACE.trace("Received: \n" + command);
            }

            protocolConverter.onMQTTCommand((MQTTFrame) command);
        } catch (IOException e) {
            handleException(e);
        } catch (JMSException e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    @Override
    public void sendToActiveMQ(Command command) {
        TransportListener l = transportListener;
        if (l != null) {
            l.onCommand(command);
        }
    }

    @Override
    public void sendToMQTT(MQTTFrame command) throws IOException {
        if( !stopped.get() ) {
            if (trace) {
                TRACE.trace("Sending: \n" + command);
            }
            Transport n = next;
            if (n != null) {
                n.oneway(command);
            }
        }
    }

    @Override
    public void stop() throws Exception {
        if( stopped.compareAndSet(false, true) ) {
            super.stop();
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

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    @Override
    public MQTTInactivityMonitor getInactivityMonitor() {
        return monitor;
    }

    public void setInactivityMonitor(MQTTInactivityMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public MQTTWireFormat getWireFormat() {
        return this.wireFormat;
    }

    public void handleException(IOException e) {
        protocolConverter.onTransportError();
        super.onException(e);
    }

    public long getDefaultKeepAlive() {
        return protocolConverter != null ? protocolConverter.getDefaultKeepAlive() : -1;
    }

    public void setDefaultKeepAlive(long defaultHeartBeat) {
        protocolConverter.setDefaultKeepAlive(defaultHeartBeat);
    }

    public int getActiveMQSubscriptionPrefetch() {
        return protocolConverter.getActiveMQSubscriptionPrefetch();
    }

    /**
     * set the default prefetch size when mapping the MQTT subscription to an ActiveMQ one
     * The default = 1
     * @param activeMQSubscriptionPrefetch set the prefetch for the corresponding ActiveMQ subscription
     */
    public void setActiveMQSubscriptionPrefetch(int activeMQSubscriptionPrefetch) {
        protocolConverter.setActiveMQSubscriptionPrefetch(activeMQSubscriptionPrefetch);
    }
}

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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.nio.NIOSSLTransport;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.mqtt.codec.CONNACK;
import org.fusesource.mqtt.codec.CONNECT;
import org.fusesource.mqtt.codec.DISCONNECT;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.fusesource.mqtt.codec.PINGREQ;
import org.fusesource.mqtt.codec.PINGRESP;
import org.fusesource.mqtt.codec.PUBACK;
import org.fusesource.mqtt.codec.PUBCOMP;
import org.fusesource.mqtt.codec.PUBLISH;
import org.fusesource.mqtt.codec.PUBREC;
import org.fusesource.mqtt.codec.PUBREL;
import org.fusesource.mqtt.codec.SUBACK;
import org.fusesource.mqtt.codec.SUBSCRIBE;
import org.fusesource.mqtt.codec.UNSUBSCRIBE;
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
    private final Object sendLock = new Object();

    public MQTTTransportFilter(Transport next, WireFormat wireFormat, BrokerService brokerService) {
        super(next);
        this.protocolConverter = new MQTTProtocolConverter(this, brokerService);

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
            MQTTFrame frame = (MQTTFrame) command;
            if (trace) {
                TRACE.trace("Received: " + toString(frame));
            }
            protocolConverter.onMQTTCommand(frame);
        } catch (IOException e) {
            onException(e);
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
                TRACE.trace("Sending : " + toString(command));
            }
            Transport n = next;
            if (n != null) {
                // sync access to underlying transport buffer
                synchronized (sendLock) {
                    n.oneway(command);
                }
            }
        }
    }

    static private String toString(MQTTFrame frame) {
        if( frame == null )
            return null;
        try {
            switch (frame.messageType()) {
                case PINGREQ.TYPE: return new PINGREQ().decode(frame).toString();
                case PINGRESP.TYPE: return new PINGRESP().decode(frame).toString();
                case CONNECT.TYPE: return new CONNECT().decode(frame).toString();
                case DISCONNECT.TYPE: return new DISCONNECT().decode(frame).toString();
                case SUBSCRIBE.TYPE: return new SUBSCRIBE().decode(frame).toString();
                case UNSUBSCRIBE.TYPE: return new UNSUBSCRIBE().decode(frame).toString();
                case PUBLISH.TYPE: return new PUBLISH().decode(frame).toString();
                case PUBACK.TYPE: return new PUBACK().decode(frame).toString();
                case PUBREC.TYPE: return new PUBREC().decode(frame).toString();
                case PUBREL.TYPE: return new PUBREL().decode(frame).toString();
                case PUBCOMP.TYPE: return new PUBCOMP().decode(frame).toString();
                case CONNACK.TYPE: return new CONNACK().decode(frame).toString();
                case SUBACK.TYPE: return new SUBACK().decode(frame).toString();
                default: return frame.toString();
            }
        } catch (Throwable e) {
            LOG.warn(e.getMessage(), e);
            return frame.toString();
        }
    }

    @Override
    public void start() throws Exception {
        if (monitor != null) {
            monitor.startConnectChecker(getConnectAttemptTimeout());
        }
        super.start();
    }

    @Override
    public void stop() throws Exception {
        if (stopped.compareAndSet(false, true)) {
            super.stop();
        }
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        X509Certificate[] peerCerts = null;
        if (next instanceof SslTransport) {
            peerCerts = ((SslTransport) next).getPeerCertificates();
        }
        if (next instanceof  NIOSSLTransport) {
            peerCerts = ((NIOSSLTransport)next).getPeerCertificates();
        }
        if (trace && peerCerts != null) {
            LOG.debug("Peer Identity has been verified\n");
        }
        return peerCerts;
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

    @Override
    public void onException(IOException error) {
        protocolConverter.onTransportError();
        super.onException(error);
    }

    public long getDefaultKeepAlive() {
        return protocolConverter != null ? protocolConverter.getDefaultKeepAlive() : -1;
    }

    public void setDefaultKeepAlive(long defaultHeartBeat) {
        protocolConverter.setDefaultKeepAlive(defaultHeartBeat);
    }

    /**
     * @return the timeout value used to fail a connection if no CONNECT frame read.
     */
    public long getConnectAttemptTimeout() {
        return wireFormat.getConnectAttemptTimeout();
    }

    /**
     * Sets the timeout value used to fail a connection if no CONNECT frame is read
     * in the given interval.
     *
     * @param connectTimeout
     *        the connection frame received timeout value.
     */
    public void setConnectAttemptTimeout(long connectTimeout) {
        wireFormat.setConnectAttemptTimeout(connectTimeout);
    }

    public boolean getPublishDollarTopics() {
        return protocolConverter != null && protocolConverter.getPublishDollarTopics();
    }

    public void setPublishDollarTopics(boolean publishDollarTopics) {
        protocolConverter.setPublishDollarTopics(publishDollarTopics);
    }

    public String getSubscriptionStrategy() {
        return protocolConverter != null ? protocolConverter.getSubscriptionStrategy() : "default";
    }

    public void setSubscriptionStrategy(String name) {
        protocolConverter.setSubscriptionStrategy(name);
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

    /**
     * @return the maximum number of bytes a single MQTT message frame is allowed to be.
     */
    public int getMaxFrameSize() {
        return wireFormat.getMaxFrameSize();
    }

    /**
     * Sets the maximum frame size for an incoming MQTT frame.  The protocl limit is
     * 256 megabytes and this value cannot be set higher.
     *
     * @param maxFrameSize
     *        the maximum allowed frame size for a single MQTT frame.
     */
    public void setMaxFrameSize(int maxFrameSize) {
        wireFormat.setMaxFrameSize(maxFrameSize);
    }

    @Override
    public void setPeerCertificates(X509Certificate[] certificates) {}
}

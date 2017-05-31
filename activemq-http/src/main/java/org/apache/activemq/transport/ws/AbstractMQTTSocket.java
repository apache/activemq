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
package org.apache.activemq.transport.ws;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.command.Command;
import org.apache.activemq.jms.pool.IntrospectionSupport;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.transport.mqtt.MQTTInactivityMonitor;
import org.apache.activemq.transport.mqtt.MQTTProtocolConverter;
import org.apache.activemq.transport.mqtt.MQTTTransport;
import org.apache.activemq.transport.mqtt.MQTTWireFormat;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.fusesource.mqtt.codec.MQTTFrame;

public abstract class AbstractMQTTSocket extends TransportSupport implements MQTTTransport, BrokerServiceAware {

    protected ReentrantLock protocolLock = new ReentrantLock();
    protected volatile MQTTProtocolConverter protocolConverter = null;
    protected MQTTWireFormat wireFormat = new MQTTWireFormat();
    protected final MQTTInactivityMonitor mqttInactivityMonitor = new MQTTInactivityMonitor(this, wireFormat);
    protected final CountDownLatch socketTransportStarted = new CountDownLatch(1);
    protected BrokerService brokerService;
    protected volatile int receiveCounter;
    protected final String remoteAddress;
    protected X509Certificate[] peerCertificates;
    private Map<String, Object> transportOptions;

    public AbstractMQTTSocket(String remoteAddress) {
        super();
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void oneway(Object command) throws IOException {
        protocolLock.lock();
        try {
            getProtocolConverter().onActiveMQCommand((Command)command);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        } finally {
            protocolLock.unlock();
        }
    }

    @Override
    public void sendToActiveMQ(Command command) {
        protocolLock.lock();
        try {
            doConsume(command);
        } finally {
            protocolLock.unlock();
        }
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        mqttInactivityMonitor.stop();
        handleStopped();
    }

    @Override
    protected void doStart() throws Exception {
        socketTransportStarted.countDown();
        mqttInactivityMonitor.setTransportListener(getTransportListener());
        mqttInactivityMonitor.startConnectChecker(wireFormat.getConnectAttemptTimeout());
    }

    //----- Abstract methods for subclasses to implement ---------------------//

    @Override
    public abstract void sendToMQTT(MQTTFrame command) throws IOException;

    /**
     * Called when the transport is stopping to allow the dervied classes
     * a chance to close WebSocket resources.
     *
     * @throws IOException if an error occurs during the stop.
     */
    public abstract void handleStopped() throws IOException;

    //----- Accessor methods -------------------------------------------------//

    @Override
    public MQTTInactivityMonitor getInactivityMonitor() {
        return mqttInactivityMonitor;
    }

    @Override
    public MQTTWireFormat getWireFormat() {
        return wireFormat;
    }

    @Override
    public String getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public int getReceiveCounter() {
        return receiveCounter;
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        return peerCertificates;
    }

    @Override
    public void setPeerCertificates(X509Certificate[] certificates) {
        this.peerCertificates = certificates;
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    //----- Internal support methods -----------------------------------------//

    protected MQTTProtocolConverter getProtocolConverter() {
        if (protocolConverter == null) {
            synchronized(this) {
                if (protocolConverter == null) {
                    protocolConverter = new MQTTProtocolConverter(this, brokerService);
                    IntrospectionSupport.setProperties(protocolConverter, transportOptions);
                }
            }
        }
        return protocolConverter;
    }

    protected boolean transportStartedAtLeastOnce() {
        return socketTransportStarted.getCount() == 0;
    }

    public void setTransportOptions(Map<String, Object> transportOptions) {
        this.transportOptions = transportOptions;
    }
}

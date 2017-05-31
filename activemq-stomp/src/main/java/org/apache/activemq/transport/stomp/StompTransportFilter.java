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

import java.io.IOException;

import javax.jms.JMSException;

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The StompTransportFilter normally sits on top of a TcpTransport that has been
 * configured with the StompWireFormat and is used to convert STOMP commands to
 * ActiveMQ commands. All of the conversion work is done by delegating to the
 * ProtocolConverter.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class StompTransportFilter extends TransportFilter implements StompTransport {

    private static final Logger TRACE = LoggerFactory.getLogger(StompTransportFilter.class.getPackage().getName() + ".StompIO");

    private final ProtocolConverter protocolConverter;
    private StompInactivityMonitor monitor;
    private StompWireFormat wireFormat;

    private boolean trace;

    public StompTransportFilter(Transport next, WireFormat wireFormat, BrokerContext brokerContext) {
        super(next);
        this.protocolConverter = new ProtocolConverter(this, brokerContext);

        if (wireFormat instanceof StompWireFormat) {
            this.wireFormat = (StompWireFormat) wireFormat;
        }
    }

    @Override
    public void start() throws Exception {
        if (monitor != null) {
            monitor.startConnectCheckTask(getConnectAttemptTimeout());
        }
        super.start();
    }

    @Override
    public void oneway(Object o) throws IOException {
        try {
            final Command command = (Command) o;
            protocolConverter.onActiveMQCommand(command);
        } catch (JMSException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public void onCommand(Object command) {
        try {
            if (trace) {
                TRACE.trace("Received: \n" + command);
            }

            protocolConverter.onStompCommand((StompFrame) command);
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
    public void sendToStomp(StompFrame command) throws IOException {
        if (trace) {
            TRACE.trace("Sending: \n" + command);
        }
        Transport n = next;
        if (n != null) {
            n.oneway(command);
        }
    }

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    @Override
    public StompInactivityMonitor getInactivityMonitor() {
        return monitor;
    }

    public void setInactivityMonitor(StompInactivityMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public StompWireFormat getWireFormat() {
        return this.wireFormat;
    }

    public String getDefaultHeartBeat() {
        return protocolConverter != null ? protocolConverter.getDefaultHeartBeat() : null;
    }

    public void setDefaultHeartBeat(String defaultHeartBeat) {
        protocolConverter.setDefaultHeartBeat(defaultHeartBeat);
    }

    /**
     * Returns the currently configured Read check grace period multiplier.
     *
     * @return the hbGracePeriodMultiplier
     */
    public float getHbGracePeriodMultiplier() {
        return protocolConverter != null ? protocolConverter.getHbGracePeriodMultiplier() : null;
    }

    /**
     * Sets the read check grace period multiplier.  New CONNECT frames that indicate a heart beat
     * value with a read check interval will have that value multiplied by this value to add a
     * grace period before the connection is considered invalid.  By default this value is set to
     * zero and no grace period is given.  When set the value must be larger than 1.0 or it will
     * be ignored.
     *
     * @param hbGracePeriodMultiplier the hbGracePeriodMultiplier to set
     */
    public void setHbGracePeriodMultiplier(float hbGracePeriodMultiplier) {
        if (hbGracePeriodMultiplier > 1.0f) {
            protocolConverter.setHbGracePeriodMultiplier(hbGracePeriodMultiplier);
        }
    }

    /**
     * Sets the maximum number of bytes that the data portion of a STOMP frame is allowed to
     * be, any incoming STOMP frame with a data section larger than this value will receive
     * an error response.
     *
     * @param maxDataLength
     *        size in bytes of the maximum data portion of a STOMP frame.
     */
    public void setMaxDataLength(int maxDataLength) {
        wireFormat.setMaxDataLength(maxDataLength);
    }

    public int getMaxDataLength() {
        return wireFormat.getMaxDataLength();
    }

    public void setMaxFrameSize(int maxFrameSize) {
        wireFormat.setMaxFrameSize(maxFrameSize);
    }

    public long getMaxFrameSize() {
        return wireFormat.getMaxFrameSize();
    }

    public long getConnectAttemptTimeout() {
        return wireFormat.getConnectionAttemptTimeout();
    }

    public void setConnectAttemptTimeout(long timeout) {
        wireFormat.setConnectionAttemptTimeout(timeout);
    }
}

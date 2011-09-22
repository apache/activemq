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
package org.apache.activemq.transport;

import java.io.IOException;

import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to make sure that commands are arriving periodically from the peer of
 * the transport.
 */
public class InactivityMonitor extends AbstractInactivityMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(InactivityMonitor.class);

    private WireFormatInfo localWireFormatInfo;
    private WireFormatInfo remoteWireFormatInfo;

    private boolean ignoreRemoteWireFormat = false;
    private boolean ignoreAllWireFormatInfo = false;

    public InactivityMonitor(Transport next, WireFormat wireFormat) {
        super(next, wireFormat);
        if (this.wireFormat == null) {
            this.ignoreAllWireFormatInfo = true;
        }
    }

    protected void processInboundWireFormatInfo(WireFormatInfo info) throws IOException {
        IOException error = null;
        remoteWireFormatInfo = info;
        try {
            startMonitorThreads();
        } catch (IOException e) {
            error = e;
        }
        if (error != null) {
            onException(error);
        }
    }

    protected void processOutboundWireFormatInfo(WireFormatInfo info) throws IOException{
        localWireFormatInfo = info;
        startMonitorThreads();
    }

    @Override
    protected synchronized void startMonitorThreads() throws IOException {
        if (isMonitorStarted()) {
            return;
        }

        long readCheckTime = getReadCheckTime();

        if (readCheckTime > 0) {
            setWriteCheckTime(readCheckTime>3 ? readCheckTime/3 : readCheckTime);
        }

        super.startMonitorThreads();
    }

    @Override
    protected boolean configuredOk() throws IOException {
        boolean configured = false;
        if (ignoreAllWireFormatInfo) {
            configured = true;
        } else if (localWireFormatInfo != null && remoteWireFormatInfo != null) {
            if (!ignoreRemoteWireFormat) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Using min of local: " + localWireFormatInfo + " and remote: " + remoteWireFormatInfo);
                }

                long readCheckTime = Math.min(localWireFormatInfo.getMaxInactivityDuration(), remoteWireFormatInfo.getMaxInactivityDuration());
                long writeCheckTime = readCheckTime>3 ? readCheckTime/3 : readCheckTime;

                setReadCheckTime(readCheckTime);
                setInitialDelayTime(Math.min(localWireFormatInfo.getMaxInactivityDurationInitalDelay(), remoteWireFormatInfo.getMaxInactivityDurationInitalDelay()));
                setWriteCheckTime(writeCheckTime);

            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Using local: " + localWireFormatInfo);
                }

                long readCheckTime = localWireFormatInfo.getMaxInactivityDuration();
                long writeCheckTime = readCheckTime>3 ? readCheckTime/3 : readCheckTime;

                setReadCheckTime(readCheckTime);
                setInitialDelayTime(localWireFormatInfo.getMaxInactivityDurationInitalDelay());
                setWriteCheckTime(writeCheckTime);
            }
            configured = true;
        }

        return configured;
    }

    public boolean isIgnoreAllWireFormatInfo() {
        return ignoreAllWireFormatInfo;
    }

    public void setIgnoreAllWireFormatInfo(boolean ignoreAllWireFormatInfo) {
        this.ignoreAllWireFormatInfo = ignoreAllWireFormatInfo;
    }

    public boolean isIgnoreRemoteWireFormat() {
        return ignoreRemoteWireFormat;
    }

    public void setIgnoreRemoteWireFormat(boolean ignoreRemoteWireFormat) {
        this.ignoreRemoteWireFormat = ignoreRemoteWireFormat;
    }
}

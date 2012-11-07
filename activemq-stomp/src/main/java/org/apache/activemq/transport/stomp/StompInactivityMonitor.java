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

import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.transport.AbstractInactivityMonitor;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to make sure that commands are arriving periodically from the peer of
 * the transport.
 */
public class StompInactivityMonitor extends AbstractInactivityMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(StompInactivityMonitor.class);

    private boolean isConfigured = false;

    public StompInactivityMonitor(Transport next, WireFormat wireFormat) {
        super(next, wireFormat);
    }

    public void startMonitoring() throws IOException {
        this.isConfigured = true;
        this.startMonitorThreads();
    }

    @Override
    protected void processInboundWireFormatInfo(WireFormatInfo info) throws IOException {
    }

    @Override
    protected void processOutboundWireFormatInfo(WireFormatInfo info) throws IOException{
    }

    @Override
    protected boolean configuredOk() throws IOException {

        if (!isConfigured) {
            return false;
        }

        LOG.debug("Stomp Inactivity Monitor read check: " + getReadCheckTime() +
                  ", write check: " + getWriteCheckTime());

        if (this.getReadCheckTime() >= 0 && this.getWriteCheckTime() >= 0) {
            return true;
        }

        return false;
    }
}

/*
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
package org.apache.activemq.transport.http;

import java.io.IOException;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inactivity Monitor specialization for use with HTTP based transports.
 */
public class HttpInactivityMonitor extends InactivityMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(HttpInactivityMonitor.class);

    /**
     * @param next
     *      The next Transport in the filter chain.
     */
    public HttpInactivityMonitor(Transport next) {
        super(next, null);
    }

    @Override
    public void onCommand(Object command) {
        if (command.getClass() == ConnectionInfo.class || command.getClass() == BrokerInfo.class) {
            synchronized (this) {
                try {
                    LOG.trace("Connection {} attempted on HTTP based transport: {}", command, this);
                    processInboundWireFormatInfo(null);
                } catch (IOException e) {
                    onException(e);
                }
            }
        }

        super.onCommand(command);
    }
}

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
package org.apache.activemq.network;

import java.io.IOException;

import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A demand forwarding bridge which works with multicast style transports where
 * a single Transport could be communicating with multiple remote brokers
 * 
 * @org.apache.xbean.XBean
 * 
 * 
 */
public class CompositeDemandForwardingBridge extends DemandForwardingBridgeSupport {
    private static final Logger LOG = LoggerFactory.getLogger(CompositeDemandForwardingBridge.class);

    public CompositeDemandForwardingBridge(NetworkBridgeConfiguration configuration, Transport localBroker,
                                           Transport remoteBroker) {
        super(configuration, localBroker, remoteBroker);
        remoteBrokerName = remoteBroker.toString();
    }

    protected void serviceLocalBrokerInfo(Command command) throws InterruptedException {
        // TODO is there much we can do here?
    }
}

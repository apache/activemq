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

import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Endpoint;
import org.apache.activemq.command.NetworkBridgeFilter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.ServiceSupport;

/**
 * A demand forwarding bridge which works with multicast style transports where
 * a single Transport could be communicating with multiple remote brokers
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class CompositeDemandForwardingBridge extends DemandForwardingBridgeSupport {

    protected final BrokerId remoteBrokerPath[] = new BrokerId[] {null};
    protected Object brokerInfoMutex = new Object();

    public CompositeDemandForwardingBridge(NetworkBridgeConfiguration configuration, Transport localBroker,
                                           Transport remoteBroker) {
        super(configuration, localBroker, remoteBroker);
        remoteBrokerName = remoteBroker.toString();
        remoteBrokerNameKnownLatch.countDown();
    }

    protected void serviceRemoteBrokerInfo(Command command) throws IOException {
        synchronized (brokerInfoMutex) {
            BrokerInfo remoteBrokerInfo = (BrokerInfo)command;
            BrokerId remoteBrokerId = remoteBrokerInfo.getBrokerId();

            // lets associate the incoming endpoint with a broker ID so we can
            // refer to it later
            Endpoint from = command.getFrom();
            if (from == null) {
                log.warn("Incoming command does not have a from endpoint: " + command);
            } else {
                from.setBrokerInfo(remoteBrokerInfo);
            }
            if (localBrokerId != null) {
                if (localBrokerId.equals(remoteBrokerId)) {
                    log.info("Disconnecting loop back connection.");
                    // waitStarted();
                    ServiceSupport.dispose(this);
                }
            }
            if (!disposed) {
                triggerLocalStartBridge();
            }
        }
    }

    protected void addRemoteBrokerToBrokerPath(ConsumerInfo info) throws IOException {
        info.setBrokerPath(appendToBrokerPath(info.getBrokerPath(), getFromBrokerId(info)));
    }

    /**
     * Returns the broker ID that the command came from
     */
    protected BrokerId getFromBrokerId(Command command) throws IOException {
        BrokerId answer = null;
        Endpoint from = command.getFrom();
        if (from == null) {
            log.warn("Incoming command does not have a from endpoint: " + command);
        } else {
            answer = from.getBrokerId();
        }
        if (answer != null) {
            return answer;
        } else {
            throw new IOException("No broker ID is available for endpoint: " + from + " from command: "
                                  + command);
        }
    }

    protected void serviceLocalBrokerInfo(Command command) throws InterruptedException {
        // TODO is there much we can do here?
    }

    protected NetworkBridgeFilter createNetworkBridgeFilter(ConsumerInfo info) throws IOException {
        return new NetworkBridgeFilter(getFromBrokerId(info), configuration.getNetworkTTL());
    }

    protected BrokerId[] getRemoteBrokerPath() {
        return remoteBrokerPath;
    }

}

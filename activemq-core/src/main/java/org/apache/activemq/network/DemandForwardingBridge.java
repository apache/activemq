/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.network;

import java.io.IOException;

import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.NetworkBridgeFilter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.ServiceSupport;

/**
 * Forwards messages from the local broker to the remote broker based on demand.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class DemandForwardingBridge extends DemandForwardingBridgeSupport {

    protected final BrokerId remoteBrokerPath[] = new BrokerId[] { null };
    protected Object brokerInfoMutex = new Object();
    protected BrokerId remoteBrokerId;

    public DemandForwardingBridge(Transport localBroker,Transport remoteBroker){
        super(localBroker, remoteBroker);
    }

    protected void serviceRemoteBrokerInfo(Command command) throws IOException {
        synchronized(brokerInfoMutex){
            BrokerInfo remoteBrokerInfo=(BrokerInfo) command;
            remoteBrokerId=remoteBrokerInfo.getBrokerId();
            remoteBrokerPath[0]=remoteBrokerId;
            remoteBrokerName=remoteBrokerInfo.getBrokerName();
            if(localBrokerId!=null){
                if(localBrokerId.equals(remoteBrokerId)){
                    log.info("Disconnecting loop back connection.");
                    //waitStarted();
                    ServiceSupport.dispose(this);
                }
            }
            if (!disposed){
                triggerLocalStartBridge();
            }
        }
    }

    protected void addRemoteBrokerToBrokerPath(ConsumerInfo info) {
        info.setBrokerPath(appendToBrokerPath(info.getBrokerPath(),remoteBrokerPath));
    }

    protected void serviceLocalBrokerInfo(Command command) throws InterruptedException {
        synchronized(brokerInfoMutex){
            localBrokerId=((BrokerInfo) command).getBrokerId();
            localBrokerPath[0]=localBrokerId;
            if(remoteBrokerId!=null){
                if(remoteBrokerId.equals(localBrokerId)){
                    log.info("Disconnecting loop back connection.");
                    waitStarted();
                    ServiceSupport.dispose(this);
                }
            }
        }
    }

    protected NetworkBridgeFilter createNetworkBridgeFilter() {
        return new NetworkBridgeFilter(remoteBrokerPath[0], networkTTL);
    }

}

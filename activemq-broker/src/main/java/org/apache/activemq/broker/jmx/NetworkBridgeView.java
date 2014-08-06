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
package org.apache.activemq.broker.jmx;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.network.NetworkBridge;

public class NetworkBridgeView implements NetworkBridgeViewMBean {

    private final NetworkBridge bridge;
    private boolean createByDuplex = false;
    private List<NetworkDestinationView> networkDestinationViewList = new CopyOnWriteArrayList<NetworkDestinationView>();

    public NetworkBridgeView(NetworkBridge bridge) {
        this.bridge = bridge;
    }

    public void start() throws Exception {
        bridge.start();
    }

    public void stop() throws Exception {
        bridge.stop();
    }

    public String getLocalAddress() {
        return bridge.getLocalAddress();
    }

    public String getRemoteAddress() {
        return bridge.getRemoteAddress();
    }

    public String getRemoteBrokerName() {
        return bridge.getRemoteBrokerName();
    }


    public String getRemoteBrokerId() {
        return bridge.getRemoteBrokerId();
    }

    public String getLocalBrokerName() {
        return bridge.getLocalBrokerName();
    }

    public long getEnqueueCounter() {
        return bridge.getEnqueueCounter();
    }

    public long getDequeueCounter() {
        return bridge.getDequeueCounter();
    }

    public boolean isCreatedByDuplex() {
        return createByDuplex;
    }

    public void setCreateByDuplex(boolean createByDuplex) {
        this.createByDuplex = createByDuplex;
    }

    public void resetStats(){
        bridge.resetStats();
        for (NetworkDestinationView networkDestinationView:networkDestinationViewList){
            networkDestinationView.resetStats();
        }
    }

    public void addNetworkDestinationView(NetworkDestinationView networkDestinationView){
        networkDestinationViewList.add(networkDestinationView);
    }

    public void removeNetworkDestinationView(NetworkDestinationView networkDestinationView){
        networkDestinationViewList.remove(networkDestinationView);
    }
}

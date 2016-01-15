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

    @Override
    public void start() throws Exception {
        bridge.start();
    }

    @Override
    public void stop() throws Exception {
        bridge.stop();
    }

    @Override
    public String getLocalAddress() {
        return bridge.getLocalAddress();
    }

    @Override
    public String getRemoteAddress() {
        return bridge.getRemoteAddress();
    }

    @Override
    public String getRemoteBrokerName() {
        return bridge.getRemoteBrokerName();
    }


    @Override
    public String getRemoteBrokerId() {
        return bridge.getRemoteBrokerId();
    }

    @Override
    public String getLocalBrokerName() {
        return bridge.getLocalBrokerName();
    }

    @Override
    public long getEnqueueCounter() {
        return bridge.getEnqueueCounter();
    }

    @Override
    public long getDequeueCounter() {
        return bridge.getDequeueCounter();
    }

    @Override
    public long getReceivedCounter() {
        return bridge.getNetworkBridgeStatistics().getReceivedCount().getCount();
    }

    @Override
    public boolean isCreatedByDuplex() {
        return createByDuplex;
    }

    public void setCreateByDuplex(boolean createByDuplex) {
        this.createByDuplex = createByDuplex;
    }

    @Override
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

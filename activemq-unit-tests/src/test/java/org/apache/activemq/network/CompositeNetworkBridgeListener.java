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
package org.apache.activemq.network;

import java.util.Arrays;
import java.util.List;

import org.apache.activemq.command.Message;

public class CompositeNetworkBridgeListener implements NetworkBridgeListener {
    private final List<NetworkBridgeListener> listeners;

    public CompositeNetworkBridgeListener(NetworkBridgeListener... wrapped) {
        this.listeners = Arrays.asList(wrapped);
    }

    @Override
    public void bridgeFailed() {
        for (NetworkBridgeListener listener : listeners) {
            listener.bridgeFailed();
        }
    }

    @Override
    public void onStart(NetworkBridge bridge) {
        for (NetworkBridgeListener listener : listeners) {
            listener.onStart(bridge);
        }
    }

    @Override
    public void onStop(NetworkBridge bridge) {
        for (NetworkBridgeListener listener : listeners) {
            listener.onStop(bridge);
        }
    }

    @Override
    public void onOutboundMessage(NetworkBridge bridge, Message message) {
        for (NetworkBridgeListener listener : listeners) {
            listener.onOutboundMessage(bridge, message);
        }
    }

    @Override
    public void onInboundMessage(NetworkBridge bridge, Message message) {
        for (NetworkBridgeListener listener : listeners) {
            listener.onInboundMessage(bridge, message);
        }
    }
}

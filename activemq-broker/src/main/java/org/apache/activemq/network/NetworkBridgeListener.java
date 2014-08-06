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

import org.apache.activemq.command.Message;

/**
 * called when a bridge fails
 * 
 * 
 */
public interface NetworkBridgeListener {

    /**
     * called when the transport fails
     */
    void bridgeFailed();

    /**
     * called after the bridge is started.
     */
    void onStart(NetworkBridge bridge);

    /**
     * called before the bridge is stopped.
     */
    void onStop(NetworkBridge bridge);

    /**
     * Called when message forwarded over the network
     * @param bridge
     * @param message
     */
    void onOutboundMessage (NetworkBridge bridge,Message message);

    /**
     * Called for when a message arrives over the network
     * @param bridge
     * @param message
     */
    void onInboundMessage (NetworkBridge bridge,Message message);

}

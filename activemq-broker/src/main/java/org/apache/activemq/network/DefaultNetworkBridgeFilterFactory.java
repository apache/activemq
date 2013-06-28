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

import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.NetworkBridgeFilter;

/**
 * implement default behavior, filter that will not allow re-send to origin
 * based on brokerPath and which respects networkTTL
 *
 *  @org.apache.xbean.XBean
 */
public class DefaultNetworkBridgeFilterFactory implements NetworkBridgeFilterFactory {
    public NetworkBridgeFilter create(ConsumerInfo info, BrokerId[] remoteBrokerPath, int messageTTL, int consumerTTL) {
        return new NetworkBridgeFilter(info, remoteBrokerPath[0], messageTTL, consumerTTL);
    }
}

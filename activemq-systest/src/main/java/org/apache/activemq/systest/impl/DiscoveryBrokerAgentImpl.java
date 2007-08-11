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
package org.apache.activemq.systest.impl;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.systest.BrokerAgent;

/**
 * A broker using discovery
 * 
 * @version $Revision: 1.1 $
 */
public class DiscoveryBrokerAgentImpl extends BrokerAgentImpl {

    public DiscoveryBrokerAgentImpl() throws Exception {
    }

    public void connectTo(BrokerAgent remoteBroker) throws Exception {
        NetworkConnector connector = getBroker().addNetworkConnector("rendezvous");
        if (isStarted()) {
            connector.start();
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setBrokerName(getBrokerName());
        answer.setPersistent(isPersistent());
        answer.addConnector("discovery:" + getConnectionURI());
        return answer;
    }

}

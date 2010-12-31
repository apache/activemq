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
package org.apache.activemq.broker.advisory;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import java.net.URI;

public class AdvisoryDuplexNetworkBridgeTest extends AdvisoryNetworkBridgeTest {

    @Override
    public void createBroker1() throws Exception {
        broker1 = new BrokerService();
        broker1.setBrokerName("broker1");
        broker1.addConnector("tcp://localhost:61617");
        broker1.setUseJmx(false);
        broker1.setPersistent(false);
        broker1.start();
        broker1.waitUntilStarted();
    }

    @Override
    public void createBroker2() throws Exception {
        broker2 = BrokerFactory.createBroker(new URI("xbean:org/apache/activemq/network/duplexLocalBroker.xml"));
        broker2.start();
        broker2.waitUntilStarted();
    }
}

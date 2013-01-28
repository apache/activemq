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
package org.apache.activemq.usecases;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkConnector;

public class TwoBrokerNetworkConnectorWildcardDynamicallyIncludedDestinationTest extends AbstractTwoBrokerNetworkConnectorWildcardIncludedDestinationTestSupport {
	
    protected void addIncludedDestination(NetworkConnector nc) {
        nc.addExcludedDestination(ActiveMQDestination.createDestination("local.>", ActiveMQDestination.QUEUE_TYPE));
        nc.addExcludedDestination(ActiveMQDestination.createDestination("local.>", ActiveMQDestination.TOPIC_TYPE));
        nc.addExcludedDestination(ActiveMQDestination.createDestination("Consumer.*.local.>", ActiveMQDestination.QUEUE_TYPE));
        nc.addDynamicallyIncludedDestination(ActiveMQDestination.createDestination("global.>", ActiveMQDestination.QUEUE_TYPE));
        nc.addDynamicallyIncludedDestination(ActiveMQDestination.createDestination("global.>", ActiveMQDestination.TOPIC_TYPE));
        nc.addDynamicallyIncludedDestination(ActiveMQDestination.createDestination("Consumer.*.global.>", ActiveMQDestination.QUEUE_TYPE));
    }
    
}

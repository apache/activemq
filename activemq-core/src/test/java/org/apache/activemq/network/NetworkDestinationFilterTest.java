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

import junit.framework.TestCase;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTopic;

import java.util.ArrayList;
import java.util.List;

public class NetworkDestinationFilterTest extends TestCase {

    public void testFilter() throws Exception {
        NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
        assertEquals(AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX + ">", config.getDestinationFilter());
        List<ActiveMQDestination> dests = new ArrayList<ActiveMQDestination>();
        config.setDynamicallyIncludedDestinations(dests);
        assertEquals(AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX + ">", config.getDestinationFilter());
        dests.add(new ActiveMQQueue("TEST.>"));
        dests.add(new ActiveMQTopic("TEST.>"));
        dests.add(new ActiveMQTempQueue("TEST.>"));
        String prefix = AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX;
        assertEquals(prefix + "Queue.TEST.>," + prefix + "Topic.TEST.>", config.getDestinationFilter());
    }


}

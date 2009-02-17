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
package org.apache.activemq.broker.policy;

import javax.jms.Destination;
import javax.jms.Message;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * @version $Revision$
 */
public class NoConsumerDeadLetterTest extends DeadLetterTestSupport {

    // lets disable the inapplicable tests
    public void testDurableQueueMessage() throws Exception {
    }

    public void testDurableTopicMessage() throws Exception {
    }

    protected void doTest() throws Exception {
        makeDlqConsumer();
        sendMessages();

        for (int i = 0; i < messageCount; i++) {
            Message msg = dlqConsumer.receive(1000);
            assertNotNull("Should be a message for loop: " + i, msg);
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();

        PolicyEntry policy = new PolicyEntry();
        policy.setSendAdvisoryIfNoConsumers(true);

        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        broker.setDestinationPolicy(pMap);

        return broker;
    }

    protected Destination createDlqDestination() {
    	if (this.topic) {
    		return AdvisorySupport.getNoTopicConsumersAdvisoryTopic((ActiveMQDestination)getDestination());
    	} else {
    		return AdvisorySupport.getNoQueueConsumersAdvisoryTopic((ActiveMQDestination)getDestination());
    	}
    }

}

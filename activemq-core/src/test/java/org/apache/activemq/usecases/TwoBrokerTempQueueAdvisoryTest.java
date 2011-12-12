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

import java.net.URI;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;
import javax.management.ObjectName;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TwoBrokerTempQueueAdvisoryTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(TwoBrokerTempQueueAdvisoryTest.class);

    private void sendReceiveTempQueueMessage(String broker) throws Exception {

    	ConnectionFactory factory = getConnectionFactory(broker);
    	Connection conn = factory.createConnection();
    	Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    	Destination dest = session.createTemporaryQueue();
    	conn.close();
    }

    public void testTemporaryQueueAdvisory() throws Exception {
    	LOG.info("Running testTemporaryQueueAdvisory()");

    	startAllBrokers();
        waitForBridgeFormation();
        waitForMinTopicRegionConsumerCount("BrokerB", 1);
        waitForMinTopicRegionConsumerCount("BrokerA", 1);

        final int iterations = 30;
        for (int i = 0; i < iterations; i++) {
	        sendReceiveTempQueueMessage("BrokerA");
        }

        waitForMinTopicRegionConsumerCount("BrokerB", 1);
        waitForMinTopicRegionConsumerCount("BrokerA", 1);

        final DestinationViewMBean brokerAView = createView("BrokerA", "ActiveMQ.Advisory.TempQueue", ActiveMQDestination.TOPIC_TYPE);
        assertTrue("exact amount of advisories created on A, one each for creation/deletion", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("BrokerA temp advisory enque count: " + brokerAView.getEnqueueCount());
                return iterations * 2 == brokerAView.getEnqueueCount();
            }
        }));

        final DestinationViewMBean brokerBView = createView("BrokerB", "ActiveMQ.Advisory.TempQueue", ActiveMQDestination.TOPIC_TYPE);
        assertTrue("exact amount of advisories created on B, one each for creation/deletion", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("BrokerB temp advisory enque count: " + brokerBView.getEnqueueCount());
                return iterations * 2 == brokerBView.getEnqueueCount();
            }
        }));
    }


    protected DestinationViewMBean createView(String broker, String destination, byte type) throws Exception {
        String domain = "org.apache.activemq";
        ObjectName name;
        if (type == ActiveMQDestination.QUEUE_TYPE) {
            name = new ObjectName(domain + ":BrokerName=" + broker + ",Type=Queue,Destination=" + destination);
        } else {
            name = new ObjectName(domain + ":BrokerName=" + broker + ",Type=Topic,Destination=" + destination);
        }
        return (DestinationViewMBean) brokers.get(broker).broker.getManagementContext().newProxyInstance(name, DestinationViewMBean.class,
                true);
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();

        String options = new String("?persistent=false");
        createBroker(new URI("broker:(tcp://localhost:0)/BrokerA" + options));
        createBroker(new URI("broker:(tcp://localhost:0)/BrokerB" + options));

        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerA");
    }
}

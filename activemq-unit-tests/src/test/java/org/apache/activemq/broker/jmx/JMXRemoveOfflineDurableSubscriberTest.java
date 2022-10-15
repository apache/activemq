/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.jmx;

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class JMXRemoveOfflineDurableSubscriberTest {

    private BrokerService broker;

    private static final String SUBSCRIBER_NAME = "testSubscriber";
    private static final String OFFLINE_CONNECTION_ID = "OFFLINE";
    private static final String TOPIC_NAME = "testTopic";

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("ActiveMQBroker");
        broker.setPersistent(false);
        broker.setUseVirtualTopics(false);
        broker.setUseJmx(true);
        broker.addConnector("tcp://localhost:0");
        broker.start();
    }

    @After
    public void teardown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test()
    public void testCreateOfflineDurableSubscriber() throws Exception {
        broker.getAdminView().addTopic(TOPIC_NAME);
        broker.getAdminView().createDurableSubscriber(OFFLINE_CONNECTION_ID, SUBSCRIBER_NAME, TOPIC_NAME, null);
        broker.getAdminView().destroyDurableSubscriber(OFFLINE_CONNECTION_ID, SUBSCRIBER_NAME);

        // Just to make sure the subscriber was actually deleted, try deleting
        // the offline subscriber again and that should throw an exception
        boolean subscriberAlreadyDeleted = false;
        try {
            broker.getAdminView().destroyDurableSubscriber(OFFLINE_CONNECTION_ID, SUBSCRIBER_NAME);
        } catch (javax.jms.InvalidDestinationException t) {
            if (t.getMessage().equals("No durable subscription exists for clientID: " +
                    OFFLINE_CONNECTION_ID + " and subscriptionName: " +
                    SUBSCRIBER_NAME)) {
                subscriberAlreadyDeleted = true;
            }
        }
        assertTrue(subscriberAlreadyDeleted);
    }
}

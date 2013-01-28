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

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import junit.framework.Test;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DurableSubscriptionRemoveOfflineTest extends EmbeddedBrokerTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionRemoveOfflineTest.class);

    protected void setUp() throws Exception {
        useTopic = true;
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = super.createBroker();
        answer.setOfflineDurableSubscriberTaskSchedule(3 * 1000);
        answer.setOfflineDurableSubscriberTimeout(5 * 1000);
        answer.setDeleteAllMessagesOnStartup(true);
        return answer;
    }

    protected BrokerService restartBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        broker = null;

        broker = super.createBroker();
        broker.setOfflineDurableSubscriberTaskSchedule(3 * 1000);
        broker.setOfflineDurableSubscriberTimeout(5 * 1000);

        broker.start();
        broker.waitUntilStarted();

        return broker;
    }

    public void testRemove() throws Exception {
        Connection connection = createConnection();
        connection.setClientID("cliID");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber subscriber = session.createDurableSubscriber((Topic) createDestination(), "subName");
        subscriber.close();
        connection.close();

        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                 return broker.getAdminView().getInactiveDurableTopicSubscribers().length == 0;
            }
        }, 15000));
    }

    public void testRemoveAfterRestart() throws Exception {
        Connection connection = createConnection();
        connection.setClientID("cliID");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber subscriber = session.createDurableSubscriber((Topic) createDestination(), "subName");
        subscriber.close();
        connection.close();

        LOG.info("Broker restarting, wait for inactive cleanup afterwards.");

        restartBroker();

        LOG.info("Broker restarted, wait for inactive cleanup now.");

        assertTrue(broker.getAdminView().getInactiveDurableTopicSubscribers().length == 1);

        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                 return broker.getAdminView().getInactiveDurableTopicSubscribers().length == 0;
            }
        }, 20000));
    }

    protected boolean isPersistent() {
        return true;
    }

    public static Test suite() {
        return suite(DurableSubscriptionRemoveOfflineTest.class);
     }
}

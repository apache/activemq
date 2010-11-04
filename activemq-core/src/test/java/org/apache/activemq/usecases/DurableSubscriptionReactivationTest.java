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
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import junit.framework.Test;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;

public class DurableSubscriptionReactivationTest extends EmbeddedBrokerTestSupport {

    public boolean keepDurableSubsActive;
    
    public void initCombosForTestReactivateKeepaliveSubscription() {
        addCombinationValues("keepDurableSubsActive", new Object[] { new Boolean(true), new Boolean(false) });
    }
    
    public void testReactivateKeepaliveSubscription() throws Exception {

        Connection connection = createConnection();
        connection.setClientID("cliID");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber subscriber = session.createDurableSubscriber((Topic) createDestination(), "subName");
        subscriber.close();
        connection.close();

        connection = createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(createDestination());
        producer.send(session.createMessage());
        connection.close();

        connection = createConnection();
        connection.setClientID("cliID");
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        subscriber = session.createDurableSubscriber((Topic) createDestination(), "subName");
        Message message = subscriber.receive(1 * 1000);
        subscriber.close();
        connection.close();

        assertNotNull("Message not received.", message);
    }

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
        answer.setKeepDurableSubsActive(keepDurableSubsActive);
        answer.setPersistenceAdapter(new JDBCPersistenceAdapter());
        return answer;
    }

    protected boolean isPersistent() {
        return true;
    }
    
    public static Test suite() {
        return suite(DurableSubscriptionReactivationTest.class);
      }
}

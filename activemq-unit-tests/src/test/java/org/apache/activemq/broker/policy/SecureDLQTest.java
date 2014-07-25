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

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.security.*;

import javax.jms.*;

import static org.apache.activemq.security.SimpleSecurityBrokerSystemTest.*;

public class SecureDLQTest extends DeadLetterTestSupport {

    Connection dlqConnection;
    Session dlqSession;

    public static AuthorizationMap createAuthorizationMap() {
        DestinationMap readAccess = new DefaultAuthorizationMap();
        readAccess.put(new ActiveMQQueue("TEST"), ADMINS);
        readAccess.put(new ActiveMQQueue("TEST"), USERS);
        readAccess.put(new ActiveMQQueue("ActiveMQ.DLQ"), ADMINS);

        DestinationMap writeAccess = new DefaultAuthorizationMap();
        writeAccess.put(new ActiveMQQueue("TEST"), ADMINS);
        writeAccess.put(new ActiveMQQueue("TEST"), USERS);
        writeAccess.put(new ActiveMQQueue("ActiveMQ.DLQ"), ADMINS);

        readAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), WILDCARD);
        writeAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), WILDCARD);

        DestinationMap adminAccess = new DefaultAuthorizationMap();
        adminAccess.put(new ActiveMQQueue("TEST"), ADMINS);
        adminAccess.put(new ActiveMQQueue("TEST"), USERS);
        adminAccess.put(new ActiveMQQueue("ActiveMQ.DLQ"), ADMINS);
        adminAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), WILDCARD);

        return new SimpleAuthorizationMap(writeAccess, readAccess, adminAccess);
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(createAuthorizationMap());

        broker.setPlugins(new BrokerPlugin[] {authorizationPlugin, new SimpleSecurityBrokerSystemTest.SimpleAuthenticationFactory()});
        return broker;
    }

    // lets disable the inapplicable tests
    public void testTransientTopicMessage() throws Exception {
    }

    public void testDurableTopicMessage() throws Exception {
    }

    @Override
    protected void doTest() throws Exception {
        timeToLive = 1000;
        acknowledgeMode = Session.CLIENT_ACKNOWLEDGE;
        makeConsumer();
        sendMessages();
        Thread.sleep(1000);
        consumer.close();

        Thread.sleep(1000);
        // this should try to send expired messages to dlq
        makeConsumer();

        makeDlqConsumer();
        for (int i = 0; i < messageCount; i++) {
            Message msg = dlqConsumer.receive(1000);
            assertMessage(msg, i);
            assertNotNull("Should be a DLQ message for loop: " + i, msg);
        }

    }

    @Override
    public void tearDown() throws Exception {
        if (dlqConnection != null) {
            dlqConnection.close();
        }
        super.tearDown();
    }

    @Override
    protected Connection createConnection() throws Exception {
        return getConnectionFactory().createConnection("user", "password");
    }

    @Override
    protected void makeDlqConsumer() throws Exception {
        dlqDestination = createDlqDestination();
        dlqConnection = getConnectionFactory().createConnection("system", "manager");
        dlqConnection.start();
        dlqSession = dlqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        dlqConsumer = dlqSession.createConsumer(dlqDestination);
    }

    @Override
    protected Destination createDlqDestination() {
        return new ActiveMQQueue("ActiveMQ.DLQ");
    }

    @Override
    protected String getDestinationString() {
        return "TEST";
    }
}

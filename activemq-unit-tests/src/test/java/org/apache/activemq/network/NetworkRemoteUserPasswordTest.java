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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

public class NetworkRemoteUserPasswordTest extends BaseNetworkTest {

    @Override
    protected void doSetUp(boolean deleteAllMessages) throws Exception {
        remoteBroker = createRemoteBroker();
        remoteBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        localBroker = createLocalBroker();
        localBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        localBroker.start();
        localBroker.waitUntilStarted();
        URI localURI = localBroker.getVmConnectorURI();
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(localURI);
        fac.setAlwaysSyncSend(true);
        fac.setDispatchAsync(false);
        localConnection = fac.createConnection("userA", "passwordA");
        localConnection.setClientID("clientId");
        localConnection.start();
        URI remoteURI = remoteBroker.getVmConnectorURI();
        fac = new ActiveMQConnectionFactory(remoteURI);
        remoteConnection = fac.createConnection("userB", "passwordB");
        remoteConnection.setClientID("clientId");
        remoteConnection.start();
        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    protected String getRemoteBrokerURI() {
        return "org/apache/activemq/network/remoteBroker-authn.xml";
    }

    @Override
    protected String getLocalBrokerURI() {
        return "org/apache/activemq/network/localBroker-remote-userName.xml";
    }

    @Test
    public void testNetworkRemoteUserPassword() throws JMSException {

        // Across the network
        sendTextMessage(localConnection, "include.test.foo", "This network message uses remoteUserName");
        verifyTextMessage(remoteConnection, "include.test.foo", "This network message uses remoteUserName", null, null, false);
    }

    protected void sendTextMessage(Connection connection, String queueName, String textBody) throws JMSException {
        Session tmpSession = null;
        MessageProducer tmpProducer = null;
        try { 
            tmpSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer tmpLocalProducer = tmpSession.createProducer(tmpSession.createQueue(queueName));
            tmpLocalProducer.send(tmpSession.createTextMessage(textBody));
        } finally {
            if(tmpProducer != null) { tmpProducer.close(); }
            if(tmpSession != null) { tmpSession.close(); }
        }
    }

    protected void verifyTextMessage(Connection connection, String queueName, String body, String property, String value, boolean propertyExists) throws JMSException {
        int maxLoops = 50;
        int loopCount = 0;
        boolean found = false;
        
        Session tmpSession = null;
        MessageConsumer tmpConsumer = null;

        try {
            tmpSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            tmpConsumer = tmpSession.createConsumer(tmpSession.createQueue(queueName));
            do {
                Message tmpMessage = tmpConsumer.receive(100l);
                if(tmpMessage != null) {
                    assert(TextMessage.class.isAssignableFrom(tmpMessage.getClass()));
                    TextMessage tmpTextMessage = TextMessage.class.cast(tmpMessage);
                    assertEquals(body, tmpTextMessage.getText());
                    
                    if(propertyExists) {
                        assertNotNull(tmpTextMessage.getStringProperty(property));
                        assertEquals(value, tmpTextMessage.getStringProperty(property));
                    } else if(property != null) {
                        assertNull(tmpTextMessage.getStringProperty(property));
                    }

                    found = true;
                }
                loopCount++;
            } while(!found && loopCount < maxLoops);
            assertTrue(found);
        } finally {
            if(tmpConsumer != null) { tmpConsumer.close(); }
            if(tmpSession != null) { tmpSession.close(); }
        }
    }
}

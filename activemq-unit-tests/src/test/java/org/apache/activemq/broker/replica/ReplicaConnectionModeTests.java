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
package org.apache.activemq.broker.replica;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.io.IOException;

public class ReplicaConnectionModeTests extends ReplicaPluginTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaConnectionModeTests.class);
    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    @Override
    protected void setUp() throws Exception {
        cleanKahaDB(FIRST_KAHADB_DIRECTORY);
        cleanKahaDB(SECOND_KAHADB_DIRECTORY);
        super.setUp();

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
    }

    @Override
    protected void tearDown() throws Exception {
        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }
        super.tearDown();
    }

    @Test (timeout = 60000)
    public void testAsyncConnection() throws Exception {
        ((ActiveMQConnection) firstBrokerConnection).setUseAsyncSend(true);
        firstBrokerConnection.start();
        secondBrokerConnection.start();

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertTrue(((TextMessage) receivedMessage).getText().contains(getName()));

        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination);
        receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertTrue(((TextMessage) receivedMessage).getText().contains(getName()));
        receivedMessage.acknowledge();
        Thread.sleep(LONG_TIMEOUT);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testConsumerAutoAcknowledgeMode() throws Exception {
        firstBrokerConnection.start();
        secondBrokerConnection.start();

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);


        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertTrue(((TextMessage) receivedMessage).getText().contains(getName()));

        Session firstBrokerConsumerSession = firstBrokerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer firstBrokerConsumer = firstBrokerConsumerSession.createConsumer(destination);

        receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertTrue(((TextMessage) receivedMessage).getText().contains(getName()));
        Thread.sleep(LONG_TIMEOUT);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testConsumerDupsOkAcknowledgeMode() throws Exception {
        firstBrokerConnection.start();
        secondBrokerConnection.start();

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);


        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertTrue(((TextMessage) receivedMessage).getText().contains(getName()));

        Session firstBrokerConsumerSession = firstBrokerConnection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        MessageConsumer firstBrokerConsumer = firstBrokerConsumerSession.createConsumer(destination);

        receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertTrue(((TextMessage) receivedMessage).getText().contains(getName()));
        Thread.sleep(LONG_TIMEOUT);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    private void cleanKahaDB(String filePath) throws IOException {
        File kahaDBFile = new File(filePath);
        if (kahaDBFile.exists()) {
            FileUtils.cleanDirectory(kahaDBFile);
        }
    }

}

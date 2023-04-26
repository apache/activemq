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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.replica.ReplicaSupport;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import java.io.File;
import java.io.IOException;

public class ReplicaPluginPersistentBrokerFunctionTest extends ReplicaPluginTestSupport {

    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    protected XAConnection firstBrokerXAConnection;
    protected XAConnection secondBrokerXAConnection;

    @Override
    protected void setUp() throws Exception {

        if (firstBroker == null) {
            firstBroker = createFirstBroker();
            firstBroker.setPersistent(true);
        }
        if (secondBroker == null) {
            secondBroker = createSecondBroker();
            secondBroker.setPersistent(true);
        }

        cleanKahaDB(FIRST_KAHADB_DIRECTORY);
        cleanKahaDB(SECOND_KAHADB_DIRECTORY);
        startFirstBroker();
        startSecondBroker();

        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress);
        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress);

        firstBrokerXAConnectionFactory = new ActiveMQXAConnectionFactory(firstBindAddress);
        secondBrokerXAConnectionFactory = new ActiveMQXAConnectionFactory(secondBindAddress);

        destination = createDestination();

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        firstBrokerXAConnection = firstBrokerXAConnectionFactory.createXAConnection();
        firstBrokerXAConnection.start();

        secondBrokerXAConnection = secondBrokerXAConnectionFactory.createXAConnection();
        secondBrokerXAConnection.start();
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

        if (firstBrokerXAConnection != null) {
            firstBrokerXAConnection.close();
            firstBrokerXAConnection = null;
        }
        if (secondBrokerXAConnection != null) {
            secondBrokerXAConnection.close();
            secondBrokerXAConnection = null;
        }

        cleanKahaDB(FIRST_KAHADB_DIRECTORY);
        cleanKahaDB(SECOND_KAHADB_DIRECTORY);
        super.tearDown();
    }

    @Test
    public void testReplicaBrokerShouldAbleToRestoreSequence() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        int messagesToSend = 10;
        for (int i = 0; i < messagesToSend; i++) {
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName() + " No. " + i);
            firstBrokerProducer.send(message);
        }

        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean firstBrokerMainQueueView = getQueueView(firstBroker, ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
        assertEquals(firstBrokerMainQueueView.getDequeueCount(), 1);

        QueueViewMBean secondBrokerSequenceQueueView = getQueueView(secondBroker, ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
        assertEquals(secondBrokerSequenceQueueView.browseMessages().size(), 1);
        TextMessage sequenceQueueMessage = (TextMessage) secondBrokerSequenceQueueView.browseMessages().get(0);
        String[] textMessageSequence = sequenceQueueMessage.getText().split("#");
        assertEquals(Integer.parseInt(textMessageSequence[0]), messagesToSend + 1);
        secondBrokerSession.close();

        restartSecondBroker(true);
        Thread.sleep(LONG_TIMEOUT);
        secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        secondBrokerSequenceQueueView = getQueueView(secondBroker, ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
        assertEquals(secondBrokerSequenceQueueView.browseMessages().size(), 1);
        sequenceQueueMessage = (TextMessage) secondBrokerSequenceQueueView.browseMessages().get(0);
        textMessageSequence = sequenceQueueMessage.getText().split("#");
        assertEquals(Integer.parseInt(textMessageSequence[0]), messagesToSend + 1);
        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testReplicaBrokerHasMessageToCatchUp() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        int messagesToSend = 10;
        for (int i = 0; i < messagesToSend; i++) {
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName() + " No. " + i);
            firstBrokerProducer.send(message);
        }

        Thread.sleep(LONG_TIMEOUT);

        secondBroker.stop();
        secondBroker.waitUntilStopped();

        for (int i = messagesToSend; i < messagesToSend * 2; i++) {
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName() + " No. " + i);
            firstBrokerProducer.send(message);
        }

        restartSecondBroker(true);

        Thread.sleep(LONG_TIMEOUT);
        secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        QueueViewMBean secondBrokerSequenceQueueView = getQueueView(secondBroker, ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
        assertEquals(secondBrokerSequenceQueueView.browseMessages().size(), 1);
        TextMessage sequenceQueueMessage = (TextMessage) secondBrokerSequenceQueueView.browseMessages().get(0);
        String[] textMessageSequence = sequenceQueueMessage.getText().split("#");
        assertEquals(Integer.parseInt(textMessageSequence[0]), messagesToSend * 2 + 1);
        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    private void restartSecondBroker(boolean persistent) throws Exception {
        secondBrokerConnection.close();
        secondBrokerXAConnection.close();
        secondBroker.stop();
        secondBroker.waitUntilStopped();

        secondBroker = createSecondBroker();
        secondBroker.setPersistent(persistent);
        startSecondBroker();
        secondBroker.waitUntilStarted();
        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();
        secondBrokerXAConnection = secondBrokerXAConnectionFactory.createXAConnection();
        secondBrokerXAConnection.start();
    }

    private void cleanKahaDB(String filePath) throws IOException {
        FileUtils.cleanDirectory(new File(filePath));
    }
}

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

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.apache.activemq.replica.ReplicaSupport;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;


public class ReplicaPluginFunctionsTest extends ReplicaPluginTestSupport {
    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    protected XAConnection firstBrokerXAConnection;
    protected XAConnection secondBrokerXAConnection;

    protected ReplicaReplicationQueueSupplier replicationQueueSupplier;
    static final int MAX_BATCH_LENGTH = 500;
    static final int MAX_BATCH_SIZE = 5_000_000; // 5 Mb

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        firstBrokerXAConnection = firstBrokerXAConnectionFactory.createXAConnection();
        firstBrokerXAConnection.start();

        secondBrokerXAConnection = secondBrokerXAConnectionFactory.createXAConnection();
        secondBrokerXAConnection.start();

        replicationQueueSupplier = new ReplicaReplicationQueueSupplier(secondBroker.getBroker());
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

        super.tearDown();
    }

    @Test
    public void testSendMessageOverMAX_BATCH_LENGTH() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        for (int i = 0; i < (int) (MAX_BATCH_LENGTH * 1.5); i++) {
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName() + " No. " + i);
            firstBrokerProducer.send(message);
        }

        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean firstBrokerMainQueueView = getQueueView(firstBroker, ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
        assertEquals(firstBrokerMainQueueView.getDequeueCount(), 2);

        QueueViewMBean secondBrokerSequenceQueueView = getQueueView(secondBroker, ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
        assertEquals(secondBrokerSequenceQueueView.browseMessages().size(), 1);
        TextMessage sequenceQueueMessage = (TextMessage) secondBrokerSequenceQueueView.browseMessages().get(0);
        String[] textMessageSequence = sequenceQueueMessage.getText().split("#");
        assertEquals(Integer.parseInt(textMessageSequence[0]), (int) (MAX_BATCH_LENGTH * 1.5) + 1);


        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testSendMessageOverMAX_BATCH_SIZE() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        String bigTextMessage = RandomStringUtils.randomAlphanumeric(MAX_BATCH_SIZE + 100);
        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(bigTextMessage);
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean firstBrokerMainQueueView = getQueueView(firstBroker, ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
        assertEquals(firstBrokerMainQueueView.getDequeueCount(), 2);

        QueueViewMBean secondBrokerSequenceQueueView = getQueueView(secondBroker, ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
        assertEquals(secondBrokerSequenceQueueView.browseMessages().size(), 1);
        TextMessage sequenceQueueMessage = (TextMessage) secondBrokerSequenceQueueView.browseMessages().get(0);
        String[] textMessageSequence = sequenceQueueMessage.getText().split("#");
        assertEquals(Integer.parseInt(textMessageSequence[0]), 2);


        firstBrokerSession.close();
        secondBrokerSession.close();
    }

}

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
package org.apache.activemq.bugs;
//package org.apache.activemq.transport.failover;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Assert;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.state.ConnectionStateTracker;
import org.apache.activemq.state.TransactionState;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.junit.Test;


public class AMQ2364Test {

    @SuppressWarnings("unchecked")
    @Test
    public void testRollbackLeak() throws Exception {

        int messageCount = 1000;
        URI failoverUri = new URI("failover:(vm://localhost)?jms.redeliveryPolicy.maximumRedeliveries=0");

        Destination dest = new ActiveMQQueue("Failover.Leak");

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(failoverUri);
        ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer producer = session.createProducer(dest);

        for (int i = 0; i < messageCount; ++i)
            producer.send(session.createTextMessage("Test message #" + i));
        producer.close();
        session.commit();

        MessageConsumer consumer = session.createConsumer(dest);

        final CountDownLatch latch = new CountDownLatch(messageCount);
        consumer.setMessageListener(new MessageListener() {

            public void onMessage(Message msg) {
                try {
                    session.rollback();
                } catch (JMSException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }
        });

        latch.await();
        consumer.close();
        session.close();

        ResponseCorrelator respCorr = (ResponseCorrelator) connection.getTransport();
        MutexTransport mutexTrans = (MutexTransport) respCorr.getNext();
        FailoverTransport failoverTrans = (FailoverTransport) mutexTrans.getNext();
        Field stateTrackerField = FailoverTransport.class.getDeclaredField("stateTracker");
        stateTrackerField.setAccessible(true);
        ConnectionStateTracker stateTracker = (ConnectionStateTracker) stateTrackerField.get(failoverTrans);
        Field statesField = ConnectionStateTracker.class.getDeclaredField("connectionStates");
        statesField.setAccessible(true);
        ConcurrentHashMap<ConnectionId, ConnectionState> states =
                (ConcurrentHashMap<ConnectionId, ConnectionState>) statesField.get(stateTracker);

        ConnectionState state = states.get(connection.getConnectionInfo().getConnectionId());

        Collection<TransactionState> transactionStates = state.getTransactionStates();

        connection.stop();
        connection.close();

        Assert.assertEquals("Transaction states not cleaned up", 0,transactionStates.size());
    }
}
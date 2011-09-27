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

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ3465Test
{
    private final String xaDestinationName = "DestinationXA";
    private final String destinationName = "Destination";
    private BrokerService broker;
    private String connectionUri;
    private long txGenerator = System.currentTimeMillis();

    private XAConnectionFactory xaConnectionFactory;
    private ConnectionFactory connectionFactory;

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://0.0.0.0:0");
        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();

        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
        xaConnectionFactory = new ActiveMQXAConnectionFactory(connectionUri);
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

   @Test
   public void testMixedXAandNonXAorTXSessions() throws Exception {

       XAConnection xaConnection = xaConnectionFactory.createXAConnection();
       xaConnection.start();
       XASession session = xaConnection.createXASession();
       XAResource resource = session.getXAResource();
       Destination dest = new ActiveMQQueue(xaDestinationName);

       // publish a message
       Xid tid = createXid();
       resource.start(tid, XAResource.TMNOFLAGS);
       MessageProducer producer = session.createProducer(dest);
       ActiveMQTextMessage message  = new ActiveMQTextMessage();
       message.setText("Some Text");
       producer.send(message);
       resource.end(tid, XAResource.TMSUCCESS);
       resource.commit(tid, true);
       session.close();

       session = xaConnection.createXASession();
       MessageConsumer consumer = session.createConsumer(dest);
       tid = createXid();
       resource = session.getXAResource();
       resource.start(tid, XAResource.TMNOFLAGS);
       TextMessage receivedMessage = (TextMessage) consumer.receive(1000);
       assertNotNull(receivedMessage);
       assertEquals("Some Text", receivedMessage.getText());
       resource.end(tid, XAResource.TMSUCCESS);

       // Test that a normal session doesn't operate on XASession state.
       Connection connection2 = connectionFactory.createConnection();
       connection2.start();
       ActiveMQSession session2 = (ActiveMQSession) connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

       if (session2.isTransacted()) {
           session2.rollback();
       }

       session2.close();

       resource.commit(tid, true);
   }

   @Test
   public void testMixedXAandNonXALocalTXSessions() throws Exception {

       XAConnection xaConnection = xaConnectionFactory.createXAConnection();
       xaConnection.start();
       XASession session = xaConnection.createXASession();
       XAResource resource = session.getXAResource();
       Destination dest = new ActiveMQQueue(xaDestinationName);

       // publish a message
       Xid tid = createXid();
       resource.start(tid, XAResource.TMNOFLAGS);
       MessageProducer producer = session.createProducer(dest);
       ActiveMQTextMessage message  = new ActiveMQTextMessage();
       message.setText("Some Text");
       producer.send(message);
       resource.end(tid, XAResource.TMSUCCESS);
       resource.commit(tid, true);
       session.close();

       session = xaConnection.createXASession();
       MessageConsumer consumer = session.createConsumer(dest);
       tid = createXid();
       resource = session.getXAResource();
       resource.start(tid, XAResource.TMNOFLAGS);
       TextMessage receivedMessage = (TextMessage) consumer.receive(1000);
       assertNotNull(receivedMessage);
       assertEquals("Some Text", receivedMessage.getText());
       resource.end(tid, XAResource.TMSUCCESS);

       // Test that a normal session doesn't operate on XASession state.
       Connection connection2 = connectionFactory.createConnection();
       connection2.start();
       ActiveMQSession session2 = (ActiveMQSession) connection2.createSession(true, Session.AUTO_ACKNOWLEDGE);
       Destination destination = new ActiveMQQueue(destinationName);
       ActiveMQMessageProducer producer2 = (ActiveMQMessageProducer) session2.createProducer(destination);
       producer2.send(session2.createTextMessage("Local-TX"));

       if (session2.isTransacted()) {
           session2.rollback();
       }

       session2.close();

       resource.commit(tid, true);
   }

   public Xid createXid() throws IOException {

       ByteArrayOutputStream baos = new ByteArrayOutputStream();
       DataOutputStream os = new DataOutputStream(baos);
       os.writeLong(++txGenerator);
       os.close();
       final byte[] bs = baos.toByteArray();

       return new Xid() {
           public int getFormatId() {
               return 86;
           }

           public byte[] getGlobalTransactionId() {
               return bs;
           }

           public byte[] getBranchQualifier() {
               return bs;
           }
       };
   }
}

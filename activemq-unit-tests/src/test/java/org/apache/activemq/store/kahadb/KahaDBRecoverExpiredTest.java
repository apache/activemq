/*
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
package org.apache.activemq.store.kahadb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TopicSession;
import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.SubscriptionKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Test for {@link TopicMessageStore#recoverExpired(Set, int)}
 */
public class KahaDBRecoverExpiredTest {

  @Rule
  public Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

  @Rule
  public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

  private BrokerService broker;
  private URI brokerConnectURI;
  private final ActiveMQTopic topic = new ActiveMQTopic("test.topic");
  private final SubscriptionKey subKey1 = new SubscriptionKey("clientId", "sub1");
  private final SubscriptionKey subKey2 = new SubscriptionKey("clientId", "sub2");

  @Before
  public void startBroker() throws Exception {
    broker = new BrokerService();
    broker.setPersistent(true);
    KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
    persistenceAdapter.setDirectory(dataFileDir.getRoot());
    broker.setPersistenceAdapter(persistenceAdapter);
    //set up a transport
    TransportConnector connector = broker
        .addConnector(new TransportConnector());
    connector.setUri(new URI("tcp://0.0.0.0:0"));
    connector.setName("tcp");
    broker.start();
    broker.waitUntilStarted();
    brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();
  }

  @After
  public void stopBroker() throws Exception {
    broker.stop();
    broker.waitUntilStopped();
  }

  private Session initializeSubs() throws JMSException {
    Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
    connection.setClientID("clientId");
    connection.start();

    Session session = connection.createSession(false, TopicSession.AUTO_ACKNOWLEDGE);
    session.createDurableSubscriber(topic, "sub1");
    session.createDurableSubscriber(topic, "sub2");

    return session;
  }

  // test recover expired works in general, verify does not return
  // expired if subs have already acked
  @Test
  public void testRecoverExpired() throws Exception {
    try (Session session = initializeSubs()) {
      MessageProducer prod = session.createProducer(topic);

      Destination dest = broker.getDestination(topic);
      TopicMessageStore store = (TopicMessageStore) dest.getMessageStore();

      // nothing should be expired yet, no messags
      var expired = store.recoverExpired(Set.of(subKey1, subKey2), 100);
      assertTrue(expired.isEmpty());

      // Sent 10 messages, alternating no expiration and 1 second ttl
      for (int i = 0; i < 10; i++) {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText("message" + i);
        var ttl = i % 2 == 0 ? 1000 : 0;
        prod.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, ttl);
      }

      // wait for the time to pass the point of needing expiration
      Thread.sleep(1500);
      // We should now find both durables have 5 expired messages
      expired = store.recoverExpired(Set.of(subKey1, subKey2), 100);
      assertEquals(2, expired.size());
      assertEquals(5, expired.get(subKey1).size());
      assertEquals(5, expired.get(subKey2).size());

      // Acknowledge the first 2 messages of only the first sub
      for (int i = 0; i < 2; i++) {
        MessageAck ack = new MessageAck();
        ack.setLastMessageId(expired.get(subKey1).get(i).getMessageId());
        ack.setAckType(MessageAck.EXPIRED_ACK_TYPE);
        ack.setDestination(topic);
        store.acknowledge(broker.getAdminConnectionContext(),"clientId", "sub1",
            ack.getLastMessageId(), ack);
      }

      // Now the first sub should only have 3 expired, but still 5 on the second
      expired = store.recoverExpired(Set.of(subKey1, subKey2), 100);
      assertEquals(3, expired.get(subKey1).size());
      assertEquals(5, expired.get(subKey2).size());

      // ack all remaining
      for (Entry<SubscriptionKey, List<org.apache.activemq.command.Message>> entry : expired.entrySet()) {
        for (org.apache.activemq.command.Message message : entry.getValue()) {
          MessageAck ack = new MessageAck();
          ack.setLastMessageId(message.getMessageId());
          ack.setAckType(MessageAck.EXPIRED_ACK_TYPE);
          ack.setDestination(topic);
          store.acknowledge(broker.getAdminConnectionContext(),entry.getKey().getClientId(),
              entry.getKey().getSubscriptionName(), ack.getLastMessageId(), ack);
        }
      }

      // should be empty again
      expired = store.recoverExpired(Set.of(subKey1, subKey2), 100);
      assertTrue(expired.isEmpty());
    }

  }

  // test max number of messages to check works
  @Test
  public void testRecoverExpiredMax() throws Exception {
    try (Session session = initializeSubs()) {
      MessageProducer prod = session.createProducer(topic);

      Destination dest = broker.getDestination(topic);
      TopicMessageStore store = (TopicMessageStore) dest.getMessageStore();

      // nothing should be expired yet, no messags
      var expired = store.recoverExpired(Set.of(subKey1, subKey2), 100);
      assertTrue(expired.isEmpty());

      // Sent 50 messages with no ttl followed by 50 with ttl
      ActiveMQTextMessage message = new ActiveMQTextMessage();
      for (int i = 0; i < 100; i++) {
        message.setText("message" + i);
        var ttl = i >= 50 ? 1000 : 0;
        prod.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, ttl);
      }

      // wait for the time to pass the point of needing expiration
      Thread.sleep(1500);

      // We should now find both durables have 50 expired messages
      expired = store.recoverExpired(Set.of(subKey1, subKey2), 100);
      assertEquals(2, expired.size());
      assertEquals(50, expired.get(subKey1).size());
      assertEquals(50, expired.get(subKey2).size());

      // Max is 50, should find none expired
      expired = store.recoverExpired(Set.of(subKey1, subKey2), 50);
      assertTrue(expired.isEmpty());

      // We should now find both durables have 25 expired messages with
      // max at 75
      expired = store.recoverExpired(Set.of(subKey1, subKey2), 75);
      assertEquals(2, expired.size());
      assertEquals(25, expired.get(subKey1).size());
      assertEquals(25, expired.get(subKey2).size());

      // Acknowledge the first 25 messages of only the first sub
      for (int i = 0; i < 25; i++) {
        MessageAck ack = new MessageAck();
        ack.setLastMessageId(expired.get(subKey1).get(i).getMessageId());
        ack.setAckType(MessageAck.EXPIRED_ACK_TYPE);
        ack.setDestination(topic);
        store.acknowledge(broker.getAdminConnectionContext(),"clientId", "sub1",
            ack.getLastMessageId(), ack);
      }

      // We should now find 25 on sub1 and 50 on sub2 with a max of 100
      expired = store.recoverExpired(Set.of(subKey1, subKey2), 100);
      assertEquals(2, expired.size());
      assertEquals(25, expired.get(subKey1).size());
      assertEquals(50, expired.get(subKey2).size());

    }

  }

  // Test that filtering works by the set of subscriptions
  @Test
  public void testRecoverExpiredSubSet() throws Exception {
    try (Session session = initializeSubs()) {
      MessageProducer prod = session.createProducer(topic);

      Destination dest = broker.getDestination(topic);
      TopicMessageStore store = (TopicMessageStore) dest.getMessageStore();

      // nothing should be expired yet, no messags
      var expired = store.recoverExpired(Set.of(subKey1, subKey2), 100);
      assertTrue(expired.isEmpty());

      // Send 10 expired
      for (int i = 0; i < 10; i++) {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText("message" + i);
        prod.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, 1000);
      }

      // wait for the time to pass the point of needing expiration
      Thread.sleep(1500);

      // Test getting each sub individually, get sub2 first
      expired = store.recoverExpired(Set.of(subKey2), 100);
      assertEquals(1, expired.size());
      assertEquals(10, expired.get(subKey2).size());

      // ack the first message of sub2
      MessageAck ack = new MessageAck();
      ack.setLastMessageId(expired.get(subKey2).get(0).getMessageId());
      ack.setAckType(MessageAck.EXPIRED_ACK_TYPE);
      ack.setDestination(topic);
      store.acknowledge(broker.getAdminConnectionContext(),"clientId", "sub2",
          ack.getLastMessageId(), ack);

      // check only sub2 has 9
      expired = store.recoverExpired(Set.of(subKey2), 100);
      assertEquals(1, expired.size());
      assertEquals(9, expired.get(subKey2).size());

      // check only sub1 still has 10
      expired = store.recoverExpired(Set.of(subKey1), 100);
      assertEquals(1, expired.size());
      assertEquals(10, expired.get(subKey1).size());

      // verify passing in unmatched sub leaves it out of the result set
      var unmatched = new SubscriptionKey("clientId", "sub3");
      expired = store.recoverExpired(Set.of(unmatched), 100);
      assertTrue(expired.isEmpty());

      // try 2 that exist and 1 that doesn't
      expired = store.recoverExpired(Set.of(subKey1, subKey2, unmatched), 100);
      assertEquals(2, expired.size());

    }

  }

}

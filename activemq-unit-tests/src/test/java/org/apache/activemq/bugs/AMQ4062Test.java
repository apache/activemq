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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.SubscriptionKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ4062Test {

    private BrokerService service;
    private PolicyEntry policy;
    private ConcurrentMap<SubscriptionKey, DurableTopicSubscription> durableSubscriptions;

    private static final int PREFETCH_SIZE_5=5;
    private String connectionUri;

    @Before
    public void startBroker() throws IOException, Exception {
        service=new BrokerService();
        service.setPersistent(true);
        service.setDeleteAllMessagesOnStartup(true);
        service.setUseJmx(false);

        KahaDBPersistenceAdapter pa=new KahaDBPersistenceAdapter();
        File dataFile=new File("createData");
        pa.setDirectory(dataFile);
        pa.setJournalMaxFileLength(1024*1024*32);

        service.setPersistenceAdapter(pa);

        policy = new PolicyEntry();
        policy.setTopic(">");
        policy.setDurableTopicPrefetch(PREFETCH_SIZE_5);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        service.setDestinationPolicy(pMap);

        service.addConnector("tcp://localhost:0");

        service.start();
        service.waitUntilStarted();

        connectionUri = service.getTransportConnectors().get(0).getPublishableConnectString();
    }

    public void restartBroker() throws IOException, Exception {
        service=new BrokerService();
        service.setPersistent(true);
        service.setUseJmx(false);
        service.setKeepDurableSubsActive(false);

        KahaDBPersistenceAdapter pa=new KahaDBPersistenceAdapter();
        File dataFile=new File("createData");
        pa.setDirectory(dataFile);
        pa.setJournalMaxFileLength(1024*1024*32);

        service.setPersistenceAdapter(pa);

        policy = new PolicyEntry();
        policy.setTopic(">");
        policy.setDurableTopicPrefetch(PREFETCH_SIZE_5);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        service.setDestinationPolicy(pMap);
        service.addConnector("tcp://localhost:0");
        service.start();
        service.waitUntilStarted();

        connectionUri = service.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @After
    public void stopBroker() throws Exception {
        service.stop();
        service.waitUntilStopped();
        service = null;
    }

    @Test
    public void testDirableSubPrefetchRecovered() throws Exception{

        PrefetchConsumer consumer=new PrefetchConsumer(true, connectionUri);
        consumer.recieve();
        durableSubscriptions=getDurableSubscriptions();
        ConsumerInfo info=getConsumerInfo(durableSubscriptions);

        //check if the prefetchSize equals to the size we set in the PolicyEntry
        assertEquals(PREFETCH_SIZE_5, info.getPrefetchSize());

        consumer.a.countDown();
        Producer p=new Producer(connectionUri);
        p.send();
        p = null;

        service.stop();
        service.waitUntilStopped();
        durableSubscriptions=null;

        consumer = null;
        stopBroker();

        restartBroker();

        getDurableSubscriptions();
        info=null;
        info = getConsumerInfo(durableSubscriptions);

        //check if the prefetchSize equals to 0 after persistent storage recovered
        //assertEquals(0, info.getPrefetchSize());

        consumer=new PrefetchConsumer(false, connectionUri);
        consumer.recieve();
        consumer.a.countDown();

        info=null;
        info = getConsumerInfo(durableSubscriptions);

        //check if the prefetchSize is the default size for durable consumer and the PolicyEntry
        //we set earlier take no effect
        //assertEquals(100, info.getPrefetchSize());
        //info.getPrefetchSize() is 100,it should be 5,because I set the PolicyEntry as follows,
        //policy.setDurableTopicPrefetch(PREFETCH_SIZE_5);
        assertEquals(5, info.getPrefetchSize());
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<SubscriptionKey, DurableTopicSubscription> getDurableSubscriptions() throws NoSuchFieldException, IllegalAccessException {
        if(durableSubscriptions!=null) return durableSubscriptions;
        RegionBroker regionBroker=(RegionBroker)service.getRegionBroker();
        TopicRegion region=(TopicRegion)regionBroker.getTopicRegion();
        Field field=TopicRegion.class.getDeclaredField("durableSubscriptions");
        field.setAccessible(true);
        durableSubscriptions=(ConcurrentMap<SubscriptionKey, DurableTopicSubscription>)field.get(region);
        return durableSubscriptions;
    }

    private ConsumerInfo getConsumerInfo(ConcurrentMap<SubscriptionKey, DurableTopicSubscription> durableSubscriptions) {
        ConsumerInfo info=null;
        for(Iterator<DurableTopicSubscription> it=durableSubscriptions.values().iterator();it.hasNext();){
            Subscription sub = it.next();
            info=sub.getConsumerInfo();
            if(info.getSubscriptionName().equals(PrefetchConsumer.SUBSCRIPTION_NAME)){
                return info;
            }
        }
        return null;
    }

    public class PrefetchConsumer implements MessageListener{
        public static final String SUBSCRIPTION_NAME = "A_NAME_ABC_DEF";
        private final String user = ActiveMQConnection.DEFAULT_USER;
        private final String password = ActiveMQConnection.DEFAULT_PASSWORD;
        private final String uri;
        private boolean transacted;
        ActiveMQConnection connection;
        Session session;
        MessageConsumer consumer;
        private boolean needAck=false;
        CountDownLatch a=new CountDownLatch(1);

        public PrefetchConsumer(boolean needAck, String uri){
            this.needAck=needAck;
            this.uri = uri;
        }

        public void recieve() throws Exception{
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, uri);
            connection = (ActiveMQConnection)connectionFactory.createConnection();
            connection.setClientID("3");
            connection.start();

            session = connection.createSession(transacted, Session.CLIENT_ACKNOWLEDGE);
            Destination destination = session.createTopic("topic2");
            consumer = session.createDurableSubscriber((Topic)destination,SUBSCRIPTION_NAME);
            consumer.setMessageListener(this);
        }

        @Override
        public void onMessage(Message message) {
            try {
                a.await();
            } catch (InterruptedException e1) {
            }
            if(needAck){
                try {
                    message.acknowledge();
                    consumer.close();
                    session.close();
                    connection.close();
                } catch (JMSException e) {
                }
            }
        }
    }

    public class Producer {

        protected final String user = ActiveMQConnection.DEFAULT_USER;

        private final String password = ActiveMQConnection.DEFAULT_PASSWORD;
        private final String uri;
        private boolean transacted;

        public Producer(String uri) {
            this.uri = uri;
        }

        public void send() throws Exception{
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, uri);
            ActiveMQConnection connection = (ActiveMQConnection)connectionFactory.createConnection();
            connection.start();

            ActiveMQSession session = (ActiveMQSession)connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic("topic2");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            for(int i=0;i<100;i++){
                TextMessage om=session.createTextMessage("hello from producer");
                producer.send(om);
            }
            producer.close();
            session.close();
            connection.close();
        }
    }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport.PersistenceAdapterChoice;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.MessageId;
import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class ActiveDurableSubscriptionBrowseExpireTest extends DurableSubscriptionOfflineTestBase {    
	//private static final Logger LOG = LoggerFactory.getLogger(ActiveDurableSubscriptionBrowseExpireTest.class);
    private boolean enableExpiration = true;

    public ActiveDurableSubscriptionBrowseExpireTest(boolean enableExpiration) {
    	keepDurableSubsActive = true;
    	this.enableExpiration = enableExpiration;
    }
    
    @Parameterized.Parameters(name = "enableExpiration_{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { false },
                { true }
            });
    }
    
    @Override
    public PersistenceAdapter setDefaultPersistenceAdapter(BrokerService broker) throws IOException {
        return super.setPersistenceAdapter(broker, PersistenceAdapterChoice.MEM);
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://" + getName(true));
        connectionFactory.setWatchTopicAdvisories(false);
        return connectionFactory;
    }

    @Test(timeout = 60 * 1000)
    public void testBrowseExpireActiveSub() throws Exception {
    	final int numberOfMessages = 10;
        
        broker.setEnableMessageExpirationOnActiveDurableSubs(enableExpiration);

        // create durable subscription
        Connection con = createConnection("consumer");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId");
        
        long timeStamp = System.currentTimeMillis();
        sendMessages(numberOfMessages, timeStamp);

        ObjectName[] subs = broker.getAdminView().getDurableTopicSubscribers();
        assertEquals(1, subs.length);
        
        ObjectName subName = subs[0];
        DurableSubscriptionViewMBean sub = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(subName, DurableSubscriptionViewMBean.class, true);
        assertEquals(true, sub.isActive());
        
        // browse the durable sub
        CompositeData[] data  = sub.browse();
        assertNotNull(data);
        assertEquals(numberOfMessages, data.length);

        Destination dest = broker.getDestination(topic);
        assertEquals(0, dest.getDestinationStatistics().getExpired().getCount());
        
        // add every 3rd message to the expiration list
        TopicMessageStore topicStore = (TopicMessageStore)dest.getMessageStore();
        LinkedList<org.apache.activemq.command.Message> messagesToExpire = new LinkedList<>(); 
        topicStore.recover(new MessageRecoveryListener() {
            @Override
            public boolean recoverMessage(org.apache.activemq.command.Message message) throws Exception {
            	int index = (int)message.getProperty("index");
            	if(index % 3 == 0)
            		messagesToExpire.add(message);
                return true;
            }

            @Override
            public boolean recoverMessageReference(MessageId messageReference) throws Exception {
                return true;
            }

            @Override
            public boolean hasSpace() {

                return true;
            }

            @Override
            public boolean isDuplicate(MessageId id) {
                return false;
            }
        });
       
        // expire messages in the topic store
        for(org.apache.activemq.command.Message message: messagesToExpire) {
        	message.setExpiration(timeStamp - 1);
    		topicStore.updateMessage(message);
        }
        
        // browse (should | should not) expire the messages on the destination if expiration is (enabled | not enabled)
        data = sub.browse();
        assertNotNull(data);
        assertEquals(enableExpiration ? messagesToExpire.size() : 0, dest.getDestinationStatistics().getExpired().getCount());
        
        session.close();
        con.close();
    }
    
    private void sendMessages(int numberOfMessages, long timeStamp) throws Exception {
    	Connection con = createConnection("producer");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        for (int i = 0; i < numberOfMessages; i++) {
            Message message = session.createMessage();
            message.setIntProperty("index", i);
            message.setJMSTimestamp(timeStamp);
            producer.send(topic, message);
        }

        session.close();
        con.close();
    }
}

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
package org.apache.activemq.broker.virtual;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.spring.ConsumerBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class VirtualTopicSelectorTest extends CompositeTopicTest {

    private static final Log LOG = LogFactory.getLog(VirtualTopicSelectorTest.class);
            
    protected Destination getConsumer1Dsetination() {
        return new ActiveMQQueue("Consumer.1.VirtualTopic.TEST");
    }

    protected Destination getConsumer2Dsetination() {
        return new ActiveMQQueue("Consumer.2.VirtualTopic.TEST");
    }
    
    protected Destination getProducerDestination() {
        return new ActiveMQTopic("VirtualTopic.TEST");
    }
    
    @Override
    protected void assertMessagesArrived(ConsumerBean messageList1, ConsumerBean messageList2) {
        messageList1.assertMessagesArrived(total/2);
        messageList2.assertMessagesArrived(total/2);
 
        messageList1.flushMessages();
        messageList2.flushMessages();
        
        LOG.info("validate no other messages on queues");
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                
            Destination destination1 = getConsumer1Dsetination();
            Destination destination2 = getConsumer2Dsetination();
            MessageConsumer c1 = session.createConsumer(destination1, null);
            MessageConsumer c2 = session.createConsumer(destination2, null);
            c1.setMessageListener(messageList1);
            c2.setMessageListener(messageList2);
            
            
            LOG.info("send one simple message that should go to both consumers");
            MessageProducer producer = session.createProducer(getProducerDestination());
            assertNotNull(producer);
            
            producer.send(session.createTextMessage("Last Message"));
            
            messageList1.assertMessagesArrived(1);
            messageList2.assertMessagesArrived(1);
        
        } catch (JMSException e) {
            e.printStackTrace();
            fail("unexpeced ex while waiting for last messages: " + e);
        }
    }
    
    @Override
    protected BrokerService createBroker() throws Exception {
        // use message selectors on consumers that need to propagate up to the virtual
        // topic dispatch so that un matched messages do not linger on subscription queues
        messageSelector1 = "odd = 'yes'";
        messageSelector2 = "odd = 'no'";
        
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);

        VirtualTopic virtualTopic = new VirtualTopic();
        // the new config that enables selectors on the intercepter
        virtualTopic.setSelectorAware(true);
        VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
        interceptor.setVirtualDestinations(new VirtualDestination[]{virtualTopic});
        broker.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});
        return broker;
    }
}
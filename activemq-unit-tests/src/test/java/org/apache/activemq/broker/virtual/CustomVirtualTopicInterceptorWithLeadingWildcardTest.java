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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Test for ticket AMQ-6310, leading wildcards no longer match after AMQ-6058
 */
public class CustomVirtualTopicInterceptorWithLeadingWildcardTest extends EmbeddedBrokerTestSupport{

	private static final Logger LOG = LoggerFactory.getLogger(CustomVirtualTopicInterceptorWithLeadingWildcardTest.class);
	protected int total = 10;
	protected Connection connection;	
	
    protected ActiveMQDestination getConsumer1Destination() {
        return new ActiveMQQueue("q1.a.virtualtopic.topic");
    }

    protected ActiveMQDestination getConsumer2Destination() {
        return new ActiveMQQueue("q2.a.virtualtopic.topic");
    }     
    
    protected ActiveMQDestination getProducerDestination() {
        return new ActiveMQTopic("virtualtopic.topic");
    }    
    
    public void testVirtualTopicRouting() throws Exception {
    	if (connection == null) {
            connection = createConnection();
        }
        connection.start();
        
        LOG.info("validate no other messages on queues");        
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            ActiveMQDestination destination1 = getConsumer1Destination();
            ActiveMQDestination destination2 = getConsumer2Destination();

            MessageConsumer c1 = session.createConsumer(destination1, null);
            MessageConsumer c2 = session.createConsumer(destination2, null);

            LOG.info("send one simple message that should go to both consumers");
            MessageProducer producer = session.createProducer(getProducerDestination());
            assertNotNull(producer);

            producer.send(session.createTextMessage("Last Message"));
            //check that c1 received the message as it should
            assertNotNull(c1.receive(3000));
            //check that c2 received the message as well - this breaks pre-patch,
            //when VirtualTopicInterceptor.shouldDispatch only returned true if the prefix
            //did not have ".*", or the destination name started with the first part of the
            //prefix (i.e. in the case of "*.*.", the destination name would have had
            //to be "*").
            assertNotNull(c2.receive(3000));
            
        } catch (JMSException e) {
            e.printStackTrace();
            fail("unexpected ex while waiting for last messages: " + e);
        }
    }
    
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }
    
    //setup the broker and virtual topic to test custom Virtual topic name 
    //and a multilevel prefix
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);

        VirtualTopic virtualTopic = new VirtualTopic();
        virtualTopic.setName("virtualtopic.>");
        virtualTopic.setPrefix("*.*.");
        VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
        interceptor.setVirtualDestinations(new VirtualDestination[]{virtualTopic});        
        broker.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});
        return broker;
    }
}

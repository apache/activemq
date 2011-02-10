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

import javax.jms.Destination;
import javax.jms.MessageConsumer;

import junit.framework.Test;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.MessageIdList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

public class MessageReroutingTest extends JmsMultipleBrokersTestSupport {
    private static final transient Logger LOG = LoggerFactory.getLogger(MessageReroutingTest.class);
    
    
    public Destination dest;
    public static final int MESSAGE_COUNT = 50;

    protected void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        
        createBroker(new ClassPathResource("org/apache/activemq/usecases/rerouting-activemq-D.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/rerouting-activemq-C.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/rerouting-activemq-B.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/rerouting-activemq-A.xml"));
        
        brokers.get("broker-A").broker.waitUntilStarted();
    }
    
    public void initCombos() {
        addCombinationValues("dest", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST")});
    }
    
    public void testMessageRerouting() throws Exception {
        MessageConsumer consumer = createConsumer("broker-D", dest);
        
        MessageIdList received = getConsumerMessages("broker-D", consumer);
        
        Thread.sleep(2000); //wait for subs to propagate

        // send/receive messages
        sendMessages("broker-A", dest, MESSAGE_COUNT);
        received.waitForMessagesToArrive(MESSAGE_COUNT);
        LOG.info("received " +  received.getMessageCount() + " messages");
        assertEquals(MESSAGE_COUNT, received.getMessageCount());
        
        brokers.get("broker-B").broker.stop();
        brokers.get("broker-B").broker.waitUntilStopped();
        Thread.sleep(2000);
        
        // ensure send/receive still works
        sendMessages("broker-A", dest, MESSAGE_COUNT);
        received.waitForMessagesToArrive(MESSAGE_COUNT);
        LOG.info("received " +  received.getMessageCount() + " messages");
        assertTrue("Didn't receive any more messages " + received.getMessageCount(), received.getMessageCount() > MESSAGE_COUNT);
        
        
    }

    
    public static Test suite() {
        return suite(MessageReroutingTest.class);
    }
    
    
}

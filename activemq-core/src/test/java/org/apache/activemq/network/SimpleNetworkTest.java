/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.network;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import javax.jms.*;
import junit.framework.TestCase;

import org.apache.activemq.*;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.QueueRegion;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class SimpleNetworkTest extends TestCase  {
    
    protected static final int MESSAGE_COUNT = 10;
    protected AbstractApplicationContext context;
    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    
   

    protected void setUp() throws Exception {
        
        super.setUp();
        Resource resource = new ClassPathResource("org/apache/activemq/network/localBroker.xml");
        BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();
        localBroker = factory.getBroker();
        
        resource = new ClassPathResource("org/apache/activemq/network/remoteBroker.xml");
        factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();
        remoteBroker = factory.getBroker();
        
        localBroker.start();
        remoteBroker.start();
        
        URI localURI = localBroker.getVmConnectorURI();
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(localURI);
        localConnection = fac.createConnection();
        localConnection.start();
        
        URI remoteURI = remoteBroker.getVmConnectorURI();
        fac = new ActiveMQConnectionFactory(remoteURI);
        remoteConnection = fac.createConnection();
        remoteConnection.start();
        
    }

    
    protected void tearDown() throws Exception {
        localConnection.close();
        remoteConnection.close();
        localBroker.stop();
        remoteBroker.stop();
        super.tearDown();
    }
    
      
    public void testFiltering() throws Exception{
       ActiveMQTopic included = new ActiveMQTopic("include.test.bar");
       ActiveMQTopic excluded = new ActiveMQTopic("exclude.test.bar");
       Session localSession = localConnection.createSession(false,Session.AUTO_ACKNOWLEDGE);
       Session remoteSession = remoteConnection.createSession(false,Session.AUTO_ACKNOWLEDGE);
       MessageConsumer includedConsumer = remoteSession.createConsumer(included);
       MessageConsumer excludedConsumer = remoteSession.createConsumer(excluded);
       MessageProducer includedProducer = localSession.createProducer(included);
       MessageProducer excludedProducer = localSession.createProducer(excluded);
       Thread.sleep(1000);
       
       Message test = localSession.createTextMessage("test");
       includedProducer.send(test);
       excludedProducer.send(test);
       
       assertNull(excludedConsumer.receive(500));
       assertNotNull(includedConsumer.receive(500));
    }
    
    public void testConduitBridge() throws Exception{
        ActiveMQTopic included = new ActiveMQTopic("include.test.bar");
       
        Session localSession = localConnection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        Session remoteSession = remoteConnection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = remoteSession.createConsumer(included);
        MessageConsumer consumer2 = remoteSession.createConsumer(included);
        MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
       
        Thread.sleep(1000);
        
        
        int count = 10;
        for (int i = 0; i < count; i++){
            Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
            assertNotNull(consumer1.receive(500));
            assertNotNull(consumer2.receive(500));
        }
        
        
        //ensure no more messages received
        assertNull(consumer1.receive(500));
        assertNull(consumer2.receive(500));
     }
    
    

}

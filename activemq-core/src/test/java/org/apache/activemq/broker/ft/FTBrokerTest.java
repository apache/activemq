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
package org.apache.activemq.broker.ft;

import java.net.URI;

import javax.jms.*;
import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

public class FTBrokerTest extends TestCase {
    
    protected static final int MESSAGE_COUNT = 10;
    protected BrokerService master;
    protected BrokerService slave;
    protected Connection connection;
    protected String uriString = "failover://(tcp://localhost:62001,tcp://localhost:62002)?randomize=false";
    //protected String uriString = "tcp://localhost:62001";

    protected void setUp() throws Exception {
        BrokerFactoryBean brokerFactory = new BrokerFactoryBean(new ClassPathResource("org/apache/activemq/broker/ft/master.xml"));
        brokerFactory.afterPropertiesSet();
        master = brokerFactory.getBroker();
        brokerFactory = new BrokerFactoryBean(new ClassPathResource("org/apache/activemq/broker/ft/slave.xml"));
        brokerFactory.afterPropertiesSet();
        slave = brokerFactory.getBroker();
        //uriString = "failover://(" + master.getVmConnectorURI() + "," + slave.getVmConnectorURI() + ")?randomize=false";
        //uriString = "failover://(" + master.getVmConnectorURI() + "," + slave.getVmConnectorURI() + ")";
        System.out.println("URI = " + uriString);
        URI uri = new URI(uriString);
        ConnectionFactory fac = new ActiveMQConnectionFactory(uri);
        connection = fac.createConnection();
        master.start();
        slave.start();
        //wait for thing to connect
        Thread.sleep(1000);
        connection.start();
        super.setUp();
        
        
       
    }


 
    
    protected void tearDown() throws Exception {
        try {
        connection.close();
        slave.stop();
        master.stop();
        }catch(Throwable e){
            e.printStackTrace();
        }
        
        super.tearDown();
    }
    
    public void testFTBroker() throws Exception{
       
        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getClass().toString());
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < MESSAGE_COUNT; i++){
            Message msg = session.createTextMessage("test: " + i);
            producer.send(msg);
        }
        master.stop();
        session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        for (int i = 0; i < MESSAGE_COUNT; i++){
            System.out.println("GOT MSG: " + consumer.receive(1000));
        }
        
    }

}

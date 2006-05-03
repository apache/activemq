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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTopicSendReceiveWithTwoConnectionsTest;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 *Test failover for Queues
 *
 */
public class QueueMasterSlaveTest extends JmsTopicSendReceiveWithTwoConnectionsTest{

   
   
    protected BrokerService master;
    protected BrokerService slave;
    protected int inflightMessageCount = 0;
    protected int failureCount = 50;
    protected String uriString="failover://(tcp://localhost:62001,tcp://localhost:62002)?randomize=false";

    protected void setUp() throws Exception{
        failureCount = super.messageCount/2;
        super.topic = isTopic();
        BrokerFactoryBean brokerFactory=new BrokerFactoryBean(new ClassPathResource("org/apache/activemq/broker/ft/master.xml"));
        brokerFactory.afterPropertiesSet();
        master=brokerFactory.getBroker();
        brokerFactory=new BrokerFactoryBean(new ClassPathResource("org/apache/activemq/broker/ft/slave.xml"));
        brokerFactory.afterPropertiesSet();
        slave=brokerFactory.getBroker();
        master.start();
        slave.start();
        // wait for thing to connect
        Thread.sleep(1000);
        super.setUp();

    }

    protected void tearDown() throws Exception{
        super.tearDown();
        slave.stop();
        master.stop();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception{
        return new ActiveMQConnectionFactory(uriString);
    }
    
    protected void messageSent() throws Exception{
        if (++inflightMessageCount >= failureCount){
            inflightMessageCount = 0;
            Thread.sleep(1000);
            master.stop();
        }
    }
    
    protected boolean isTopic(){
        return false;
    }
}

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
package org.apache.activemq.perf;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

/**
 * 
 */
public class SlowConsumerTopicTest extends SimpleTopicTest {

    protected PerfConsumer[] slowConsumers;
    
    protected void setUp() throws Exception {
        
        playloadSize = 10 * 1024;
        super.setUp();
    }
   

    protected PerfConsumer createConsumer(ConnectionFactory fac, Destination dest, int number) throws JMSException {
        PerfConsumer result = new SlowConsumer(fac, dest);
        return result;
    }

    protected PerfProducer createProducer(ConnectionFactory fac, Destination dest, int number, byte[] payload) throws JMSException {
        PerfProducer result = super.createProducer(fac, dest, number, payload);
        result.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        result.setSleep(10);
        return result;
    }

    protected BrokerService createBroker(String url) throws Exception {
        Resource resource = new ClassPathResource("org/apache/activemq/perf/slowConsumerBroker.xml");
        System.err.println("CREATE BROKER FROM " + resource);
        BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();
        BrokerService broker = factory.getBroker();
        
        broker.start();
        return broker;
    }

    protected ActiveMQConnectionFactory createConnectionFactory(String uri) throws Exception {
        ActiveMQConnectionFactory result = super.createConnectionFactory(uri);
        ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
        policy.setTopicPrefetch(10);
        result.setPrefetchPolicy(policy);
        return result;
    }
}

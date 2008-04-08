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
 * @version $Revision: 1.3 $
 */
public class SlowConsumerTopicTest extends SimpleTopicTest {

    protected PerfConsumer[] slowConsumers;
    
    protected void setUp() throws Exception {
        
        playloadSize = 10 * 1024;
        super.setUp();
    }
   

    protected PerfConsumer createConsumer(ConnectionFactory fac, Destination dest, int number) throws JMSException {
        return new SlowConsumer(fac, dest);
    }

    protected PerfProducer createProducer(ConnectionFactory fac, Destination dest, int number, byte[] payload) throws JMSException {
        PerfProducer result = super.createProducer(fac, dest, number, payload);
        result.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return result;
    }

    protected BrokerService createBroker() throws Exception {
        Resource resource = new ClassPathResource("org/apache/activemq/perf/slowConsumerBroker.xml");
        BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();
        BrokerService broker = factory.getBroker();
        broker.start();
        return broker;
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory result = super.createConnectionFactory(bindAddress);
        ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
        policy.setTopicPrefetch(1000);
        result.setPrefetchPolicy(policy);
        return result;
    }
}

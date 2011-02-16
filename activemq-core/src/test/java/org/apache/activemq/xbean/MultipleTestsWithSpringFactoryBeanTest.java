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
package org.apache.activemq.xbean;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 
 * @author Neil Clayton
 * 
 */
public class MultipleTestsWithSpringFactoryBeanTest extends TestCase {
    
    private static final Logger LOG = LoggerFactory.getLogger(MultipleTestsWithSpringFactoryBeanTest.class);
    
    protected AbstractApplicationContext context;
    protected BrokerService service;
    private Connection connection;

    public void test1() throws Exception {
    }
    
    public void test2() throws Exception {
    }
    
    protected void setUp() throws Exception {
        LOG.info("### starting up the test case: " + getName());
        
        super.setUp();
        context = new ClassPathXmlApplicationContext("org/apache/activemq/xbean/spring2.xml");
        service = (BrokerService) context.getBean("broker");
        
        // already started
        service.start();
        
        connection = createConnectionFactory().createConnection();
        connection.start();
        LOG.info("### started up the test case: " + getName());
    }

    protected void tearDown() throws Exception {
        connection.close();
        
        // stopped as part of the context
        service.stop();
        context.close();
        super.tearDown();
        
        LOG.info("### closed down the test case: " + getName());
    }

    protected ConnectionFactory createConnectionFactory() {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL("vm://localhost");
        return factory;
    }
}

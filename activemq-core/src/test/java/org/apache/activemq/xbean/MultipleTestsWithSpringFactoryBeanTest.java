/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.apache.activemq.xbean;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import junit.framework.TestCase;

/**
 * 
 * @author Neil Clayton
 * @version $Revision$
 */
public class MultipleTestsWithSpringFactoryBeanTest extends TestCase {
    protected AbstractApplicationContext context;
    protected BrokerService service;
    private Connection connection;

    public void test1() throws Exception {
    }
    
    public void test2() throws Exception {
    }
    
    protected void setUp() throws Exception {
        System.out.println("### starting up the test case: " + getName());
        
        super.setUp();
        context = new ClassPathXmlApplicationContext("org/apache/activemq/xbean/spring2.xml");
        service = (BrokerService) context.getBean("broker");
        
        // already started
        service.start();
        
        connection = createConnectionFactory().createConnection();
        connection.start();
        System.out.println("### started up the test case: " + getName());
    }

    protected void tearDown() throws Exception {
        connection.close();
        
        // stopped as part of the context
        service.stop();
        context.close();
        super.tearDown();
        
        System.out.println("### closed down the test case: " + getName());
        System.out.println();
    }

    protected ConnectionFactory createConnectionFactory() {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL("vm://localhost");
        return factory;
    }
}

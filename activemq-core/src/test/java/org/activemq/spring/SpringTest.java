/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
*
**/

package org.activemq.spring;

import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringTest extends TestCase {
    
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(SpringTest.class);

    protected AbstractApplicationContext context;
    protected SpringConsumer consumer;
    protected SpringProducer producer;

    /**
     * Uses ActiveMQConnectionFactory to create the connection context.
     * Configuration file is /resources/spring.xml
     *
     * @throws Exception
     */
    public void testSenderWithSpringXml() throws Exception {
        String config = "spring.xml";
        assertSenderConfig(config);
    }

    /**
     * Spring configured test that uses ActiveMQConnectionFactory for
     * connection context and ActiveMQQueue for destination. Configuration
     * file is /resources/spring-queue.xml.
     *
     * @throws Exception
     */
    public void testSenderWithSpringXmlAndQueue() throws Exception {
        String config = "spring-queue.xml";
        assertSenderConfig(config);
    }

    /**
     * Spring configured test that uses JNDI. Configuration file is
     * /resources/spring-jndi.xml.
     *
     * @throws Exception
     */
    public void testSenderWithSpringXmlUsingJNDI() throws Exception {
        String config = "spring-jndi.xml";
        assertSenderConfig(config);
    }

    /**
     * Spring configured test where in the connection context is set to use
     * an embedded broker. Configuration file is /resources/spring-embedded.xml
     * and /resources/activemq.xml.
     *
     * @throws Exception
     */
    public void testSenderWithSpringXmlEmbeddedBrokerConfiguredViaXml() throws Exception {
        String config = "spring-embedded.xml";
        assertSenderConfig(config);
    }

    /**
     * assert method that is used by all the test method to send and receive messages
     * based on each spring configuration.
     *
     * @param config
     * @throws Exception
     */
    protected void assertSenderConfig(String config) throws Exception {
        context = new ClassPathXmlApplicationContext(config);

        consumer = (SpringConsumer) context.getBean("consumer");
        assertTrue("Found a valid consumer", consumer != null);

        consumer.start();

        producer = (SpringProducer) context.getBean("producer");
        assertTrue("Found a valid producer", producer != null);

        consumer.flushMessages();
        producer.start();

        // lets sleep a little to give the JMS time to dispatch stuff
        consumer.waitForMessagesToArrive(producer.getMessageCount());

        // now lets check that the consumer has received some messages
        List messages = consumer.flushMessages();
        log.info("Consumer has received messages....");
        for (Iterator iter = messages.iterator(); iter.hasNext();) {
            Object message = iter.next();
            log.info("Received: " + message);
        }

        assertEquals("Message count", producer.getMessageCount(), messages.size());
    }

    /**
     * Clean up method.
     *
     * @throws Exception
     */
    protected void tearDown() throws Exception {
        if (consumer != null) {
            consumer.stop();
        }
        if (producer != null) {
            producer.stop();
        }

        if (context != null) {
            context.destroy();
        }
    }

}

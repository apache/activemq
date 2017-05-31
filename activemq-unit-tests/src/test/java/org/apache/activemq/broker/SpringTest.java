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

package org.apache.activemq.broker;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.apache.activemq.spring.SpringConsumer;
import org.apache.activemq.spring.SpringProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringTest extends TestCase {
    
    private static final Logger LOG = LoggerFactory.getLogger(SpringTest.class);

    protected AbstractApplicationContext context;
    protected SpringConsumer consumer;
    protected SpringProducer producer;

    public void testSenderWithSpringXml() throws Exception {
        assertSenderConfig("org/apache/activemq/broker/spring.xml");
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
        LOG.info("Consumer has received messages....");
        for (Iterator iter = messages.iterator(); iter.hasNext();) {
            Object message = iter.next();
            LOG.info("Received: " + message);
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

    protected void setUp() throws Exception {
        if (System.getProperty("basedir") == null) {
            File file = new File(".");
            System.setProperty("basedir", file.getAbsolutePath());
        }
        super.setUp();
    }

}

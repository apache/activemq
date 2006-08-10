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
package org.apache.activemq;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.spring.SpringConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * 
 * @version $Revision$
 */
public class ZeroPrefetchConsumerTest extends TestSupport {

    private static final Log log = LogFactory.getLog(ZeroPrefetchConsumerTest.class);

    protected Connection connection;
    protected Queue queue;

    public void testCannotUseMessageListener() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);

        MessageListener listener = new SpringConsumer();
        try {
            consumer.setMessageListener(listener);
            fail("Should have thrown JMSException as we cannot use MessageListener with zero prefetch");
        }
        catch (JMSException e) {
            log.info("Received expected exception : " + e);
        }
    }

    public void testPullConsumerWorks() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello World!"));
        
        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);
        Message answer = consumer.receive(5000);
        assertNotNull("Should have received a message!", answer);
    }

    protected void setUp() throws Exception {
        topic = false;
        super.setUp();

        connection = createConnection();
        connection.start();
        queue = createQueue();
    }

    protected void tearDown() throws Exception {
        connection.close();
        super.tearDown();
    }

    protected Queue createQueue() {
        return new ActiveMQQueue(getClass().getName() + "." + getName() + "?consumer.prefetchSize=0");
    }

}

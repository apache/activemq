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
package org.apache.activemq.transport.amqp.profile;

import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.activemq.transport.amqp.JMSClientTestSupport;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that creates a large number of QueueBrowser and Session instances over time.
 *
 * There is a pause at the end of the test to allow for heap dumps or post run analysis.
 */
public class QueueBrowserStressTest extends JMSClientTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(QueueBrowserStressTest.class);

    private final int NUM_ITERATIONS = 1000;

    @Ignore("Used for profiling broker memory usage.")
    @Test
    public void testBrowserLeak() throws JMSException, InterruptedException {
        connection = createConnection();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(getDestinationName());
        session.close();

        for (int i = 1; i <= NUM_ITERATIONS; ++i) {
            // When recreating session, memory leak does occurs on the client but memory leak still occurs on the server
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueBrowser browser = session.createBrowser(queue, "JMSXGroupSeq=0");

            int j = 1;

            Enumeration<?> enums = browser.getEnumeration();
            if (!enums.hasMoreElements()) {
                LOG.debug("No messages in {}", queue.getQueueName());
            } else {
                Message message = (Message) enums.nextElement();
                if (message != null) {
                    LOG.debug("Received {} message : {} from {}", new Object[] {j++, message, queue.getQueueName()});
                }
            }

            LOG.debug("close browser for {}", queue.getQueueName());
            try {
                browser.close();
            } catch (JMSException e) {
                LOG.error("Error on browser close: {}", e);
            }
            browser = null;

            LOG.debug("close session for {}", queue.getQueueName());
            try {
                session.close();
            } catch (JMSException e) {
                LOG.error("Error on session close: {}", e);
            }
            session = null;
        }

        LOG.info("Task complete, capture heap dump now");
        TimeUnit.MINUTES.sleep(5);
    }
}

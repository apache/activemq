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
package org.apache.activemq.usecases;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.ConsumerBean;

/**
 * 
 * 
 */
public class StartAndStopClientAndBrokerDoesNotLeaveThreadsRunningTest extends TestCase {

    public static interface Task {
        void execute() throws Exception;
    }

    public void setUp() throws Exception {
    }

    public void testStartAndStopClientAndBrokerAndCheckNoThreadsAreLeft() throws Exception {
        runTest(new Task() {

            public void execute() throws Exception {
                BrokerService broker = new BrokerService();
                broker.setPersistent(false);
                broker.start();

                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
                Connection connection = factory.createConnection();
                connection.start();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue destination = session.createQueue(getName());

                // consumer
                MessageConsumer consumer = session.createConsumer(destination);
                ConsumerBean listener = new ConsumerBean();
                consumer.setMessageListener(listener);

                // producer
                MessageProducer producer = session.createProducer(destination);
                TextMessage message = session.createTextMessage("Hello World!");
                producer.send(message);
                producer.close();

                listener.assertMessagesArrived(1);

                consumer.close();
                session.close();
                connection.close();

                broker.stop();
            }
        });
    }

    public void runTest(Task task) throws Exception {
        int before = Thread.currentThread().getThreadGroup().activeCount();

        task.execute();

        Thread.yield();
        // need to wait for slow servers
        Thread.sleep(5000);

        int after = Thread.currentThread().getThreadGroup().activeCount();
        int diff = Math.abs(before - after);
        assertTrue("Should be at most one more thread. Diff = " + diff, diff + 1 <= after);
    }
}

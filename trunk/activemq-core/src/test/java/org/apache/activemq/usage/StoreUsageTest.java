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

package org.apache.activemq.usage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.ProducerThread;
import org.apache.activemq.util.Wait;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Session;

public class StoreUsageTest extends EmbeddedBrokerTestSupport {

    final int WAIT_TIME_MILLS = 20*1000;

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.getSystemUsage().getStoreUsage().setLimit(10 * 1024);
        broker.deleteAllMessages();
        return broker;
    }

    protected boolean isPersistent() {
        return true;
    }

    public void testJmx() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection conn = factory.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = sess.createQueue(this.getClass().getName());
        final ProducerThread producer = new ProducerThread(sess, dest);
        producer.start();

        // wait for the producer to block
        Thread.sleep(WAIT_TIME_MILLS / 2);

        broker.getAdminView().setStoreLimit(1024 * 1024);

        Thread.sleep(WAIT_TIME_MILLS);

        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return producer.getSentCount() == producer.getMessageCount();
            }
        }, WAIT_TIME_MILLS * 2);

        assertEquals("Producer didn't send all messages", producer.getMessageCount(), producer.getSentCount());

    }
}

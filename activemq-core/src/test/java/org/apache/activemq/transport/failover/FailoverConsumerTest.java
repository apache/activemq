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
package org.apache.activemq.transport.failover;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkTestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverConsumerTest extends NetworkTestSupport {

    public static final int MSG_COUNT = 100;
    private static final Logger LOG = LoggerFactory.getLogger(FailoverConsumerTest.class);


    public void testPublisherFailsOver() throws Exception {
        // Uncomment this if you want to use remote broker created by
        // NetworkTestSupport.
        // But it doesn't work. See comments below.
        // URI failoverURI = new
        // URI("failover://"+remoteConnector.getServer().getConnectURI());
        URI failoverURI = new URI("failover://tcp://localhost:61616");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(failoverURI);
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();

        // Prefetch size must be less than messages in the queue!!
        prefetchPolicy.setQueuePrefetch(MSG_COUNT - 10);
        factory.setPrefetchPolicy(prefetchPolicy);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQQueue("Test"));
        for (int idx = 0; idx < MSG_COUNT; ++idx) {
            producer.send(session.createTextMessage("Test"));
        }
        producer.close();
        session.close();
        int count = 0;

        Session consumerSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(new ActiveMQQueue("Test"));
        connection.start();
        Message msg = consumer.receive(3000);

        // restartRemoteBroker() doesn't work (you won't get received any
        // messages
        // after restart, javadoc says, that messages should be received
        // though).
        // So we must use external broker ant restart it manually.
        LOG.info("You should restart remote broker now and press enter!");
        System.in.read();
        // Thread.sleep(20000);
        restartRemoteBroker();
        msg.acknowledge();
        ++count;

        for (int idx = 1; idx < MSG_COUNT; ++idx) {
            msg = consumer.receive(3000);
            if (msg == null) {
                LOG.error("No messages received! Received:" + count);
                break;
            }
            msg.acknowledge();
            ++count;
        }
        assertEquals(count, MSG_COUNT);
        consumer.close();
        consumerSession.close();
        connection.close();

        connection = factory.createConnection();
        consumerSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = consumerSession.createConsumer(new ActiveMQQueue("Test"));
        connection.start();

        count = 0;
        do {
            msg = consumer.receive(1000);
            if (msg != null) {
                msg.acknowledge();
                ++count;
            }
        } while (msg != null);

        assertEquals(count, 0);

        consumer.close();
        consumerSession.close();
        connection.close();
    }

    protected String getRemoteURI() {
        return "tcp://localhost:55555";
    }
}

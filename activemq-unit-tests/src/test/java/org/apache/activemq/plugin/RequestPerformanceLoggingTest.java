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
package org.apache.activemq.plugin;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.util.AccessLogPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.net.URI;

public class RequestPerformanceLoggingTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(RequestPerformanceLoggingTest.class);

    private Connection connection;
    private BrokerService broker;

    public void testDestinationStats() throws Exception{
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue testQueue = session.createQueue("Test.Queue");
        MessageProducer producer = session.createProducer(testQueue);
        Message msg = session.createTextMessage("This is a test");
        producer.send(msg);
    }

    @Override
    protected void setUp() throws Exception {
        broker = createBroker();
        ConnectionFactory factory = new ActiveMQConnectionFactory(broker.getTransportConnectorURIsAsMap().get("tcp"));
        connection = factory.createConnection();
        connection.start();
    }

    @Override
    protected void tearDown() throws Exception{
        if (this.connection != null) {
            this.connection.close();
        }
        if (this.broker!=null) {
            this.broker.stop();
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        BrokerPlugin[] plugins = new BrokerPlugin[1];
        plugins[0] = new AccessLogPlugin();
        answer.setPlugins(plugins);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector("tcp://localhost:0");
        answer.start();
        return answer;
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }
}

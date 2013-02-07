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
package org.apache.activemq.perf;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ConnectionChurnTest extends TestCase {
    protected static final int CONNECTION_COUNT = 200;
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionChurnTest.class);
    protected BrokerService broker;
    protected String bindAddress = ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL+"?transport.closeAsync=false";
    protected int topicCount;

    public void testPerformance() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        List<Connection> list = new ArrayList<Connection>();
        for (int i = 0; i < CONNECTION_COUNT; i++) {
            Connection connection = factory.createConnection();
            connection.start();
            list.add(connection);
            LOG.info("Created " + i);
            if (i % 100 == 0) {
                closeConnections(list);
            }
        }
        closeConnections(list);
    }

    protected void closeConnections(List<Connection> list) throws JMSException {
        for (Connection c : list) {
            c.close();
        }
        for (TransportConnector tc : broker.getTransportConnectors()) {
            System.out.println(tc.getConnections().size());
        }
        list.clear();
    }

    @Override
    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory()
            throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
                ActiveMQConnection.DEFAULT_BROKER_URL);
        return cf;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        answer.setPersistent(false);
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }
}

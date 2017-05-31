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
package org.apache.activemq.bugs;

import static org.junit.Assert.fail;

import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ProducerInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ4213Test {

    private static BrokerService brokerService;
    private static String TEST_QUEUE = "testQueue";
    private static ActiveMQQueue queue = new ActiveMQQueue(TEST_QUEUE);

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);

        brokerService.setPlugins(new BrokerPlugin[]{
            new BrokerPluginSupport() {

                @Override
                public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
                    throw new javax.jms.JMSSecurityException(brokerService.getVmConnectorURI().toString());
                }
            }
        });

        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test(timeout = 30000)
    public void testExceptionOnProducerCreateThrows() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerService.getVmConnectorURI());
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        connection.start();

        try {
            session.createProducer(queue);
            fail("Should not be able to create this producer.");
        } catch (JMSException ex) {
        }
    }
}

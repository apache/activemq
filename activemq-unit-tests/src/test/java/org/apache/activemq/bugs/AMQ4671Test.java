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

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4671Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ4671Test.class);
    private static BrokerService brokerService;

    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        connectionUri = brokerService.getVmConnectorURI().toString();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test(timeout = 30000)
    public void testNonDurableSubscriberInvalidUnsubscribe() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUri);

        Connection connection = connectionFactory.createConnection();
        connection.setClientID(getClass().getName());
        connection.start();

        try {
            Session ts = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            try {
                ts.unsubscribe("invalid-subscription-name");
                fail("this should fail");
            } catch (javax.jms.InvalidDestinationException e) {
                LOG.info("Test caught correct invalid destination exception");
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}

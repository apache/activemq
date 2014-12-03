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

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ConnectionFailedException;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Oliver Belikan
 */
public class ExceptionListenerTest implements ExceptionListener {
    private static final Logger LOG = LoggerFactory.getLogger(ExceptionListenerTest.class);
    BrokerService brokerService;
    URI brokerUri;
    LinkedList<Throwable> exceptionsViaListener = new LinkedList<Throwable>();

    @Before
    public void startBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setAdvisorySupport(false);
        brokerService.setUseJmx(false);
        brokerService.setPersistent(false);
        brokerService.setPlugins(new BrokerPlugin[]{new SimpleAuthenticationPlugin(new ArrayList())});
        brokerUri = brokerService.addConnector("tcp://0.0.0.0:0").getConnectUri();
        brokerService.start();
    }

    @After
    public void stopBroker() throws Exception {
        exceptionsViaListener.clear();
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void fireOnSecurityException() throws Exception {
        doFireOnSecurityException(new ActiveMQConnectionFactory(brokerUri));
    }

    @Test
    public void fireOnSecurityExceptionFailover() throws Exception {
        doFireOnSecurityException(new ActiveMQConnectionFactory("failover://" + brokerUri));
    }

    public void doFireOnSecurityException(ActiveMQConnectionFactory factory) throws Exception {
        factory.setWatchTopicAdvisories(false);
        Connection connection = factory.createConnection();
        connection.setExceptionListener(this);

        try {
            connection.start();
            fail("Expect securityException");
        } catch (JMSSecurityException expected) {
            expected.printStackTrace();
            assertTrue("nested security exception: " + expected, expected.getCause() instanceof SecurityException);
        }

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return !exceptionsViaListener.isEmpty();
            }
        });
        Throwable expected = exceptionsViaListener.getFirst();
        assertNotNull(expected);
        assertNotNull(expected.getCause());

        assertTrue("expected exception: " + expected, expected.getCause().getCause() instanceof SecurityException);

        try {
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Expect error b/c connection is auto closed on security exception above");
        } catch (ConnectionFailedException e) {
        }
    }

    public void onException(JMSException e) {
        LOG.info("onException:" + e, new Throwable("FromHere"));
        exceptionsViaListener.add(e);
    }
}

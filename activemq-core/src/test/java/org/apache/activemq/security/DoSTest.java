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
package org.apache.activemq.security;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.JMSException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The configuration is set to except a maximum of 2 concurrent connections
 * As the exception is delibrately ignored, the ActiveMQConnection would continue to
 * attempt to connect unless the connection's transport was also stopped on an error.
 * <p/>
 * As the maximum connections allowed is 2, no more connections would be allowed unless
 * the transport was adequately destroyed on the broker side.
 */

public class DoSTest extends JmsTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(DoSTest.class);

    public void testInvalidAuthentication() throws Throwable {

        for (int i = 0; i < 1000; i++) {

            try {
                // Bad password
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
                Connection c = factory.createConnection("bad", "krap");
                c.start();
                fail("Expected exception.");
            } catch (JMSException e) {

            }
        }


    }

    protected BrokerService createBroker() throws Exception {
        return createBroker("org/apache/activemq/security/dos-broker.xml");
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }

}

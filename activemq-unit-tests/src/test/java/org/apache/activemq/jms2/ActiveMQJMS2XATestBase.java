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
package org.apache.activemq.jms2;

import java.lang.management.ManagementFactory;
import javax.jms.Session;
import javax.jms.XAConnection;

import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.junit.After;
import org.junit.Before;

public abstract class ActiveMQJMS2XATestBase extends ActiveMQJMS2TestBase {

    protected ActiveMQXAConnectionFactory activemqXAConnectionFactory = null;
    protected XAConnection xaConnection = null;

    @Before
    @Override
    public void setUp() throws Exception {
        activemqXAConnectionFactory = new ActiveMQXAConnectionFactory("vm://localhost?marshal=false&broker.persistent=false");
        xaConnection = activemqXAConnectionFactory.createXAConnection();

        // [AMQ-8325] Test using standard JMS connection with XAConnectionFactory
        activemqConnectionFactory = activemqXAConnectionFactory;
        connection = activemqConnectionFactory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        methodNameDestinationName = "AMQ.JMS2." + cleanParameterizedMethodName(testName.getMethodName().toUpperCase());
        messageProducer = session.createProducer(session.createQueue(methodNameDestinationName));
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
        if(xaConnection != null) {
            try { xaConnection.close(); } catch (Exception e) { } finally { xaConnection = null; }
        }
    }
}

/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.config;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.io.File;

/**
 * @version $Revision: 1.2 $
 */
public class BrokerXmlConfigTest extends TestCase {
    private BrokerService broker;

    public void testStartBrokerUsingXmlConfig() throws Exception {
        Connection connection = null;
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
            connection = connectionFactory.createConnection();
            connection.start();
            connection.close();
            connection = null;
        }
        catch (Exception e) {
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (JMSException e1) {
                    // ignore exception as we're throwing one anyway
                }
            }
            throw e;
        }
    }

    protected void setUp() throws Exception {
        System.setProperty("activemq.base", "target");
        new File("target/data").mkdirs();
        broker = BrokerFactory.createBroker("xbean:src/release/conf/activemq.xml");
    }

    protected void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }
}

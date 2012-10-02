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
package org.apache.activemq.config;

import javax.jms.Connection;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class BrokerPropertiesTest extends TestCase {
    private static final transient Logger LOG = LoggerFactory.getLogger(BrokerPropertiesTest.class);
    
    public void testPropertiesFile() throws Exception {
        BrokerService broker = BrokerFactory.createBroker("properties:org/apache/activemq/config/broker.properties");

        LOG.info("Created broker: " + broker);
        assertNotNull(broker);

        assertEquals("isUseJmx()", false, broker.isUseJmx());
        assertEquals("isPersistent()", false, broker.isPersistent());
        assertEquals("getBrokerName()", "Cheese", broker.getBrokerName());
        broker.stop();
    }


    public void testVmBrokerPropertiesFile() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?brokerConfig=properties:org/apache/activemq/config/broker.properties");
        Connection connection = factory.createConnection();
        BrokerService broker = BrokerRegistry.getInstance().lookup("Cheese");
        LOG.info("Found broker : " + broker);
        assertNotNull(broker);

        assertEquals("isUseJmx()", false, broker.isUseJmx());
        assertEquals("isPersistent()", false, broker.isPersistent());
        assertEquals("getBrokerName()", "Cheese", broker.getBrokerName());
        connection.close();
        broker.stop();
    }
}

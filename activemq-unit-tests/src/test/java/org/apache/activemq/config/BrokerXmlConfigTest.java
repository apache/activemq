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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.test.JmsTopicSendReceiveWithTwoConnectionsTest;

/**
 * 
 */
public class BrokerXmlConfigTest extends JmsTopicSendReceiveWithTwoConnectionsTest {
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        // START SNIPPET: bean

        // configure the connection factory using
        // normal Java Bean property methods
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();

        // configure the embedded broker using an XML config file
        // which is either a URL or a resource on the classpath

        // TODO ...

        //connectionFactory.setBrokerXmlConfig("file:src/sample-conf/default.xml");

        // you only need to configure the broker URL if you wish to change the
        // default connection mechanism, which in this test case we do
        connectionFactory.setBrokerURL("vm://localhost");

        // END SNIPPET: bean
        return connectionFactory;
    }

}

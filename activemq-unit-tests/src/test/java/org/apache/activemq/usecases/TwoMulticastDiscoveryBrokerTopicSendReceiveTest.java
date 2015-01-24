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

import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 *  reproduced: https://issues.apache.org/jira/browse/AMQ-4107
 */
public class TwoMulticastDiscoveryBrokerTopicSendReceiveTest extends TwoBrokerTopicSendReceiveTest {

    protected ActiveMQConnectionFactory createReceiverConnectionFactory() throws JMSException {
        return createConnectionFactory("org/apache/activemq/usecases/receiver-discovery.xml", "receiver", "vm://receiver");
    }

    protected ActiveMQConnectionFactory createSenderConnectionFactory() throws JMSException {
        return createConnectionFactory("org/apache/activemq/usecases/sender-discovery.xml", "sender", "vm://sender");
    }
    
    protected void setUp() throws Exception{
    	System.setProperty("groupId", getClass().getName()+"-"+System.currentTimeMillis());
        messageCount = 100000;
        super.setUp();
    }
}

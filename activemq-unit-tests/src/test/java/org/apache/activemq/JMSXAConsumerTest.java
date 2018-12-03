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
package org.apache.activemq;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import junit.framework.Test;

/*
 * allow an XA session to be used as an auto ack session when no XA transaction
 * https://issues.apache.org/activemq/browse/AMQ-2659
 */
public class JMSXAConsumerTest extends JMSConsumerTest {

    public static Test suite() {
        return suite(JMSXAConsumerTest.class);
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQXAConnectionFactory activeMQXAConnectionFactory = new ActiveMQXAConnectionFactory("vm://localhost?jms.xaAckMode=1");
        activeMQXAConnectionFactory.setXaAckMode(Session.AUTO_ACKNOWLEDGE);
        return activeMQXAConnectionFactory;
    }

    // some tests use transactions, these will not work unless an XA transaction is in place
    // slip these
    public void testPrefetch1MessageNotDispatched() throws Exception {
    }

    public void testRedispatchOfUncommittedTx() throws Exception {
    }

    public void testRedispatchOfRolledbackTx() throws Exception {
    }

    public void testMessageListenerOnMessageCloseUnackedWithPrefetch1StayInQueue() throws Exception {
    }

    // needs client ack, xa is auto ack if no transaction
    public void testExceptionOnClientAckAfterConsumerClose() throws Exception {
    }

    public void testReceiveTopicWithPrefetch1() throws Exception {
    }

    public void testReceiveQueueWithPrefetch1() throws Exception {
    }
}

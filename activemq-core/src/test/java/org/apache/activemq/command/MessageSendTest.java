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
package org.apache.activemq.command;

import java.io.IOException;

import junit.framework.Test;

import org.apache.activemq.JmsQueueSendReceiveTwoConnectionsStartBeforeBrokerTest;
import org.apache.activemq.util.ByteSequence;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MessageSendTest extends DataStructureTestSupport {
    private static final Log LOG = LogFactory.getLog(MessageSendTest.class);

    public static Test suite() {
        return suite(MessageSendTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombosForTestMessageSendMarshaling() {
        addCombinationValues("cacheEnabled", new Object[] {Boolean.TRUE, Boolean.FALSE});
    }

    public void testMessageSendMarshaling() throws IOException {
        ActiveMQMessage message = new ActiveMQMessage();
        message.setCommandId((short)1);
        message.setDestination(new ActiveMQQueue("queue"));
        message.setGroupID("group");
        message.setGroupSequence(4);
        message.setCorrelationId("correlation");
        message.setMessageId(new MessageId("c1:1:1", 1));

        assertBeanMarshalls(message);
        assertBeanMarshalls(message);

    }

    public void xtestPerformance() throws IOException {
        ActiveMQMessage message = new ActiveMQMessage();
        message.setProducerId(new ProducerId(new SessionId(new ConnectionId(new ConnectionId("test")), 1), 1));
        message.setMessageId(new MessageId(message.getProducerId(), 1));
        message.setCommandId((short)1);
        message.setGroupID("group");
        message.setGroupSequence(4);
        message.setCorrelationId("correlation");
        message.setContent(new ByteSequence(new byte[1024], 0, 1024));
        message.setTimestamp(System.currentTimeMillis());
        message.setDestination(new ActiveMQQueue("TEST"));

        int p = 1000000;

        long start = System.currentTimeMillis();
        for (int i = 0; i < p; i++) {
            marshalAndUnmarshall(message, wireFormat);
        }
        long end = System.currentTimeMillis();

        LOG.info("marshaled/unmarshaled: " + p + " msgs at " + (p * 1000f / (end - start)) + " msgs/sec");
    }
}

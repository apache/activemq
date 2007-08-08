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
import junit.textui.TestRunner;

public class MessageTest extends DataStructureTestSupport {

    public boolean cacheEnabled;

    public static Test suite() {
        return suite(MessageTest.class);
    }

    public static void main(String[] args) {
        TestRunner.run(suite());
    }

    public void initCombosForTestActiveMQMessageMarshaling() {
        addCombinationValues("cacheEnabled", new Object[] { Boolean.TRUE, Boolean.FALSE });
    }

    public void testActiveMQMessageMarshaling() throws IOException {
        ActiveMQMessage message = new ActiveMQMessage();
        message.setCommandId((short) 1);
        message.setOriginalDestination(new ActiveMQQueue("queue"));
        message.setGroupID("group");
        message.setGroupSequence(4);
        message.setCorrelationId("correlation");
        message.setMessageId(new MessageId("c1:1:1", 1));
        assertBeanMarshalls(message);
    }

    public void testActiveMQMessageMarshalingBigMessageId() throws IOException {
        ActiveMQMessage message = new ActiveMQMessage();
        message.setCommandId((short) 1);
        message.setOriginalDestination(new ActiveMQQueue("queue"));
        message.setGroupID("group");
        message.setGroupSequence(4);
        message.setCorrelationId("correlation");
        message.setMessageId(new MessageId("c1:1:1", Short.MAX_VALUE));
        assertBeanMarshalls(message);
    }

    public void testActiveMQMessageMarshalingBiggerMessageId() throws IOException {
        ActiveMQMessage message = new ActiveMQMessage();
        message.setCommandId((short) 1);
        message.setOriginalDestination(new ActiveMQQueue("queue"));
        message.setGroupID("group");
        message.setGroupSequence(4);
        message.setCorrelationId("correlation");
        message.setMessageId(new MessageId("c1:1:1", Integer.MAX_VALUE));
        assertBeanMarshalls(message);
    }

    public void testActiveMQMessageMarshalingBiggestMessageId() throws IOException {
        ActiveMQMessage message = new ActiveMQMessage();
        message.setCommandId((short) 1);
        message.setOriginalDestination(new ActiveMQQueue("queue"));
        message.setGroupID("group");
        message.setGroupSequence(4);
        message.setCorrelationId("correlation");
        message.setMessageId(new MessageId("c1:1:1", Long.MAX_VALUE));
        assertBeanMarshalls(message);
    }

    public void testMessageIdMarshaling() throws IOException {
        assertBeanMarshalls(new MessageId("c1:1:1", 1));
    }

}

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
package org.apache.activemq.jndi;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

import javax.naming.*;

/**
 * @version $Revision: 1.4 $
 */
public class ActiveMQInitialContextFactoryTest extends JNDITestSupport {

    public void testConnectionFactoriesArePresent() throws NamingException {
        String lookupName = getConnectionFactoryLookupName();
        assertConnectionFactoryPresent(lookupName);
    }

    public void testDestinationsArePresent() throws NamingException {

        // Retrieving destinations context is not yet implemented on the broker.
        // For this test, a jndi file properties will be used.

        InitialContext context = new InitialContext();

        // make sure context is not null
        assertTrue("Created context", context != null);

        Object topicDestination = context.lookup("MyTopic");

        // check if MyTopic is an ActiveMQTopic
        assertTrue("Should have found a topic but found: " + topicDestination, topicDestination instanceof ActiveMQTopic);

        Object queueDestination = context.lookup("MyQueue");

        // check if MyQueue is an ActiveMQueue
        assertTrue("Should have found a queue but found: " + queueDestination, queueDestination instanceof ActiveMQQueue);

    }

    public void testDynamicallyGrowing() throws Exception {
        Object answer = context.lookup("dynamicQueues/FOO.BAR");
        assertTrue("Should have found a queue but found: " + answer, answer instanceof ActiveMQQueue);

        ActiveMQQueue queue = (ActiveMQQueue)answer;
        assertEquals("queue name", "FOO.BAR", queue.getPhysicalName());

        answer = context.lookup("dynamicTopics/A.B.C");
        assertTrue("Should have found a topic but found: " + answer, answer instanceof ActiveMQTopic);

        ActiveMQTopic topic = (ActiveMQTopic)answer;
        assertEquals("topic name", "A.B.C", topic.getPhysicalName());

    }

    protected String getConnectionFactoryLookupName() {
        return "ConnectionFactory";
    }
}

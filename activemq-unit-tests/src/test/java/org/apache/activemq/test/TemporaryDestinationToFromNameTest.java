/*
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
package org.apache.activemq.test;

import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.EmbeddedBrokerAndConnectionTestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemporaryDestinationToFromNameTest extends EmbeddedBrokerAndConnectionTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(TemporaryDestinationToFromNameTest.class);

    public void testCreateTemporaryQueueThenCreateAQueueFromItsName() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue tempQueue = session.createTemporaryQueue();
        String name = tempQueue.getQueueName();
        LOG.info("Created queue named: " + name);

        Queue createdQueue = session.createQueue(name);

        assertEquals("created queue not equal to temporary queue", tempQueue, createdQueue);
    }

    public void testCreateTemporaryTopicThenCreateATopicFromItsName() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic tempTopic = session.createTemporaryTopic();
        String name = tempTopic.getTopicName();
        LOG.info("Created topic named: " + name);

        Topic createdTopic = session.createTopic(name);

        assertEquals("created topic not equal to temporary topic", tempTopic, createdTopic);
    }
}

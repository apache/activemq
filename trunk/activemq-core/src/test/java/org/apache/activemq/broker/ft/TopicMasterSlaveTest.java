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
package org.apache.activemq.broker.ft;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

/**
 * Test failover for Queues
 */
public class TopicMasterSlaveTest extends QueueMasterSlaveTest {

    protected boolean isTopic() {
        return true;
    }

    protected MessageConsumer createConsumer(Session session, Destination dest) throws JMSException {
        return session.createDurableSubscriber((Topic)dest, "subName");
    }

    protected Connection createReceiveConnection() throws Exception {
        Connection result = super.createReceiveConnection();
        result.setClientID("clientId");
        return result;
    }
}

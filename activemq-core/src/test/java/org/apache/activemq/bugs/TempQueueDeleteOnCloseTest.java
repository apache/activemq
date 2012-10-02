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
package org.apache.activemq.bugs;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

/**
 * Demonstrates how unmarshalled VM advisory messages for temporary queues prevent other connections from being closed.
 */
public class TempQueueDeleteOnCloseTest {

    @Test
    public void test() throws Exception {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

        // create a connection and session with a temporary queue
        Connection connectionA = connectionFactory.createConnection();
        connectionA.setClientID("ConnectionA");
        Session sessionA = connectionA.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination tempQueueA = sessionA.createTemporaryQueue();
        MessageConsumer consumer = sessionA.createConsumer(tempQueueA);
        connectionA.start();

        // start and stop another connection
        Connection connectionB = connectionFactory.createConnection();
        connectionB.setClientID("ConnectionB");
        connectionB.start();
        connectionB.close();

        consumer.close();
        connectionA.close();
    }
}

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
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.command.ActiveMQQueue;

public class CraigsBugTest extends EmbeddedBrokerTestSupport {

    private String connectionUri;

    public void testConnectionFactory() throws Exception {
        final ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(connectionUri);
        final ActiveMQQueue queue = new ActiveMQQueue("testqueue");
        final Connection conn = cf.createConnection();

        Runnable r = new Runnable() {
            public void run() {
                try {
                    Session session = conn.createSession(false, 1);
                    MessageConsumer consumer = session.createConsumer(queue, null);
                    consumer.receive(1000);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        };
        new Thread(r).start();
        conn.start();

        try {
            synchronized (this) {
                wait(3000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:0";
        super.setUp();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

}

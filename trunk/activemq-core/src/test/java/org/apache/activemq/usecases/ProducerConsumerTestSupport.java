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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;


/**
 * Base class for simple test cases using a single connection, session
 * producer and consumer
 *
 * 
 */
public class ProducerConsumerTestSupport extends TestSupport {
    protected Connection connection;
    protected Session session;
    protected MessageProducer producer;
    protected MessageConsumer consumer;
    protected Destination destination;

    protected void setUp() throws Exception {
        super.setUp();
        connection = createConnection();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = this.createDestination(getSubject());
        producer = session.createProducer(destination);
        consumer = session.createConsumer(destination);
        connection.start();
    }

    protected void tearDown() throws Exception {
        consumer.close();
        producer.close();
        session.close();
        connection.close();
        super.tearDown();
    }
}

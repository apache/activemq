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
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

public class MessageSender {
    private MessageProducer producer;
    private Session session;

    public MessageSender(String queueName, Connection connection, boolean useTransactedSession, boolean topic) throws Exception {
        session = useTransactedSession ? connection.createSession(true, Session.SESSION_TRANSACTED) : connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(topic ? session.createTopic(queueName) : session.createQueue(queueName));
    }

    public void send(String payload) throws Exception {
        ObjectMessage message = session.createObjectMessage();
        message.setObject(payload);
        producer.send(message);
        if (session.getTransacted()) {
            session.commit();
        }
    }
    
    public MessageProducer getProducer() {
        return producer;
    }
}

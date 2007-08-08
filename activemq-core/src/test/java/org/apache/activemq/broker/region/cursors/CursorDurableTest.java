/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.broker.region.cursors;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.activemq.broker.BrokerService;

/**
 * @version $Revision: 1.3 $
 */
public class CursorDurableTest extends CursorSupport {

    protected Destination getDestination(Session session) throws JMSException {
        String topicName = getClass().getName();
        return session.createTopic(topicName);
    }

    protected Connection getConsumerConnection(ConnectionFactory fac) throws JMSException {
        Connection connection = fac.createConnection();
        connection.setClientID("testConsumer");
        connection.start();
        return connection;
    }

    protected MessageConsumer getConsumer(Connection connection) throws Exception {
        Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = (Topic)getDestination(consumerSession);
        MessageConsumer consumer = consumerSession.createDurableSubscriber(topic, "testConsumer");
        return consumer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }
}

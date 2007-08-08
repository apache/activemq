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
package org.apache.activemq.spring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

public class SpringConsumer extends ConsumerBean implements MessageListener {
    private static final Log log = LogFactory.getLog(SpringConsumer.class);
    private JmsTemplate template;
    private String myId = "foo";
    private Destination destination;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;

    public void start() throws JMSException {
        String selector = "next = '" + myId + "'";

        try {
            ConnectionFactory factory = template.getConnectionFactory();
            connection = factory.createConnection();

            // we might be a reusable connection in spring
            // so lets only set the client ID once if its not set
            synchronized (connection) {
                if (connection.getClientID() == null) {
                    connection.setClientID(myId);
                }
            }

            connection.start();

            session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
            consumer = session.createConsumer(destination, selector, false);
            consumer.setMessageListener(this);
        }
        catch (JMSException ex) {
            log.error("", ex);
            throw ex;
        }
    }

    public void stop() throws JMSException {
        if (consumer != null) {
            consumer.close();
        }
        if (session != null) {
            session.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    public void onMessage(Message message) {
        super.onMessage(message);
        try {
            message.acknowledge();
        }
        catch (JMSException e) {
            log.error("Failed to acknowledge: " + e, e);
        }
    }

    // Properties
    //-------------------------------------------------------------------------
    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public String getMyId() {
        return myId;
    }

    public void setMyId(String myId) {
        this.myId = myId;
    }

    public JmsTemplate getTemplate() {
        return template;
    }

    public void setTemplate(JmsTemplate template) {
        this.template = template;
    }
}

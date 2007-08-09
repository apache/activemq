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
package org.apache.activemq.broker.util;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.util.FactoryFinder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @version $Revision: $
 */
public class CommandMessageListener implements MessageListener {
    private static final Log LOG = LogFactory.getLog(CommandMessageListener.class);

    private Session session;
    private MessageProducer producer;
    private CommandHandler handler;

    public CommandMessageListener(Session session) {
        this.session = session;
    }

    public void onMessage(Message message) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received command: " + message);
        }
        if (message instanceof TextMessage) {
            TextMessage request = (TextMessage) message;
            try {
                Destination replyTo = message.getJMSReplyTo();
                if (replyTo == null) {
                    LOG.warn("Ignored message as no JMSReplyTo set: " + message);
                    return;
                }
                Message response = processCommand(request);
                addReplyHeaders(request, response);
                getProducer().send(replyTo, response);
            }
            catch (Exception e) {
                LOG.error("Failed to process message due to: " + e + ". Message: " + message, e);
            }
        }
        else {
            LOG.warn("Ignoring invalid message: " + message);
        }
    }

    protected void addReplyHeaders(TextMessage request, Message response) throws JMSException {
        String correlationID = request.getJMSCorrelationID();
        if (correlationID != null) {
            response.setJMSCorrelationID(correlationID);
        }
    }

    /**
     * Processes an incoming JMS message returning the response message
     */
    public Message processCommand(TextMessage request) throws Exception {
        TextMessage response = session.createTextMessage();
        getHandler().processCommand(request, response);
        return response;
    }

    /**
     * Processes an incoming command from a console and returning the text to output
     */
    public String processCommandText(String line) throws Exception {
        TextMessage request = new ActiveMQTextMessage();
        request.setText(line);
        TextMessage response = new ActiveMQTextMessage();
        getHandler().processCommand(request, response);
        return response.getText();
    }

    public Session getSession() {
        return session;
    }

    public MessageProducer getProducer() throws JMSException {
        if (producer == null) {
            producer = getSession().createProducer(null);
        }
        return producer;
    }

    public CommandHandler getHandler() throws IllegalAccessException, IOException, InstantiationException, ClassNotFoundException {
        if (handler == null) {
            handler = createHandler();
        }
        return handler;
    }

    private CommandHandler createHandler() throws IllegalAccessException, IOException, ClassNotFoundException, InstantiationException {
        FactoryFinder factoryFinder = new FactoryFinder("META-INF/services/org/apache/activemq/broker/");
        return (CommandHandler) factoryFinder.newInstance("agent");
    }
}

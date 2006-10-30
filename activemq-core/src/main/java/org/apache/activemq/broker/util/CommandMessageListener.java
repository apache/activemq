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
package org.apache.activemq.broker.util;

import org.apache.activemq.util.FactoryFinder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import java.io.IOException;

/**
 * @version $Revision: $
 */
public class CommandMessageListener implements MessageListener {
    private static final Log log = LogFactory.getLog(CommandMessageListener.class);

    private Session session;
    private MessageProducer producer;
    private CommandHandler handler;

    public CommandMessageListener(Session session) {
        this.session = session;
    }

    public void onMessage(Message message) {
        if (message instanceof TextMessage) {
            TextMessage request = (TextMessage) message;
            try {
                Destination replyTo = message.getJMSReplyTo();
                if (replyTo == null) {
                    log.warn("Ignored message as no JMSReplyTo set: " + message);
                    return;
                }
                Message response = processCommand(request);
                addReplyHeaders(request, response);

            }
            catch (Exception e) {
                log.error("Failed to process message due to: " + e + ". Message: " + message, e);
            }
        }
        else {
            log.warn("Ignoring invalid message: " + message);
        }
    }

    protected void addReplyHeaders(TextMessage request, Message response) throws JMSException {
        String correlationID = request.getJMSCorrelationID();
        if (correlationID != null) {
            response.setJMSCorrelationID(correlationID);
        }
    }

    protected Message processCommand(TextMessage request) throws Exception {
        TextMessage response = session.createTextMessage();
        getHandler().processCommand(request, response);
        return response;
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

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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

public class SpringProducer {
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(SpringProducer.class);
    private JmsTemplate template;
    private Destination destination;
    private int messageCount = 10;

    public void start() throws JMSException {
        for (int i = 0; i < messageCount; i++) {
            final String text = "Text for message: " + i;
            template.send(destination, new MessageCreator() {
                public Message createMessage(Session session) throws JMSException {
                    log.info("Sending message: " + text);
                    TextMessage message = session.createTextMessage(text);
                    message.setStringProperty("next", "foo");
                    return message;
                }
            });
        }
    }

    public void stop() throws JMSException {
    }

    // Properties
    //-------------------------------------------------------------------------

    public JmsTemplate getTemplate() {
        return template;
    }

    public void setTemplate(JmsTemplate template) {
        this.template = template;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }

    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }
}

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
package org.apache.activemq.web.controller;

import java.util.Iterator;
import java.util.Map;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.web.BrokerFacade;
import org.apache.activemq.web.DestinationFacade;
import org.apache.activemq.web.WebClient;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

/**
 * Sends a message
 */
@Component
@RequestScope
public class SendMessage extends DestinationFacade implements Controller {

    private String jmsText;
    private boolean jmsPersistent;
    private int jmsPriority;
    private int jmsTimeToLive = -1;
    private String jmsCorrelationID;
    private String jmsReplyTo;
    private String jmsType;
    private int jmsMessageCount = 1;
    private String jmsMessageCountHeader = "JMSXMessageNumber";

    public SendMessage(final BrokerFacade brokerFacade) {
        super(brokerFacade);
    }

    public void handleRequest(final HttpServletRequest request, final HttpServletResponse response) throws Exception {
        WebClient client = WebClient.getWebClient(request);
        ActiveMQDestination dest = createDestination();

        sendMessages(request, client, dest);
        response.sendRedirect(isQueue() ? "queues.jsp" : "topics.jsp");
    }

    protected void sendMessages(final HttpServletRequest request, final WebClient client, final ActiveMQDestination dest)
            throws JMSException {
        if (jmsMessageCount <= 1) {
            jmsMessageCount = 1;
        }
        for (int i = 0; i < jmsMessageCount; i++) {
            Message message = createMessage(client);
            appendHeaders(message, request);
            if (jmsMessageCount > 1) {
                message.setIntProperty(jmsMessageCountHeader, i + 1);
            }

            client.send(dest, message, jmsPersistent, jmsPriority, jmsTimeToLive);
        }
    }

    // Properties
    // -------------------------------------------------------------------------

    public String getJMSCorrelationID() {
        return jmsCorrelationID;
    }

    public void setJMSCorrelationID(String correlationID) {
        if (correlationID != null) {
            correlationID = correlationID.trim();
        }
        jmsCorrelationID = correlationID;
    }

    public String getJMSReplyTo() {
        return jmsReplyTo;
    }

    public void setJMSReplyTo(String replyTo) {
        if (replyTo != null) {
            replyTo = replyTo.trim();
        }
        jmsReplyTo = replyTo;
    }

    public String getJMSType() {
        return jmsType;
    }

    public void setJMSType(String type) {
        if (type != null) {
            type = type.trim();
        }
        jmsType = type;
    }

    public boolean isJMSPersistent() {
        return jmsPersistent;
    }

    public void setJMSPersistent(final boolean persistent) {
        this.jmsPersistent = persistent;
    }

    public int getJMSPriority() {
        return jmsPriority;
    }

    public void setJMSPriority(final int priority) {
        this.jmsPriority = priority;
    }

    public String getJMSText() {
        return jmsText;
    }

    public void setJMSText(final String text) {
        this.jmsText = text;
    }

    public int getJMSTimeToLive() {
        return jmsTimeToLive;
    }

    public void setJMSTimeToLive(final int timeToLive) {
        this.jmsTimeToLive = timeToLive;
    }

    public int getJMSMessageCount() {
        return jmsMessageCount;
    }

    public void setJMSMessageCount(final int copies) {
        jmsMessageCount = copies;
    }

    public String getJMSMessageCountHeader() {
        return jmsMessageCountHeader;
    }

    public void setJMSMessageCountHeader(String messageCountHeader) {
        if (messageCountHeader != null) {
            messageCountHeader = messageCountHeader.trim();
        }
        jmsMessageCountHeader = messageCountHeader;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Message createMessage(final WebClient client) throws JMSException {
        if (jmsText != null) {
            return client.getSession().createTextMessage(jmsText);
        }
        // TODO create Bytes message from request body...
        return client.getSession().createMessage();
    }

    @SuppressWarnings("rawtypes")
    protected void appendHeaders(final Message message, final HttpServletRequest request) throws JMSException {
        message.setJMSCorrelationID(jmsCorrelationID);
        if (jmsReplyTo != null && !jmsReplyTo.trim().isEmpty()) {
            message.setJMSReplyTo(ActiveMQDestination.createDestination(jmsReplyTo, ActiveMQDestination.QUEUE_TYPE));
        }
        message.setJMSType(jmsType);

        // now lets add all the parameters
        Map map = request.getParameterMap();
        if (map != null) {
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext();) {
                Map.Entry entry = (Map.Entry) iter.next();
                String name = (String) entry.getKey();
                if (name.equals("secret")) {
                    continue;
                }
                Object value = entry.getValue();
                if (isValidPropertyName(name)) {
                    if (value instanceof String[]) {
                        String[] array = (String[]) value;
                        if (array.length > 0) {
                            value = array[0];
                        } else {
                            value = null;
                        }
                    }
                    if ((name.equals("AMQ_SCHEDULED_DELAY") || name.equals("AMQ_SCHEDULED_PERIOD"))) {
                        if (value != null) {
                            String str = value.toString().trim();
                            if (!str.isEmpty()) {
                                message.setLongProperty(name, Long.parseLong(str));
                            }
                        }
                    } else if (name.equals("AMQ_SCHEDULED_REPEAT")) {
                        if (value != null) {
                            String str = value.toString().trim();
                            if (!str.isEmpty()) {
                                message.setIntProperty(name, Integer.parseInt(str));
                            }
                        }
                    } else if (name.equals("AMQ_SCHEDULED_CRON")) {
                        if (value != null) {
                            String str = value.toString().trim();
                            if (!str.isEmpty()) {
                                message.setStringProperty(name, str);
                            }
                        }
                    } else {
                        if (value instanceof String) {
                            String text = value.toString().trim();
                            if (text.isEmpty()) {
                                value = null;
                            } else {
                                value = text;
                            }
                        }
                        if (value != null) {
                            message.setObjectProperty(name, value);
                        }
                    }
                }
            }
        }
    }
    protected boolean isValidPropertyName(final String name) {
        // allow JMSX extensions or non JMS properties
        return name.startsWith("JMSX") || !name.startsWith("JMS");
    }

    public String[] getSupportedHttpMethods() {
        return new String[]{"POST"};
    }
}

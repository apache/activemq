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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.web.BrokerFacade;
import org.apache.activemq.web.DestinationFacade;
import org.apache.activemq.web.WebClient;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.Controller;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Iterator;
import java.util.Map;

/**
 * Sends a message
 * 
 * @version $Revision$
 */
public class SendMessage extends DestinationFacade implements Controller {

    private String JMSText;
    private boolean JMSPersistent;
    private int JMSPriority;
    private int JMSTimeToLive = -1;
    private String JMSCorrelationID;
    private String JMSReplyTo;
    private String JMSType;
    private int JMSMessageCount = 1;
    private String JMSMessageCountHeader = "JMSXMessageNumber";
    private boolean redirectToBrowse;

    public SendMessage(BrokerFacade brokerFacade) {
        super(brokerFacade);
    }

    public ModelAndView handleRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
        WebClient client = WebClient.getWebClient(request);
        ActiveMQDestination dest = createDestination();

        sendMessages(request, client, dest);
        if (redirectToBrowse) {
            if (isQueue()) {
                return new ModelAndView("redirect:browse.jsp?destination=" + getJMSDestination());
            }
        }
        return redirectToBrowseView();
    }

    protected void sendMessages(HttpServletRequest request, WebClient client, ActiveMQDestination dest) throws JMSException {
        if (JMSMessageCount <= 1) {
            JMSMessageCount = 1;
        }
        for (int i = 0; i < JMSMessageCount; i++) {
            Message message = createMessage(client, request);
            appendHeaders(message, request);
            if (JMSMessageCount > 1) {
                message.setIntProperty(JMSMessageCountHeader, i + 1);
            }

            client.send(dest, message, JMSPersistent, JMSPriority, JMSTimeToLive);
        }
    }

    // Properties
    // -------------------------------------------------------------------------

    public String getJMSCorrelationID() {
        return JMSCorrelationID;
    }

    public void setJMSCorrelationID(String correlationID) {
        JMSCorrelationID = correlationID;
    }

    public String getJMSReplyTo() {
        return JMSReplyTo;
    }

    public void setJMSReplyTo(String replyTo) {
        JMSReplyTo = replyTo;
    }

    public String getJMSType() {
        return JMSType;
    }

    public void setJMSType(String type) {
        JMSType = type;
    }

    public boolean isJMSPersistent() {
        return JMSPersistent;
    }

    public void setJMSPersistent(boolean persistent) {
        this.JMSPersistent = persistent;
    }

    public int getJMSPriority() {
        return JMSPriority;
    }

    public void setJMSPriority(int priority) {
        this.JMSPriority = priority;
    }

    public String getJMSText() {
        return JMSText;
    }

    public void setJMSText(String text) {
        this.JMSText = text;
    }

    public int getJMSTimeToLive() {
        return JMSTimeToLive;
    }

    public void setJMSTimeToLive(int timeToLive) {
        this.JMSTimeToLive = timeToLive;
    }

    public int getJMSMessageCount() {
        return JMSMessageCount;
    }

    public void setJMSMessageCount(int copies) {
        JMSMessageCount = copies;
    }

    public String getJMSMessageCountHeader() {
        return JMSMessageCountHeader;
    }

    public void setJMSMessageCountHeader(String messageCountHeader) {
        JMSMessageCountHeader = messageCountHeader;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Message createMessage(WebClient client, HttpServletRequest request) throws JMSException {
        if (JMSText != null) {
            return client.getSession().createTextMessage(JMSText);
        }
        // TODO create Bytes message from request body...
        return client.getSession().createMessage();
    }

    protected void appendHeaders(Message message, HttpServletRequest request) throws JMSException {
        message.setJMSCorrelationID(JMSCorrelationID);
        if (JMSReplyTo != null && JMSReplyTo.trim().length() > 0) {
            message.setJMSReplyTo(ActiveMQDestination.createDestination(JMSReplyTo, ActiveMQDestination.QUEUE_TYPE));
        }
        message.setJMSType(JMSType);

        // now lets add all of the parameters
        Map map = request.getParameterMap();
        if (map != null) {
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext();) {
                Map.Entry entry = (Map.Entry) iter.next();
                String name = (String) entry.getKey();
                Object value = entry.getValue();
                if (isValidPropertyName(name)) {
                    if (value instanceof String[]) {
                        String[] array = (String[]) value;
                        if (array.length > 0) {
                            value = array[0];
                        }
                        else {
                            value = null;
                        }
                    }
                    if (value instanceof String) {
                        String text = value.toString().trim();
                        if (text.length() == 0) {
                            value = null;
                        }
                        else {
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

    protected boolean isValidPropertyName(String name) {
        // allow JMSX extensions or non JMS properties
        return name.startsWith("JMSX") || !name.startsWith("JMS");
    }
}

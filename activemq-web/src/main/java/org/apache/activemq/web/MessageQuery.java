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
package org.apache.activemq.web;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.QueueBrowser;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

/**
 * Allow the user to browse a message on a queue by its ID
 * 
 * 
 */
public class MessageQuery extends QueueBrowseQuery {

    private String id;
    private Message message;

    public MessageQuery(BrokerFacade brokerFacade, SessionPool sessionPool) throws JMSException {
        super(brokerFacade, sessionPool);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public Message getMessage() throws JMSException {
        if (message == null) {
            if (id != null) {
                QueueBrowser tempBrowser=getBrowser();
                Enumeration iter = tempBrowser.getEnumeration();
                while (iter.hasMoreElements()) {
                    Message item = (Message) iter.nextElement();
                    if (id.equals(item.getJMSMessageID())) {
                        message = item;
                        break;
                    }
                }
                tempBrowser.close();
            }

        }
        return message;
    }

    public Object getBody() throws JMSException {
        Message message = getMessage();
        if (message instanceof TextMessage) {
            return ((TextMessage) message).getText();
        }
        if (message instanceof ObjectMessage) {
            try {
                return ((ObjectMessage) message).getObject();
            } catch (Exception e) {
                //message could not be parsed, make the reason available
                return new String("Cannot display ObjectMessage body. Reason: " + e.getMessage());
            }
        }
        if (message instanceof MapMessage) {
            return createMapBody((MapMessage) message);
        }
        if (message instanceof BytesMessage) {
            BytesMessage msg = (BytesMessage) message;
            int len = (int) msg.getBodyLength();
            if (len > -1) {
                byte[] data = new byte[len];
                msg.readBytes(data);
                return new String(data);
            } else {
                return "";
            }
        }
        if (message instanceof StreamMessage) {
            return "StreamMessage is not viewable";
        }

        // unknown message type
        if (message != null) {
            return "Unknown message type [" + message.getClass().getName() + "] " + message;
        }

        return null;
    }
    
    public boolean isDLQ() throws Exception {
    	return getQueueView().isDLQ();
    }

    public Map<String, Object> getPropertiesMap() throws JMSException {
        Map<String, Object> answer = new HashMap<String, Object>();
        Message aMessage = getMessage();
        Enumeration iter = aMessage.getPropertyNames();
        while (iter.hasMoreElements()) {
            String name = (String) iter.nextElement();
            Object value = aMessage.getObjectProperty(name);
            if (value != null) {
                answer.put(name, value);
            }
        }
        return answer;
    }

    protected Map<String, Object> createMapBody(MapMessage mapMessage) throws JMSException {
        Map<String, Object> answer = new HashMap<String, Object>();
        Enumeration iter = mapMessage.getMapNames();
        while (iter.hasMoreElements()) {
            String name = (String) iter.nextElement();
            Object value = mapMessage.getObject(name);
            if (value != null) {
                answer.put(name, value);
            }
        }
        return answer;
    }
}

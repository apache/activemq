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
package org.apache.activemq.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.NamingException;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * An abstract base class for implementation inheritence for a log4j JMS
 * appender
 * 
 * @version $Revision$
 */
public abstract class JmsLogAppenderSupport extends AppenderSkeleton {

    public static final int JMS_PUBLISH_ERROR_CODE = 61616;

    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private boolean allowTextMessages = true;
    private String subjectPrefix = "log4j.";

    public JmsLogAppenderSupport() {
    }

    public Connection getConnection() throws JMSException, NamingException {
        if (connection == null) {
            connection = createConnection();
        }
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Session getSession() throws JMSException, NamingException {
        if (session == null) {
            session = createSession();
        }
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public MessageProducer getProducer() throws JMSException, NamingException {
        if (producer == null) {
            producer = createProducer();
        }
        return producer;
    }

    public void setProducer(MessageProducer producer) {
        this.producer = producer;
    }

    public void close() {
        List<JMSException> errors = new ArrayList<JMSException>();
        if (producer != null) {
            try {
                producer.close();
            } catch (JMSException e) {
                errors.add(e);
            }
        }
        if (session != null) {
            try {
                session.close();
            } catch (JMSException e) {
                errors.add(e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                errors.add(e);
            }
        }
        for (Iterator<JMSException> iter = errors.iterator(); iter.hasNext();) {
            JMSException e = iter.next();
            getErrorHandler().error("Error closing JMS resources: " + e, e, JMS_PUBLISH_ERROR_CODE);
        }
    }

    public boolean requiresLayout() {
        return false;
    }

    public void activateOptions() {
        try {
            // lets ensure we're all created
            getProducer();
        } catch (Exception e) {
            getErrorHandler().error("Could not create JMS resources: " + e, e, JMS_PUBLISH_ERROR_CODE);
        }
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected abstract Connection createConnection() throws JMSException, NamingException;

    protected Session createSession() throws JMSException, NamingException {
        return getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected MessageProducer createProducer() throws JMSException, NamingException {
        return getSession().createProducer(null);
    }

    protected void append(LoggingEvent event) {
        try {
            Message message = createMessage(event);
            Destination destination = getDestination(event);
            getProducer().send(destination, message);
        } catch (Exception e) {
            getErrorHandler().error("Could not send message due to: " + e, e, JMS_PUBLISH_ERROR_CODE, event);
        }
    }

    protected Message createMessage(LoggingEvent event) throws JMSException, NamingException {
        Message answer = null;
        Object value = event.getMessage();
        if (allowTextMessages && value instanceof String) {
            answer = getSession().createTextMessage((String)value);
        } else {
            answer = getSession().createObjectMessage((Serializable)value);
        }
        answer.setStringProperty("level", event.getLevel().toString());
        answer.setIntProperty("levelInt", event.getLevel().toInt());
        answer.setStringProperty("threadName", event.getThreadName());
        return answer;
    }

    protected Destination getDestination(LoggingEvent event) throws JMSException, NamingException {
        String name = subjectPrefix + event.getLoggerName();
        return getSession().createTopic(name);
    }
}

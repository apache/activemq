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

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;

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


/**
 * An abstract base class for implementation inheritence for a log4j JMS
 * appender
 */
public abstract class JmsLogAppenderSupport extends AbstractAppender {

    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private boolean allowTextMessages = true;
    private String subjectPrefix = "log4j.";

    public JmsLogAppenderSupport() {
        this("jmslog", (Filter) null);
    }

    protected JmsLogAppenderSupport(String name, Filter filter) {
        super(name, filter, (Layout) null, true);
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
            getHandler().error("Error closing JMS resources: " + e, e);
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
            getHandler().error("Could not create JMS resources: " + e, e);
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

    private static final ThreadLocal<Object> APPENDING = new ThreadLocal<Object>();

    public void append(LogEvent event) {
        if( APPENDING.get()==null ) {
            APPENDING.set(true);
            try {
                Message message = createMessage(event);
                Destination destination = getDestination(event);
                getProducer().send(destination, message);
            } catch (Exception e) {
                getHandler().error("Could not send message due to: " + e, event, e);
            } finally {
                APPENDING.remove();
            }
        }
    }

    protected Message createMessage(LogEvent event) throws JMSException, NamingException {
        Message answer = null;
        Object value = event.getMessage();
        if (allowTextMessages && value instanceof String) {
            answer = getSession().createTextMessage((String)value);
        } else {
            answer = getSession().createObjectMessage((Serializable)value);
        }
        answer.setStringProperty("level", event.getLevel().toString());
        answer.setIntProperty("levelInt", event.getLevel().intLevel());
        answer.setStringProperty("threadName", event.getThreadName());
        return answer;
    }

    protected Destination getDestination(LogEvent event) throws JMSException, NamingException {
        String name = subjectPrefix + event.getLoggerName();
        return getSession().createTopic(name);
    }
}

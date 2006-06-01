/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import java.util.Map;
import java.util.HashMap;

public class JmsConfigurableClientSupport extends JmsBasicClientSupport {
    private static final Log log = LogFactory.getLog(JmsConfigurableClientSupport.class);

    public static final String AMQ_SERVER = "amq";
    public static final String AMQ_CONNECTION_FACTORY_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";

    private String serverType = "";
    private String factoryClass = "";

    private Map factorySettings    = new HashMap();
    private Map connectionSettings = new HashMap();
    private Map sessionSettings    = new HashMap();
    private Map queueSettings      = new HashMap();
    private Map topicSettings      = new HashMap();
    private Map consumerSettings   = new HashMap();
    private Map producerSettings   = new HashMap();
    private Map messageSettings    = new HashMap();

    protected ConnectionFactory jmsFactory         = null;
    protected Connection        jmsConnection      = null;
    protected Session           jmsSession         = null;
    protected MessageProducer   jmsMessageProducer = null;
    protected MessageConsumer   jmsMessageConsumer = null;

    public ConnectionFactory createConnectionFactory(String url) {
        jmsFactory = super.createConnectionFactory(factoryClass, url, factorySettings);
        return jmsFactory;
    }

    public ConnectionFactory createConnectionFactory(String clazz, String url) {
        factoryClass = clazz;
        jmsFactory = super.createConnectionFactory(clazz, url, factorySettings);
        return jmsFactory;
    }

    public ConnectionFactory createConnectionFactory(String clazz, String url, Map props) {
        factoryClass = clazz;
        // Add previous settings to current settings
        props.putAll(factorySettings);
        jmsFactory = super.createConnectionFactory(clazz, url, props);
        return jmsFactory;
    }

    public ConnectionFactory getConnectionFactory() {
        return jmsFactory;
    }

    public Connection getConnection() throws JMSException {
        if (jmsConnection == null) {
            // Retrieve username and password parameter is they exist
            String username = (String)connectionSettings.get("username");
            String password = (String)connectionSettings.get("password");

            if (username == null) {
                username = "";
            }

            if (password == null) {
                password = "";
            }

            jmsConnection = getConnectionFactory().createConnection(username, password);
            configureJmsObject(jmsConnection, connectionSettings);
        }
        return jmsConnection;
    }

    public Session getSession() throws JMSException {
        if (jmsSession == null) {
            boolean transacted;

            // Check if session is transacted
            if (sessionSettings.get("transacted") != null && ((String)sessionSettings.get("transacted")).equals("true")) {
                transacted = true;
            } else {
                transacted = false;
            }

            // Check acknowledge type - default is AUTO_ACKNOWLEDGE
            String ackModeStr = (String)sessionSettings.get("ackMode");
            int ackMode = Session.AUTO_ACKNOWLEDGE;
            if (ackModeStr != null) {
                if (ackModeStr.equalsIgnoreCase("CLIENT_ACKNOWLEDGE")) {
                    ackMode = Session.CLIENT_ACKNOWLEDGE;
                } else if (ackModeStr.equalsIgnoreCase("DUPS_OK_ACKNOWLEDGE")) {
                    ackMode = Session.DUPS_OK_ACKNOWLEDGE;
                } else if (ackModeStr.equalsIgnoreCase("SESSION_TRANSACTED")) {
                    ackMode = Session.SESSION_TRANSACTED;
                }
            }

            jmsSession = getConnection().createSession(transacted, ackMode);
            configureJmsObject(jmsSession, sessionSettings);
        }
        return jmsSession;
    }

    public MessageProducer createMessageProducer(Destination dest) throws JMSException {
        jmsMessageProducer = getSession().createProducer(dest);
        configureJmsObject(jmsMessageProducer, producerSettings);
        return jmsMessageProducer;
    }

    public MessageProducer getMessageProducer() {
        return jmsMessageProducer;
    }

    public MessageConsumer createMessageConsumer(Destination dest, String selector, boolean noLocal) throws JMSException {
        jmsMessageConsumer = getSession().createConsumer(dest, selector, noLocal);
        configureJmsObject(jmsMessageConsumer, consumerSettings);
        return jmsMessageConsumer;
    }

    public MessageConsumer getMessageConsumer() {
        return jmsMessageConsumer;
    }

    public TopicSubscriber createDurableSubscriber(Topic dest, String name, String selector, boolean noLocal) throws JMSException {
        jmsMessageConsumer = getSession().createDurableSubscriber(dest, name, selector, noLocal);
        configureJmsObject(jmsMessageConsumer, consumerSettings);
        return (TopicSubscriber)jmsMessageConsumer;
    }

    public TopicSubscriber getDurableSubscriber() {
        return (TopicSubscriber)jmsMessageConsumer;
    }

    public TextMessage createTextMessage(String text) throws JMSException {
        TextMessage msg = getSession().createTextMessage(text);
        configureJmsObject(msg, messageSettings);
        return msg;
    }

    public Queue createQueue(String name) throws JMSException {
        Queue queue = getSession().createQueue(name);
        configureJmsObject(queue, queueSettings);
        return queue;
    }

    public Topic createTopic(String name) throws JMSException {
        Topic topic = getSession().createTopic(name);
        configureJmsObject(topic, topicSettings);
        return topic;
    }

    public void addConfigParam(String key, Object value) {
        // Simple mapping of JMS Server to connection factory class
        if (key.equalsIgnoreCase("server")) {
            serverType = value.toString();
            if (serverType.equalsIgnoreCase(AMQ_SERVER)) {
                factoryClass = AMQ_CONNECTION_FACTORY_CLASS;
            }

        // Manually specify the connection factory class to use
        } else if (key.equalsIgnoreCase("factoryClass")) {
            factoryClass = value.toString();

        // Connection factory specific settings
        } else if (key.startsWith("factory.")) {
            factorySettings.put(key.substring("factory.".length()), value);

        // Connection specific settings
        } else if (key.startsWith("connection.")) {
            connectionSettings.put(key.substring("session.".length()), value);

        // Session specific settings
        }  else if (key.startsWith("session.")) {
            sessionSettings.put(key.substring("session.".length()), value);

        // Destination specific settings
        } else if (key.startsWith("dest.")) {
            queueSettings.put(key.substring("dest.".length()), value);
            topicSettings.put(key.substring("dest.".length()), value);

        // Queue specific settings
        } else if (key.startsWith("queue.")) {
            queueSettings.put(key.substring("queue.".length()), value);

        // Topic specific settings
        } else if (key.startsWith("topic.")) {
            topicSettings.put(key.substring("topic.".length()), value);

        // Consumer specific settings
        } else if (key.startsWith("consumer.")) {
            consumerSettings.put(key.substring("consumer.".length()), value);

        // Producer specific settings
        } else if (key.startsWith("producer.")) {
            producerSettings.put(key.substring("producer.".length()), value);

        // Message specific settings
        } else if (key.startsWith("message.")) {
            messageSettings.put(key.substring("message.".length()), value);

        // Unknown settings
        } else {
            log.warn("Unknown setting: " + key + " = " + value);
        }
    }

    public void configureJmsObject(Object jmsObject, Map props) {
        if (props == null || props.isEmpty()) {
            return;
        }
        ReflectionUtil.configureClass(jmsObject, props);
    }

    public void configureJmsObject(Object jmsObject, String key, Object val) {
        if (key == null || key == "" || val == null) {
            return;
        }
        ReflectionUtil.configureClass(jmsObject, key, val);
    }
}

/**
 * 
 * Copyright 2004 Protique Ltd
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
 * 
 **/

package org.activemq.web;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionActivationListener;
import javax.servlet.http.HttpSessionEvent;

import org.activemq.ActiveMQConnection;
import org.activemq.ActiveMQConnectionFactory;
import org.activemq.ActiveMQSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.Semaphore;

/**
 * Represents a messaging client used from inside a web container
 * typically stored inside a HttpSession
 *
 * @version $Revision: 1.1.1.1 $
 */
public class WebClient implements HttpSessionActivationListener, Externalizable {
    public static final String webClientAttribute = "org.activemq.webclient";
    public static final String connectionFactoryAttribute = "org.activemq.connectionFactory";
    public static final String queueConsumersAttribute = "org.activemq.queueConsumers";
    public static final String brokerUrlInitParam = "org.activemq.brokerURL";

    private static final Log log = LogFactory.getLog(WebClient.class);

    private static transient ConnectionFactory factory;
    private static transient Map queueConsumers;

    private transient ServletContext context;
    private transient ActiveMQConnection connection;
    private transient ActiveMQSession session;
    private transient MessageProducer producer;
    private transient Map topicConsumers = new ConcurrentHashMap();
    private int deliveryMode = DeliveryMode.NON_PERSISTENT;

    private final Semaphore semaphore = new Semaphore(1);


    /**
     * @return the web client for the current HTTP session or null if there is not a web client created yet
     */
    public static WebClient getWebClient(HttpSession session) {
        return (WebClient) session.getAttribute(webClientAttribute);
    }


    public static void initContext(ServletContext context) {
        factory = initConnectionFactory(context);
        if (factory == null) {
            log.warn("No ConnectionFactory available in the ServletContext for: " + connectionFactoryAttribute);
            factory = new ActiveMQConnectionFactory("vm://localhost");
            context.setAttribute(connectionFactoryAttribute, factory);
        }
        queueConsumers = initQueueConsumers(context);
    }

    /**
     * Only called by serialization
     */
    public WebClient() {
    }

    public WebClient(ServletContext context) {
        this.context = context;
        initContext(context);
    }

    
    public int getDeliveryMode() {
        return deliveryMode;
    }


    public void setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }


    public void start() throws JMSException {
    }

    public void stop() throws JMSException {
        System.out.println("Closing the WebClient!!! " + this);
        
        try {
            connection.close();
        }
        finally {
            producer = null;
            session = null;
            connection = null;
            topicConsumers.clear();
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        topicConsumers = new HashMap();
    }

    public void send(Destination destination, Message message) throws JMSException {
        if (producer == null) {
            producer = getSession().createProducer(null);
            producer.setDeliveryMode(deliveryMode );
        }
        producer.send(destination, message);
        if (log.isDebugEnabled()) {
            log.debug("Sent! to destination: " + destination + " message: " + message);
        }
    }

    public Session getSession() throws JMSException {
        if (session == null) {
            session = createSession();
        }
        return session;
    }

    public ActiveMQConnection getConnection() throws JMSException {
        if (connection == null) {
            connection = (ActiveMQConnection) factory.createConnection();
            connection.start();
        }
        return connection;
    }

    public void sessionWillPassivate(HttpSessionEvent event) {
        try {
            stop();
        }
        catch (JMSException e) {
            log.warn("Could not close connection: " + e, e);
        }
    }

    public void sessionDidActivate(HttpSessionEvent event) {
        // lets update the connection factory from the servlet context
        context = event.getSession().getServletContext();
        initContext(context);
    }

    public static Map initQueueConsumers(ServletContext context) {
        Map answer = (Map) context.getAttribute(queueConsumersAttribute);
        if (answer == null) {
            answer = new HashMap();
            context.setAttribute(queueConsumersAttribute, answer);
        }
        return answer;
    }


    public static ConnectionFactory initConnectionFactory(ServletContext servletContext) {
        ConnectionFactory connectionFactory = (ConnectionFactory) servletContext.getAttribute(connectionFactoryAttribute);
        if (connectionFactory == null) {
            String brokerURL = (String) servletContext.getInitParameter(brokerUrlInitParam);

            servletContext.log("Value of: " + brokerUrlInitParam + " is: " + brokerURL);

            if (brokerURL == null) {
                brokerURL = "vm://localhost";
            }


            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
            connectionFactory = factory;
            servletContext.setAttribute(connectionFactoryAttribute, connectionFactory);
        }
        return connectionFactory;
    }

    public synchronized MessageConsumer getConsumer(Destination destination) throws JMSException {
        if (destination instanceof Topic) {
            MessageConsumer consumer = (MessageConsumer) topicConsumers.get(destination);
            if (consumer == null) {
                consumer = getSession().createConsumer(destination);
                topicConsumers.put(destination, consumer);
            }
            return consumer;
        }
        else {
            synchronized (queueConsumers) {
                SessionConsumerPair pair = (SessionConsumerPair) queueConsumers.get(destination);
                if (pair == null) {
                    pair = createSessionConsumerPair(destination);
                    queueConsumers.put(destination, pair);
                }
                return pair.consumer;
            }
        }
    }

    protected ActiveMQSession createSession() throws JMSException {
        return (ActiveMQSession) getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected SessionConsumerPair createSessionConsumerPair(Destination destination) throws JMSException {
        SessionConsumerPair answer = new SessionConsumerPair();
        answer.session = createSession();
        answer.consumer = answer.session.createConsumer(destination);
        return answer;
    }

    protected static class SessionConsumerPair {
        public Session session;
        public MessageConsumer consumer;
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }
}

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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionActivationListener;
import javax.servlet.http.HttpSessionBindingEvent;
import javax.servlet.http.HttpSessionBindingListener;
import javax.servlet.http.HttpSessionEvent;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.MessageAvailableConsumer;
import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.activemq.camel.component.ActiveMQConfiguration;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a messaging client used from inside a web container typically
 * stored inside a HttpSession TODO controls to prevent DOS attacks with users
 * requesting many consumers TODO configure consumers with small prefetch.
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class WebClient implements HttpSessionActivationListener, HttpSessionBindingListener, Externalizable {

    public static final String WEB_CLIENT_ATTRIBUTE = "org.apache.activemq.webclient";
    public static final String CONNECTION_FACTORY_ATTRIBUTE = "org.apache.activemq.connectionFactory";
    public static final String CONNECTION_FACTORY_PREFETCH_PARAM = "org.apache.activemq.connectionFactory.prefetch";
    public static final String CONNECTION_FACTORY_OPTIMIZE_ACK_PARAM = "org.apache.activemq.connectionFactory.optimizeAck";
    public static final String BROKER_URL_INIT_PARAM = "org.apache.activemq.brokerURL";
    public static final String SELECTOR_NAME = "org.apache.activemq.selectorName";

    private static final Logger LOG = LoggerFactory.getLogger(WebClient.class);

    private static transient ConnectionFactory factory;

    private transient Map<Destination, MessageConsumer> consumers = new HashMap<Destination, MessageConsumer>();
    private transient Connection connection;
    private transient Session session;
    private transient MessageProducer producer;
    private int deliveryMode = DeliveryMode.NON_PERSISTENT;
    public static String selectorName;

    private final Semaphore semaphore = new Semaphore(1);

    private CamelContext camelContext;
    private ProducerTemplate producerTemplate;

    public WebClient() {
        if (factory == null) {
            throw new IllegalStateException("initContext(ServletContext) not called");
        }
    }

    /**
     * Helper method to get the client for the current session, lazily creating
     * a client if there is none currently
     * 
     * @param request is the current HTTP request
     * @return the current client or a newly creates
     */
    public static WebClient getWebClient(HttpServletRequest request) {
        HttpSession session = request.getSession(true);
        WebClient client = getWebClient(session);
        if (client == null || client.isClosed()) {
            client = WebClient.createWebClient(request);
            session.setAttribute(WEB_CLIENT_ATTRIBUTE, client);
        }

        return client;
    }

    /**
     * @return the web client for the current HTTP session or null if there is
     *         not a web client created yet
     */
    public static WebClient getWebClient(HttpSession session) {
        return (WebClient)session.getAttribute(WEB_CLIENT_ATTRIBUTE);
    }

    public static void initContext(ServletContext context) {
        initConnectionFactory(context);
        context.setAttribute("webClients", new HashMap<String, WebClient>());
        if (selectorName == null) {
            selectorName = context.getInitParameter(SELECTOR_NAME);
        }
        if (selectorName == null) {
            selectorName = "selector";
        }        
    }

    public int getDeliveryMode() {
        return deliveryMode;
    }

    public void setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    public synchronized void closeConsumers() {
        for (Iterator<MessageConsumer> it = consumers.values().iterator(); it.hasNext();) {
            MessageConsumer consumer = it.next();
            it.remove();
            try {
                consumer.setMessageListener(null);
                if (consumer instanceof MessageAvailableConsumer) {
                    ((MessageAvailableConsumer)consumer).setAvailableListener(null);
                }
                consumer.close();
            } catch (JMSException e) {
                LOG.debug("caught exception closing consumer", e);
            }
        }
    }

    public synchronized void close() {
        try {
            if (consumers != null) {
                closeConsumers();
            }
            if (connection != null) {
                connection.close();
            }
            if (producerTemplate != null) {
            	producerTemplate.stop();
            }
        } catch (Exception e) {
            LOG.debug("caught exception closing consumer", e);
        } finally {
            producer = null;
            session = null;
            connection = null;
            producerTemplate = null;
            if (consumers != null) {
                consumers.clear();
            }
            consumers = null;

        }
    }

    public boolean isClosed() {
        return consumers == null;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (consumers != null) {
            out.write(consumers.size());
            Iterator<Destination> i = consumers.keySet().iterator();
            while (i.hasNext()) {
                out.writeObject(i.next().toString());
            }
        } else {
            out.write(-1);
        }

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        if (size >= 0) {
            consumers = new HashMap<Destination, MessageConsumer>();
            for (int i = 0; i < size; i++) {
                String destinationName = in.readObject().toString();

                try {
                    Destination destination = destinationName.startsWith("topic://") ? (Destination)getSession().createTopic(destinationName) : (Destination)getSession().createQueue(destinationName);
                    consumers.put(destination, getConsumer(destination, null, true));
                } catch (JMSException e) {
                    LOG.debug("Caought Exception ", e);
                    IOException ex = new IOException(e.getMessage());
                    ex.initCause(e.getCause() != null ? e.getCause() : e);
                    throw ex;

                }
            }
        }
    }

    public void send(Destination destination, Message message) throws JMSException {
        getProducer().send(destination, message);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sent! to destination: " + destination + " message: " + message);
        }
    }

    public void send(Destination destination, Message message, boolean persistent, int priority, long timeToLive) throws JMSException {
        int deliveryMode = persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
        getProducer().send(destination, message, deliveryMode, priority, timeToLive);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sent! to destination: " + destination + " message: " + message);
        }
    }

    public Session getSession() throws JMSException {
        if (session == null) {
            session = createSession();
        }
        return session;
    }

    public Connection getConnection() throws JMSException {
        if (connection == null) {
            connection = factory.createConnection();
            connection.start();
        }
        return connection;
    }

    protected static synchronized void initConnectionFactory(ServletContext servletContext) {
        if (factory == null) {
            factory = (ConnectionFactory)servletContext.getAttribute(CONNECTION_FACTORY_ATTRIBUTE);
        }
        if (factory == null) {
            String brokerURL = servletContext.getInitParameter(BROKER_URL_INIT_PARAM);

            LOG.debug("Value of: " + BROKER_URL_INIT_PARAM + " is: " + brokerURL);

            if (brokerURL == null) {
                throw new IllegalStateException("missing brokerURL (specified via " + BROKER_URL_INIT_PARAM + " init-Param");
            }

            ActiveMQConnectionFactory amqfactory = new ActiveMQConnectionFactory(brokerURL);

            // Set prefetch policy for factory
            if (servletContext.getInitParameter(CONNECTION_FACTORY_PREFETCH_PARAM) != null) {
                int prefetch = Integer.valueOf(servletContext.getInitParameter(CONNECTION_FACTORY_PREFETCH_PARAM)).intValue();
                amqfactory.getPrefetchPolicy().setAll(prefetch);
            }

            // Set optimize acknowledge setting
            if (servletContext.getInitParameter(CONNECTION_FACTORY_OPTIMIZE_ACK_PARAM) != null) {
                boolean optimizeAck = Boolean.valueOf(servletContext.getInitParameter(CONNECTION_FACTORY_OPTIMIZE_ACK_PARAM)).booleanValue();
                amqfactory.setOptimizeAcknowledge(optimizeAck);
            }

            factory = amqfactory;

            servletContext.setAttribute(CONNECTION_FACTORY_ATTRIBUTE, factory);
        }
    }
    
    public synchronized CamelContext getCamelContext() {
    	if (camelContext == null) {
    		LOG.debug("Creating camel context");
    		camelContext = new DefaultCamelContext();
    		ActiveMQConfiguration conf = new ActiveMQConfiguration();
    		conf.setConnectionFactory(new PooledConnectionFactory((ActiveMQConnectionFactory)factory));
    		ActiveMQComponent component = new ActiveMQComponent(conf);
    		camelContext.addComponent("activemq", component);
    	}
    	return camelContext;
    }
    
    public synchronized ProducerTemplate getProducerTemplate() throws Exception {
    	if (producerTemplate == null) {
    		LOG.debug("Creating producer template");
    		producerTemplate = getCamelContext().createProducerTemplate();
    		producerTemplate.start();
    	}
    	return producerTemplate;
    }

    public synchronized MessageProducer getProducer() throws JMSException {
        if (producer == null) {
            producer = getSession().createProducer(null);
            producer.setDeliveryMode(deliveryMode);
        }
        return producer;
    }

    public void setProducer(MessageProducer producer) {
        this.producer = producer;
    }

    public synchronized MessageConsumer getConsumer(Destination destination, String selector) throws JMSException {
        return getConsumer(destination, selector, true);
    }

    public synchronized MessageConsumer getConsumer(Destination destination, String selector, boolean create) throws JMSException {
        MessageConsumer consumer = consumers.get(destination);
        if (create && consumer == null) {
            consumer = getSession().createConsumer(destination, selector);
            consumers.put(destination, consumer);
        }
        return consumer;
    }

    public synchronized void closeConsumer(Destination destination) throws JMSException {
        MessageConsumer consumer = consumers.get(destination);
        if (consumer != null) {
            consumers.remove(destination);
            consumer.setMessageListener(null);
            if (consumer instanceof MessageAvailableConsumer) {
                ((MessageAvailableConsumer)consumer).setAvailableListener(null);
            }
            consumer.close();
        }
    }

    public synchronized List<MessageConsumer> getConsumers() {
        return new ArrayList<MessageConsumer>(consumers.values());
    }

    protected Session createSession() throws JMSException {
        return getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }

    public void sessionWillPassivate(HttpSessionEvent event) {
        close();
    }

    public void sessionDidActivate(HttpSessionEvent event) {
    }

    public void valueBound(HttpSessionBindingEvent event) {
    }

    public void valueUnbound(HttpSessionBindingEvent event) {
        close();
    }

    protected static WebClient createWebClient(HttpServletRequest request) {
        return new WebClient();
    }

}

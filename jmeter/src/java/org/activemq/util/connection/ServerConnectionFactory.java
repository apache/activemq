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
package org.activemq.util.connection;

import org.activemq.ActiveMQConnectionFactory;
import org.activemq.ActiveMQConnection;
import org.mr.api.jms.MantaTopicConnectionFactory;
import org.mr.api.jms.MantaQueueConnectionFactory;

import org.apache.jmeter.util.JMeterUtils;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.Queue;
import javax.jms.ConnectionFactory;
import javax.jms.TopicConnectionFactory;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.QueueSession;
import javax.jms.Destination;
import javax.jms.TopicConnection;
import javax.jms.QueueConnection;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Provides static methods for creating Session and Destination objects.
 */
public class ServerConnectionFactory {

    public static final String SONICMQ_SERVER = JMeterUtils.getResString("sonicmq_server");
    public static final String TIBCOMQ_SERVER = JMeterUtils.getResString("tibcomq_server");
    public static final String JBOSSMQ_SERVER = JMeterUtils.getResString("jbossmq_server");
    public static final String OPENJMS_SERVER = JMeterUtils.getResString("openjms_server");
    public static final String JORAM_SERVER  = JMeterUtils.getResString("joram_server");
    public static final String JORAM_CONNECTION_FACTORY = JMeterUtils.getResString("joram_connection_factory");
    public static final String JORAM_USERNAME = JMeterUtils.getResString("joram_username");
    public static final String JORAM_PASSWORD = JMeterUtils.getResString("joram_password");
    public static final String JORAM_NAMING_PORT = JMeterUtils.getResString("joram_naming_port");
    public static final String MANTARAY_SERVER = JMeterUtils.getResString("mantaray_server");

    // For testing within IntelliJ running main()
    /*
    public static final String SONICMQ_SERVER = "Sonic Server";
    public static final String TIBCOMQ_SERVER = "Tibco Server";
    public static final String JBOSSMQ_SERVER = "JbossMQ Server";
    public static final String OPENJMS_SERVER = "OpenJMS Server";
    public static final String ACTIVEMQ_SERVER = "ActiveMQ Server";
    public static final String JORAM_SERVER = "Joram Server";
    public static final String JORAM_CONNECTION_FACTORY = "!cf";
    public static final String JORAM_USERNAME = "root";
    public static final String JORAM_PASSWORD = "root";
    public static final String JORAM_NAMING_PORT = "16400";
    public static final String MANTARAY_SERVER = "Mantaray";
    */

    public static final String SONICMQ_TOPIC = "progress.message.jclient.TopicConnectionFactory";
    public static final String SONICMQ_QUEUE = "progress.message.jclient.QueueConnectionFactory";
    public static final String TIBCOMQ_TOPIC = "com.tibco.tibjms.TibjmsTopicConnectionFactory";
    public static final String TIBCOMQ_QUEUE = "com.tibco.tibjms.TibjmsQueueConnectionFactory";
    public static final String NAMING_CONTEXT = "org.jnp.interfaces.NamingContextFactory";
    public static final String JNP_INTERFACES = "org.jnp.interfaces";
    public static final String OPENJMS_NAMING_CONTEXT = "org.exolab.jms.jndi.InitialContextFactory";
    public static final String OPENJMS_TOPIC = "TcpTopicConnectionFactory";
    public static final String OPENJMS_QUEUE = "TcpQueueConnectionFactory";
    public static final String JORAM_NAMING_CONTEXT = "fr.dyade.aaa.jndi2.client.NamingContextFactory";
    public static final String JORAM_TOPIC = "TopicConnectionFactory";
    public static final String JORAM_QUEUE = "QueueConnectionFactory";
    public static final String NAMING_HOST = "java.naming.factory.host";
    public static final String NAMING_PORT = "java.naming.factory.post";

    public static Topic topicContext;

    private static int mantarayProducerPortCount = 0;
    private static int mantarayConsumerPortCount = 0;

    /**
     * Closes the connection passed through the parameter
     *
     * @param connection - Connection object to be closed.
     * @param session    - Session object to be closed.
     * @throws JMSException
     */
    public static void close(Connection connection, Session session) throws JMSException {
        session.close();
        connection.close();
    }

    /**
     * Dynamically creates a Connection object based on the type of broker.
     *
     * @param url            - location of the broker.
     * @param mqServer       - type of broker that is running.
     * @param isTopic        - type of message domain.
     * @param embeddedBroker - specified is the broker is embedded.
     * @return
     * @throws JMSException
     */
    public static Connection createConnectionFactory(String url,
                                                     String mqServer,
                                                     boolean isTopic,
                                                     boolean embeddedBroker) throws JMSException {
       if (SONICMQ_SERVER.equals(mqServer)) {
            //Creates a Connection object for a SONIC MQ server.
            if (isTopic) {
                return createConnectionFactory(url, SONICMQ_TOPIC);
            } else {
                return createConnectionFactory(url, SONICMQ_QUEUE);
            }
        } else if (TIBCOMQ_SERVER.equals(mqServer)) {
            //Creates a Connection object for a TIBCO MQ server.
            if (isTopic) {
                return createConnectionFactory(url, TIBCOMQ_TOPIC);
            } else {
                return createConnectionFactory(url, TIBCOMQ_QUEUE);
            }
        } else if (JBOSSMQ_SERVER.equals(mqServer)) {
            //Creates a Connection object for a JBoss MQ server.
            try {
                InitialContext context = getInitialContext(url, JBOSSMQ_SERVER);
                ConnectionFactory factory = (ConnectionFactory) context.lookup("ConnectionFactory");
                context.close();

                return factory.createConnection();

            } catch (NamingException e) {
                throw new JMSException("Error creating InitialContext ", e.toString());
            }
        } else if (OPENJMS_SERVER.equals(mqServer)) {
            //Creates a Connection object for a OpenJMS server.
            try {
                Context context = getInitialContext(url, OPENJMS_SERVER);
                if (isTopic) {
                    TopicConnectionFactory factory = (TopicConnectionFactory)
                            context.lookup(OPENJMS_TOPIC);
                    context.close();

                    return factory.createTopicConnection();

                } else {
                    QueueConnectionFactory factory = (QueueConnectionFactory)
                            context.lookup(OPENJMS_QUEUE);
                    context.close();

                    return factory.createQueueConnection();

                }
            } catch (NamingException e) {
                throw new JMSException("Error creating InitialContext ", e.toString());
            }
        } else if (JORAM_SERVER.equals(mqServer)) {
            //Creates a Connection object for a JORAM server.
            try {
                Context ictx = getInitialContext(url, JORAM_SERVER);
                ConnectionFactory cf = (ConnectionFactory) ictx.lookup(JORAM_CONNECTION_FACTORY);
                ictx.close();
                Connection cnx = cf.createConnection(JORAM_USERNAME, JORAM_PASSWORD);

                return cnx;

            } catch (NamingException e) {
                throw new JMSException("Error creating InitialContext ", e.toString());
            }
        } else if (MANTARAY_SERVER.equals(mqServer)) {
            //Creates a Connection object for a Mantaray.
            System.setProperty("mantaHome",url);

            if (isTopic) {
                TopicConnectionFactory factory = (TopicConnectionFactory) new MantaTopicConnectionFactory();

                return factory.createTopicConnection();

            } else {
                QueueConnectionFactory factory = (QueueConnectionFactory) new MantaQueueConnectionFactory();

                return factory.createQueueConnection();

            }
        } else {
            //Used to create a session from the default MQ server ActiveMQConnectionFactory.
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
            factory.setUseAsyncSend(true);

            if (embeddedBroker) {
                factory.setUseEmbeddedBroker(true);
            }
            
            ActiveMQConnection c = (ActiveMQConnection) factory.createConnection();

            c.getPrefetchPolicy().setQueuePrefetch(1000);
            c.getPrefetchPolicy().setQueueBrowserPrefetch(1000);
            c.getPrefetchPolicy().setTopicPrefetch(1000);
            c.getPrefetchPolicy().setDurableTopicPrefetch(1000);

            return c;

        }

    }

    /**
     * Creates a Destination object through Session using subject.
     *
     * @param session  - Session used to create the Destination.
     * @param subject  - the subject of the Destination to be created.
     * @param mqServer - ype of broker that is running.
     * @param url      - location of the broker.
     * @param isTopic  - specified is the broker is embedded.
     * @return
     * @throws JMSException
     */
    public static Destination createDestination(Session session,
                                                String subject,
                                                String url,
                                                String mqServer,
                                                boolean isTopic) throws JMSException {
        if (JBOSSMQ_SERVER.equals(mqServer)) {
            try {
                if (isTopic) {
                    return (Topic) getInitialContext(url, JBOSSMQ_SERVER).lookup("topic/" + subject);
                } else {
                    return (Queue) getInitialContext(url, JBOSSMQ_SERVER).lookup("queue/" + subject);
                }
            } catch (NamingException e) {
                throw new JMSException("Error on lookup for Queue " + subject, e.toString());
            }
        } else if (OPENJMS_SERVER.equals(mqServer)) {
            if (isTopic) {
                return ((TopicSession) session).createTopic(subject);
            } else {
                return ((QueueSession) session).createQueue(subject);
            }
        } else if (JORAM_SERVER.equals(mqServer)) {
            try {
                if (isTopic) {
                    return (Topic) getInitialContext(url, JORAM_SERVER).lookup(subject);
                } else {
                    return (Queue) getInitialContext(url, JORAM_SERVER).lookup(subject);
                }
            } catch (NamingException e) {
                throw new JMSException("Error on lookup for Queue " + subject, e.toString());
            }
        } else {
            if (isTopic) {
                return session.createTopic(subject);
            } else {
                return session.createQueue(subject);
            }
        }
    }

    /**
     * Creates a Session object.
     *
     * @param connection - Connection object where the session will be created from.
     * @return
     * @throws JMSException
     */
    public static Session createSession(Connection connection,
                                        boolean isTransacted,
                                        String mqServer,
                                        boolean isTopic) throws JMSException {
        if (OPENJMS_SERVER.equals(mqServer) || MANTARAY_SERVER.equals(mqServer)) {
            if (isTransacted) {
                if (isTopic) {
                    TopicSession session = ((TopicConnection) connection).createTopicSession(false, Session.SESSION_TRANSACTED);

                    return ((Session) session);

                } else {
                    QueueSession session = ((QueueConnection) connection).createQueueSession(false, Session.SESSION_TRANSACTED);

                    return ((Session) session);

                }
            } else {
                if (isTopic) {
                    TopicSession session = ((TopicConnection) connection).createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

                    return ((Session) session);

                } else {
                    QueueSession session = ((QueueConnection) connection).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

                    return ((Session) session);

                }
            }
        } else {
            // check when to use Transacted or Non-Transacted type.
            if (isTransacted) {
                return connection.createSession(true, Session.SESSION_TRANSACTED);
            } else {
                return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }
        }
    }

    /**
     * Dynamically creates a ConnectionFactory object depending on the MQ Factory class.
     *
     * @param url              - location of the broker.
     * @param connFactoryClass - fully qualified name of connection factory to be initialized.
     * @return
     * @throws JMSException
     */
    public static Connection createConnectionFactory(String url, String connFactoryClass) throws JMSException {
        Class classObject;
        Constructor constructor;
        Class[] classParameter = {url.getClass()};
        Object[] constArgs = {url};

        try {
            classObject = Class.forName(connFactoryClass);
            constructor = classObject.getConstructor(classParameter);
            ConnectionFactory factory = (ConnectionFactory) constructor.newInstance(constArgs);

            return factory.createConnection();

        } catch (ClassNotFoundException e) {
            throw new JMSException("Unable to find class ", e.toString());
        } catch (NoSuchMethodException e) {
            throw new JMSException("No such getConstructor(Class[] class) method found ", e.toString());
        } catch (InstantiationException e) {
            throw new JMSException("Unable to instantiate class ", e.toString());
        } catch (IllegalAccessException e) {
            throw new JMSException("Unable to instantiate class ", e.toString());
        } catch (InvocationTargetException e) {
            throw new JMSException("Unable to instantiate class ", e.toString());
        }
    }

    /**
     * Creates an InitialContext object which contains the information of the broker.
     * This is used if the broker uses JNDI.
     *
     * @param url - location of the broker.
     * @return
     * @throws JMSException
     */
    public static InitialContext getInitialContext(String url, String mqServer) throws JMSException {
        Properties properties = new Properties();

        if (JBOSSMQ_SERVER.equals(mqServer)) {
            //Creates a Context oject for JBOSS MQ server
            properties.put(Context.INITIAL_CONTEXT_FACTORY, NAMING_CONTEXT);
            properties.put(Context.URL_PKG_PREFIXES, JNP_INTERFACES);
            properties.put(Context.PROVIDER_URL, url);

        } else if (OPENJMS_SERVER.equals(mqServer)) {
            //Creates a Context object for OPENJMS server
            properties.put(Context.INITIAL_CONTEXT_FACTORY, OPENJMS_NAMING_CONTEXT);
            properties.put(Context.PROVIDER_URL, url);

        } else if (JORAM_SERVER.equals(mqServer)) {
            //Creates a Context object for JORAM server
            //The JNDI's host is set to be the same as with the Joram broker
            properties.put(Context.INITIAL_CONTEXT_FACTORY, JORAM_NAMING_CONTEXT);
            properties.put(NAMING_HOST, getHost(url));
            properties.put(NAMING_PORT, JORAM_NAMING_PORT);

        }

        try {
            return new InitialContext(properties);
        } catch (NamingException e) {
            throw new JMSException("Error creating InitialContext ", e.toString());
        }
    }

    /**
     * Returns the host part of the URL.
     *
     * @param url - location of the broker.
     * @return host
     */
    private static String getHost(String url) {
        return url.substring(url.lastIndexOf("/") + 1, url.lastIndexOf(":"));
    }

}

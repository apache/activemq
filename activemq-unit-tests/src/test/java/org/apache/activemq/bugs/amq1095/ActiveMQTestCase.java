/* ====================================================================
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
==================================================================== */

package org.apache.activemq.bugs.amq1095;

import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;

/**
 * <p>
 * Common functionality for ActiveMQ test cases.
 * </p>
 *
 * @author Rainer Klute <a
 *         href="mailto:rainer.klute@dp-itsolutions.de">&lt;rainer.klute@dp-itsolutions.de&gt;</a>
 * @since 2007-08-10
 * @version $Id: ActiveMQTestCase.java 12 2007-08-14 12:02:02Z rke $
 */
public class ActiveMQTestCase extends TestCase
{
    private Context context;
    private BrokerService broker;
    protected Connection connection;
    protected Destination destination;
    private final List<MessageConsumer> consumersToEmpty = new LinkedList<MessageConsumer>();
    protected final long RECEIVE_TIMEOUT = 500;


    /** <p>Constructor</p> */
    public ActiveMQTestCase()
    {}

    /** <p>Constructor</p>
     * @param name the test case's name
     */
    public ActiveMQTestCase(final String name)
    {
        super(name);
    }

    /**
     * <p>Sets up the JUnit testing environment.
     */
    @Override
    protected void setUp()
    {
        URI uri;
        try
        {
            /* Copy all system properties starting with "java.naming." to the initial context. */
            final Properties systemProperties = System.getProperties();
            final Properties jndiProperties = new Properties();
            for (final Iterator<Object> i = systemProperties.keySet().iterator(); i.hasNext();)
            {
                final String key = (String) i.next();
                if (key.startsWith("java.naming.") || key.startsWith("topic.") ||
                    key.startsWith("queue."))
                {
                    final String value = (String) systemProperties.get(key);
                    jndiProperties.put(key, value);
                }
            }
            context = new InitialContext(jndiProperties);
            uri = new URI("xbean:org/apache/activemq/bugs/amq1095/activemq.xml");
            broker = BrokerFactory.createBroker(uri);
            broker.start();
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }

        final ConnectionFactory connectionFactory;
        try
        {
            /* Lookup the connection factory. */
            connectionFactory = (ConnectionFactory) context.lookup("TopicConnectionFactory");

            destination = new ActiveMQTopic("TestTopic");

            /* Create a connection: */
            connection = connectionFactory.createConnection();
            connection.setClientID("sampleClientID");
        }
        catch (JMSException ex1)
        {
            ex1.printStackTrace();
            fail(ex1.toString());
        }
        catch (NamingException ex2) {
            ex2.printStackTrace();
            fail(ex2.toString());
        }
        catch (Throwable ex3) {
            ex3.printStackTrace();
            fail(ex3.toString());
        }
    }


    /**
     * <p>
     * Tear down the testing environment by receiving any messages that might be
     * left in the topic after a failure and shutting down the broker properly.
     * This is quite important for subsequent test cases that assume the topic
     * to be empty.
     * </p>
     */
    @Override
    protected void tearDown() throws Exception {
        TextMessage msg;
        try {
            for (final Iterator<MessageConsumer> i = consumersToEmpty.iterator(); i.hasNext();)
            {
                final MessageConsumer consumer = i.next();
                if (consumer != null)
                    do
                        msg = (TextMessage) consumer.receive(RECEIVE_TIMEOUT);
                    while (msg != null);
            }
        } catch (Exception e) {
        }
        if (connection != null) {
            connection.stop();
        }
        broker.stop();
    }

    protected void registerToBeEmptiedOnShutdown(final MessageConsumer consumer)
    {
        consumersToEmpty.add(consumer);
    }
}

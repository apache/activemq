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
package org.apache.activemq.security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.server.ldap.LdapServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith( FrameworkRunner.class )
@CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port=1024)})
@ApplyLdifFiles(
   "org/apache/activemq/security/activemq.ldif"
)
public class LDAPSecurityTest extends AbstractLdapTestUnit {

    public BrokerService broker;

    public static LdapServer ldapServer;

    @Before
    public void setup() throws Exception {
        System.setProperty("ldapPort", String.valueOf(getLdapServer().getPort()));

        broker = BrokerFactory.createBroker("xbean:org/apache/activemq/security/activemq-ldap.xml");
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void shutdown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test
    public void testSendReceive() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection conn = factory.createQueueConnection("jdoe", "sunflower");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();
        Destination queue = sess.createQueue("TEST.FOO");

        MessageProducer producer = sess.createProducer(queue);
        MessageConsumer consumer = sess.createConsumer(queue);

        producer.send(sess.createTextMessage("test"));
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
    }

    @Test
    public void testSendTopic() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection conn = factory.createQueueConnection("jdoe", "sunflower");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();
        Destination topic = sess.createTopic("TEST.BAR");

        MessageProducer producer = sess.createProducer(topic);
        MessageConsumer consumer = sess.createConsumer(topic);

        producer.send(sess.createTextMessage("test"));
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
    }

    @Test
    public void testSendDenied() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection conn = factory.createQueueConnection("jdoe", "sunflower");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();
        Queue queue = sess.createQueue("ADMIN.FOO");

        try {
            MessageProducer producer = sess.createProducer(queue);
            producer.send(sess.createTextMessage("test"));
            fail("expect auth exception");
        } catch (JMSException expected) {
        }
    }

    @Test
    public void testCompositeSendDenied() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection conn = factory.createQueueConnection("jdoe", "sunflower");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();
        Queue queue = sess.createQueue("TEST.FOO,ADMIN.FOO");

        try {
            MessageProducer producer = sess.createProducer(queue);
            producer.send(sess.createTextMessage("test"));
            fail("expect auth exception");
        } catch (JMSException expected) {
        }
    }

    @Test
    public void testTempDestinations() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection conn = factory.createQueueConnection("jdoe", "sunflower");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();
        Queue queue = sess.createTemporaryQueue();

        MessageProducer producer = sess.createProducer(queue);
        MessageConsumer consumer = sess.createConsumer(queue);

        producer.send(sess.createTextMessage("test"));
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
    }

}

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


import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SecurityJMXTest extends TestCase {
	
	private static final Log LOG = LogFactory.getLog(SimpleAuthenticationPluginTest.class);
	private BrokerService broker; 
	
	public void setUp() throws Exception {
		broker = createBroker();
		broker.waitUntilStarted();
		Thread.sleep(500);
	}

	public void tearDown() throws Exception {
	    broker.stop();
	}

    public void testMoveMessages() throws Exception {
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:1199/jmxrmi");
        JMXConnector connector = JMXConnectorFactory.connect(url, null);
        connector.connect();
        MBeanServerConnection connection = connector.getMBeanServerConnection();
        ObjectName name = new ObjectName("org.apache.activemq:BrokerName=localhost,Type=Queue,Destination=TEST.Q");
        QueueViewMBean queueMbean = (QueueViewMBean) MBeanServerInvocationHandler.newProxyInstance(connection, name, QueueViewMBean.class, true);
        String msgId = queueMbean.sendTextMessage("test", "system", "manager");
        queueMbean.moveMessageTo(msgId, "TEST1.Q");
    }
    
    
    public void testBrowseExpiredMessages() throws Exception {
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:1199/jmxrmi");
        JMXConnector connector = JMXConnectorFactory.connect(url, null);
        connector.connect();
        MBeanServerConnection connection = connector.getMBeanServerConnection();
        ObjectName name = new ObjectName("org.apache.activemq:BrokerName=localhost,Type=Queue,Destination=TEST.Q");
        QueueViewMBean queueMbean = (QueueViewMBean) MBeanServerInvocationHandler.newProxyInstance(connection, name, QueueViewMBean.class, true);
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put("JMSExpiration", Long.toString(System.currentTimeMillis() + 2000));
        headers.put("JMSDeliveryMode", Integer.toString(DeliveryMode.PERSISTENT));
        String msgId = queueMbean.sendTextMessage(headers, "test", "system", "manager");
        // allow message to expire on the queue
        TimeUnit.SECONDS.sleep(3);
        
        Connection c = new ActiveMQConnectionFactory("vm://localhost").createConnection("system", "manager");
        c.start();
        
        // browser consumer will force expriation check on addConsumer
        QueueBrowser browser = c.createSession(false, Session.AUTO_ACKNOWLEDGE).createBrowser(new ActiveMQQueue("TEST.Q"));
        assertTrue("no message in the q", !browser.getEnumeration().hasMoreElements());
        
        // verify dlq got the message, no security exception as brokers context is now used
        browser = c.createSession(false, Session.AUTO_ACKNOWLEDGE).createBrowser(new ActiveMQQueue("ActiveMQ.DLQ"));  
        assertTrue("one message in the dlq", browser.getEnumeration().hasMoreElements());
    }
    
	
    protected BrokerService createBroker() throws Exception {
        return createBroker("org/apache/activemq/security/simple-auth-broker.xml");
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }
	
}

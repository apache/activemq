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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.management.ObjectName;

import junit.framework.Test;

import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SimpleAuthenticationPluginTest extends SecurityTestSupport {

    private static final Log LOG = LogFactory.getLog(SimpleAuthenticationPluginTest.class);

    public static Test suite() {
        return suite(SimpleAuthenticationPluginTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected BrokerService createBroker() throws Exception {
        return createBroker("org/apache/activemq/security/simple-auth-broker.xml");
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }
    
    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestPredefinedDestinations() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST.Q")});
    }
    
    public void testPredefinedDestinations() throws JMSException {
    	Message sent = doSend(false);
        assertEquals("guest", ((ActiveMQMessage)sent).getUserID());
        assertEquals("guest", sent.getStringProperty("JMSXUserID"));
    }
    
    public void testTempDestinations() throws Exception {
        Connection conn = factory.createConnection("guest", "password");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String name = "org.apache.activemq:BrokerName=localhost,Type=TempTopic";
        try {
            conn.start();
            TemporaryTopic temp = sess.createTemporaryTopic();
            name += ",Destination=" + temp.getTopicName().replaceAll(":", "_");
            fail("Should have failed creating a temp topic");
        } catch (Exception ignore) {}
        
        ObjectName objName = new ObjectName(name);
        TopicViewMBean mbean = (TopicViewMBean)broker.getManagementContext().newProxyInstance(objName, TopicViewMBean.class, true);
        try {
            System.out.println(mbean.getName());
            fail("Shouldn't have created a temp topic");
        } catch (Exception ignore) {}
    }

}

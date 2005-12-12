/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.jndi;

import junit.framework.TestCase;
import org.activemq.ActiveMQConnectionFactory;

import javax.naming.InitialContext;
import javax.naming.Context;
import java.util.Properties;

/**
 * @version $Revision: 1.3 $
 */
public class InitialContextTest extends TestCase {
    
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(InitialContextTest.class);
    
    public void testInitialContext() throws Exception {
        InitialContext context = new InitialContext();
        assertTrue("Created context", context != null);

        ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) context.lookup("ConnectionFactory");

        assertTrue("Should have created a ConnectionFactory", connectionFactory != null);

        log.info("Created with brokerURL: " + connectionFactory.getBrokerURL());

    }

    public void testUsingStandardJNDIKeys() throws Exception {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.activemq.jndi.ActiveMQInitialContextFactory");
        String expected = "tcp://localhost:65432";
        properties.put(Context.PROVIDER_URL, expected);

        InitialContext context = new InitialContext(properties);
        assertTrue("Created context", context != null);

        ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) context.lookup("ConnectionFactory");

        assertTrue("Should have created a ConnectionFactory", connectionFactory != null);

        assertEquals("the brokerURL should match", expected, connectionFactory.getBrokerURL());
    }
}

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
package org.apache.activemq.jndi;

import java.util.Hashtable;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @version $Revision: 1.3 $
 */
public abstract class JNDITestSupport extends TestCase {
    
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(JNDITestSupport.class);
    
    protected Hashtable environment = new Hashtable();
    protected Context context;

    protected void assertConnectionFactoryPresent(String lookupName) throws NamingException {
        Object connectionFactory = context.lookup(lookupName);

        assertTrue("Should have created a ConnectionFactory for key: " + lookupName
                + " but got: " + connectionFactory, connectionFactory instanceof ConnectionFactory);
    }

    protected void assertBinding(Binding binding) throws NamingException {
        Object object = binding.getObject();
        assertTrue("Should have got a child context but got: " + object, object instanceof Context);

        Context childContext = (Context) object;
        NamingEnumeration iter = childContext.listBindings("");
        while (iter.hasMore()) {
            Binding destinationBinding = (Binding) iter.next();
            log.info("Found destination: " + destinationBinding.getName());
            Object destination = destinationBinding.getObject();
            assertTrue("Should have a Destination but got: " + destination, destination instanceof Destination);
        }
    }

    protected void setUp() throws Exception {
        super.setUp();

        configureEnvironment();

        InitialContextFactory factory = new ActiveMQInitialContextFactory();
        context = factory.getInitialContext(environment);
        assertTrue("No context created", context != null);
    }

    /**
     * Stops all existing ActiveMQConnectionFactory in Context.
     *
     * @throws javax.naming.NamingException
     */
    protected void tearDown() throws NamingException, JMSException {
        NamingEnumeration iter = context.listBindings("");
        while (iter.hasMore()) {
            Binding binding = (Binding) iter.next();
            Object connFactory = binding.getObject();
            if (connFactory instanceof ActiveMQConnectionFactory) {
               // ((ActiveMQConnectionFactory) connFactory).stop();
            }
        }
    }

    protected void configureEnvironment() {
        environment.put("brokerURL", "vm://localhost");
    }

    protected void assertDestinationExists(String name) throws NamingException {
        Object object = context.lookup(name);
        assertTrue("Should have received a Destination for name: " + name + " but instead found: " + object,
                object instanceof Destination);
    }
}

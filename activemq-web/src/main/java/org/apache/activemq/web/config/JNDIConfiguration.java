/*
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
package org.apache.activemq.web.config;

import java.util.Collection;

import javax.jms.ConnectionFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

/**
 * Configuration based on JNDI values.
 *
 * 
 */
public class JNDIConfiguration extends AbstractConfiguration {

    private static final String JNDI_JMS_CONNECTION_FACTORY = "java:comp/env/jms/connectionFactory";
    private static final String JNDI_JMS_URL = "java:comp/env/jms/url";
    private static final String JNDI_JMS_USER = "java:comp/env/jms/user";
    private static final String JNDI_JMS_PASSWORD = "java:comp/env/jms/password";

    private static final String JNDI_JMX_URL = "java:comp/env/jmx/url";
    private static final String JNDI_JMX_USER = "java:comp/env/jmx/user";
    private static final String JNDI_JMX_PASSWORD = "java:comp/env/jmx/password";

    private InitialContext context;

    public JNDIConfiguration() throws NamingException {
        this.context = new InitialContext();
    }

    public JNDIConfiguration(InitialContext context) {
        this.context = context;
    }

    public ConnectionFactory getConnectionFactory() {
        try {
            ConnectionFactory connectionFactory = (ConnectionFactory) this.context
                    .lookup(JNDI_JMS_CONNECTION_FACTORY);
            return connectionFactory;
        } catch (NameNotFoundException e) {
            // try to find an url
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }

        try {
            String jmsUrl = (String) this.context.lookup(JNDI_JMS_URL);
            if (jmsUrl == null) {
                throw new IllegalArgumentException(
                        "A JMS-url must be specified (system property "
                                + JNDI_JMS_URL);
            }

            String jmsUser = getJndiString(JNDI_JMS_USER);
            String jmsPassword = getJndiString(JNDI_JMS_PASSWORD);
            return makeConnectionFactory(jmsUrl, jmsUser, jmsPassword);
        } catch (NameNotFoundException e) {
            throw new IllegalArgumentException(
                    "Neither a ConnectionFactory nor a JMS-url were specified");
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    protected String getJndiString(String name) {
        try {
            return (String) this.context.lookup(name);
        } catch (NamingException e) {
            return null;
        }
    }

    public Collection<JMXServiceURL> getJmxUrls() {
        String jmxUrls = getJndiString(JNDI_JMX_URL);
        return makeJmxUrls(jmxUrls);
    }

    public String getJmxPassword() {
        return getJndiString(JNDI_JMX_PASSWORD);
    }

    public String getJmxUser() {
        return getJndiString(JNDI_JMX_USER);
    }

}

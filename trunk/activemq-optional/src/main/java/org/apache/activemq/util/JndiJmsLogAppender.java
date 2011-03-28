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
package org.apache.activemq.util;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.log4j.helpers.LogLog;

/**
 * A JMS 1.1 log4j appender which uses JNDI to locate a JMS ConnectionFactory to
 * use for logging events.
 * 
 * 
 */
public class JndiJmsLogAppender extends JmsLogAppenderSupport {

    private String jndiName;
    private String userName;
    private String password;

    private String initialContextFactoryName;
    private String providerURL;
    private String urlPkgPrefixes;
    private String securityPrincipalName;
    private String securityCredentials;

    public JndiJmsLogAppender() {
    }

    public String getJndiName() {
        return jndiName;
    }

    public void setJndiName(String jndiName) {
        this.jndiName = jndiName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    // to customize the JNDI context
    // -------------------------------------------------------------------------
    public String getInitialContextFactoryName() {
        return initialContextFactoryName;
    }

    public void setInitialContextFactoryName(String initialContextFactoryName) {
        this.initialContextFactoryName = initialContextFactoryName;
    }

    public String getProviderURL() {
        return providerURL;
    }

    public void setProviderURL(String providerURL) {
        this.providerURL = providerURL;
    }

    public String getUrlPkgPrefixes() {
        return urlPkgPrefixes;
    }

    public void setUrlPkgPrefixes(String urlPkgPrefixes) {
        this.urlPkgPrefixes = urlPkgPrefixes;
    }

    public String getSecurityPrincipalName() {
        return securityPrincipalName;
    }

    public void setSecurityPrincipalName(String securityPrincipalName) {
        this.securityPrincipalName = securityPrincipalName;
    }

    public String getSecurityCredentials() {
        return securityCredentials;
    }

    public void setSecurityCredentials(String securityCredentials) {
        this.securityCredentials = securityCredentials;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Connection createConnection() throws JMSException, NamingException {
        InitialContext context = createInitialContext();
        LogLog.debug("Looking up ConnectionFactory with jndiName: " + jndiName);
        ConnectionFactory factory = (ConnectionFactory)context.lookup(jndiName);
        if (factory == null) {
            throw new JMSException("No such ConnectionFactory for name: " + jndiName);
        }
        if (userName != null) {
            return factory.createConnection(userName, password);
        } else {
            return factory.createConnection();
        }
    }

    protected InitialContext createInitialContext() throws NamingException {
        if (initialContextFactoryName == null) {
            return new InitialContext();
        } else {
            Hashtable<String, String> env = new Hashtable<String, String>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactoryName);
            if (providerURL != null) {
                env.put(Context.PROVIDER_URL, providerURL);
            } else {
                LogLog.warn("You have set InitialContextFactoryName option but not the " + "ProviderURL. This is likely to cause problems.");
            }
            if (urlPkgPrefixes != null) {
                env.put(Context.URL_PKG_PREFIXES, urlPkgPrefixes);
            }

            if (securityPrincipalName != null) {
                env.put(Context.SECURITY_PRINCIPAL, securityPrincipalName);
                if (securityCredentials != null) {
                    env.put(Context.SECURITY_CREDENTIALS, securityCredentials);
                } else {
                    LogLog.warn("You have set SecurityPrincipalName option but not the " + "SecurityCredentials. This is likely to cause problems.");
                }
            }
            LogLog.debug("Looking up JNDI context with environment: " + env);
            return new InitialContext(env);
        }
    }
}

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

import java.net.URL;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

/**
 * Adds a JAAS based authentication security plugin
 * 
 * @org.apache.xbean.XBean description="Provides a JAAS based authentication plugin"
 * 
 * 
 */
public class JaasAuthenticationPlugin implements BrokerPlugin {
    protected String configuration = "activemq-domain";
    protected boolean discoverLoginConfig = true;

    public Broker installPlugin(Broker broker) {
        initialiseJaas();
        return new JaasAuthenticationBroker(broker, configuration);
    }


    // Properties
    // -------------------------------------------------------------------------
    public String getConfiguration() {
        return configuration;
    }

    /**
     * Sets the JAAS configuration domain name used
     */
    public void setConfiguration(String jaasConfiguration) {
        this.configuration = jaasConfiguration;
    }


    public boolean isDiscoverLoginConfig() {
        return discoverLoginConfig;
    }

    /**
     * Enables or disables the auto-discovery of the login.config file for JAAS to initialize itself. 
     * This flag is enabled by default such that if the <b>java.security.auth.login.config</b> system property
     * is not defined then it is set to the location of the <b>login.config</b> file on the classpath.
     */
    public void setDiscoverLoginConfig(boolean discoverLoginConfig) {
        this.discoverLoginConfig = discoverLoginConfig;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void initialiseJaas() {
        if (discoverLoginConfig) {
            String path = System.getProperty("java.security.auth.login.config");
            if (path == null) {
                //URL resource = Thread.currentThread().getContextClassLoader().getResource("login.config");
                URL resource = null;
                if (resource == null) {
                    resource = getClass().getClassLoader().getResource("login.config");
                }
                if (resource != null) {
                    path = resource.getFile();
                    System.setProperty("java.security.auth.login.config", path);
                }
            }
        }
    }
}

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
package org.apache.activemq.jaas.ldap;

import java.net.InetAddress;

import org.apache.ldap.server.configuration.ConfigurationException;
import org.apache.ldap.server.configuration.StartupConfiguration;
import org.apache.mina.registry.ServiceRegistry;
import org.apache.mina.registry.SimpleServiceRegistry;

/**
 * A {@link StartupConfiguration} that starts up ApacheDS with network layer support.
 *
 * @version $Rev: 233391 $ $Date: 2005-08-18 16:38:47 -0600 (Thu, 18 Aug 2005) $
 */
public class ServerStartupConfiguration extends StartupConfiguration {
    private static final long serialVersionUID = -7138616822614155454L;

    private boolean enableNetworking = true;
    private ServiceRegistry minaServiceRegistry = new SimpleServiceRegistry();
    private int ldapPort = 389;
    private int ldapsPort = 636;
    private InetAddress host;
    private boolean enableKerberos;

    protected ServerStartupConfiguration() {
    }

    protected InetAddress getHost() {
        return host;
    }

    protected void setHost(InetAddress host) {
        this.host = host;
    }

    /**
     * Returns <tt>true</tt> if networking (LDAP, LDAPS, and Kerberos) is enabled.
     */
    public boolean isEnableNetworking() {
        return enableNetworking;
    }

    /**
     * Sets whether to enable networking (LDAP, LDAPS, and Kerberos) or not.
     */
    public void setEnableNetworking(boolean enableNetworking) {
        this.enableNetworking = enableNetworking;
    }

    /**
     * Returns <tt>true</tt> if Kerberos support is enabled.
     */
    public boolean isEnableKerberos() {
        return enableKerberos;
    }

    /**
     * Sets whether to enable Kerberos support or not.
     */
    protected void setEnableKerberos(boolean enableKerberos) {
        this.enableKerberos = enableKerberos;
    }

    /**
     * Returns LDAP TCP/IP port number to listen to.
     */
    public int getLdapPort() {
        return ldapPort;
    }

    /**
     * Sets LDAP TCP/IP port number to listen to.
     */
    protected void setLdapPort(int ldapPort) {
        this.ldapPort = ldapPort;
    }

    /**
     * Returns LDAPS TCP/IP port number to listen to.
     */
    public int getLdapsPort() {
        return ldapsPort;
    }

    /**
     * Sets LDAPS TCP/IP port number to listen to.
     */
    protected void setLdapsPort(int ldapsPort) {
        this.ldapsPort = ldapsPort;
    }

    /**
     * Returns <a href="http://directory.apache.org/subprojects/network/">MINA</a>
     * {@link ServiceRegistry} that will be used by ApacheDS.
     */
    public ServiceRegistry getMinaServiceRegistry() {
        return minaServiceRegistry;
    }

    /**
     * Sets <a href="http://directory.apache.org/subprojects/network/">MINA</a>
     * {@link ServiceRegistry} that will be used by ApacheDS.
     */
    protected void setMinaServiceRegistry(ServiceRegistry minaServiceRegistry) {
        if (minaServiceRegistry == null) {
            throw new ConfigurationException("MinaServiceRegistry cannot be null");
        }
        this.minaServiceRegistry = minaServiceRegistry;
    }
}

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

import java.io.File;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;

import org.apache.mina.registry.ServiceRegistry;

/**
 * A mutable version of {@link ServerStartupConfiguration}.
 * 
 * @version $Rev: 233391 $ $Date: 2005-08-18 16:38:47 -0600 (Thu, 18 Aug 2005) $
 */
public class MutableServerStartupConfiguration extends ServerStartupConfiguration {
    private static final long serialVersionUID = 515104910980600099L;

    public MutableServerStartupConfiguration() {
        super();
    }

    public void setAllowAnonymousAccess(boolean arg0) {
        super.setAllowAnonymousAccess(arg0);
    }

    public void setAuthenticatorConfigurations(Set arg0) {
        super.setAuthenticatorConfigurations(arg0);
    }

    public void setBootstrapSchemas(Set arg0) {
        super.setBootstrapSchemas(arg0);
    }

    public void setContextPartitionConfigurations(Set arg0) {
        super.setContextPartitionConfigurations(arg0);
    }

    public void setInterceptorConfigurations(List arg0) {
        super.setInterceptorConfigurations(arg0);
    }

    public void setTestEntries(List arg0) {
        super.setTestEntries(arg0);
    }

    public void setWorkingDirectory(File arg0) {
        super.setWorkingDirectory(arg0);
    }

    public void setEnableKerberos(boolean enableKerberos) {
        super.setEnableKerberos(enableKerberos);
    }

    public void setHost(InetAddress host) {
        super.setHost(host);
    }

    public void setLdapPort(int ldapPort) {
        super.setLdapPort(ldapPort);
    }

    public void setLdapsPort(int ldapsPort) {
        super.setLdapsPort(ldapsPort);
    }

    public void setMinaServiceRegistry(ServiceRegistry minaServiceRegistry) {
        super.setMinaServiceRegistry(minaServiceRegistry);
    }
}

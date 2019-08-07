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

import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;


@RunWith( FrameworkRunner.class )
@CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP")})
@ApplyLdifFiles(
        "org/apache/activemq/security/activemq-apacheds-legacy.ldif"
)
public class CachedLDAPAuthorizationModuleLegacyTest extends AbstractCachedLDAPAuthorizationMapLegacyTest {

    @Override
    protected SimpleCachedLDAPAuthorizationMap createMap() {
        SimpleCachedLDAPAuthorizationMap map = super.createMap();
        map.setConnectionURL("ldap://localhost:" + getLdapServer().getPort());
        map.setConnectionPassword("secret");
        return map;
    }
    
    protected InputStream getAddLdif() {
        return getClass().getClassLoader().getResourceAsStream("org/apache/activemq/security/activemq-apacheds-legacy-add.ldif");
    }
    
    protected InputStream getRemoveLdif() {
        return getClass().getClassLoader().getResourceAsStream("org/apache/activemq/security/activemq-apacheds-legacy-delete.ldif");
    }
    
    protected String getQueueBaseDn() {
        return "ou=Queue,ou=Destination,ou=ActiveMQ,ou=system";
    }
    
    protected LdapConnection getLdapConnection() throws LdapException, IOException {
        LdapConnection connection = new LdapNetworkConnection("localhost", getLdapServer().getPort());
        connection.bind(new Dn("uid=admin,ou=system"), "secret");
        return connection;
    }
}


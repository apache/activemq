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

import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.apache.directory.shared.ldap.model.exception.LdapException;
import org.apache.directory.shared.ldap.model.name.Dn;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Test of the {@link SimpleCachedLDAPAuthorizationMap} that tests against a basic OpenLDAP instance.
 * Disabled by default because it requires external setup to provide the OpenLDAP instance.
 * 
 * To enable, you need an OpenLDAP with a minimum of the following in the slapd.conf file: 
 * 
 * suffix   "dc=apache,dc=org"
 * rootdn   "cn=Manager,dc=apache,dc=org"
 * rootpw   {SSHA}+Rx8kj98q3FlK5rUkT2hAtMP5v2ImQ82
 * 
 * If you wish to use different settings or don't use the default port, change the constants 
 * below for your environment.
 */
@Ignore
public class CachedLDAPAuthorizationModuleOpenLDAPTest extends AbstractCachedLDAPAuthorizationModuleTest {

    protected static final String LDAP_USER = "cn=Manager,dc=apache,dc=org";
    protected static final String LDAP_PASS = "password";
    protected static final String LDAP_HOST = "localhost";
    protected static final int LDAP_PORT = 389;
    
    @Before
    @Override
    public void setup() throws Exception {
        
        super.setup();
        
        cleanAndLoad("dc=apache,dc=org", "org/apache/activemq/security/activemq-openldap.ldif",
                LDAP_HOST, LDAP_PORT, LDAP_USER, LDAP_PASS, map.open());
    }
    
    @Test
    public void testRenameDestination() throws Exception {
        // Subtree rename not implemented by OpenLDAP.
    }
    
    @Override
    protected SimpleCachedLDAPAuthorizationMap createMap() {
        SimpleCachedLDAPAuthorizationMap newMap = super.createMap();
        newMap.setConnectionURL("ldap://" + LDAP_HOST + ":" + String.valueOf(LDAP_PORT));
        newMap.setConnectionUsername(LDAP_USER);
        newMap.setConnectionPassword(LDAP_PASS);
        // Persistent search is not supported in OpenLDAP
        newMap.setRefreshInterval(10);
        newMap.setQueueSearchBase("ou=Queue,ou=Destination,ou=ActiveMQ,dc=activemq,dc=apache,dc=org");
        newMap.setTopicSearchBase("ou=Topic,ou=Destination,ou=ActiveMQ,dc=activemq,dc=apache,dc=org");
        newMap.setTempSearchBase("ou=Temp,ou=Destination,ou=ActiveMQ,dc=activemq,dc=apache,dc=org");
        return newMap;
    }
    
    @Override
    protected InputStream getAddLdif() {
        return getClass().getClassLoader().getResourceAsStream("org/apache/activemq/security/activemq-openldap-add.ldif");
    }
    
    @Override
    protected InputStream getRemoveLdif() {
        return getClass().getClassLoader().getResourceAsStream("org/apache/activemq/security/activemq-openldap-delete.ldif");
    }
    
    @Override
    protected String getMemberAttributeValueForModifyRequest() {
        return "cn=users,ou=Group,ou=ActiveMQ,dc=activemq,dc=apache,dc=org";
    }
    
    @Override
    protected String getQueueBaseDn() {
        return "ou=Queue,ou=Destination,ou=ActiveMQ,dc=activemq,dc=apache,dc=org";
    }
    
    @Override
    protected LdapConnection getLdapConnection() throws LdapException, IOException {
        LdapConnection connection = new LdapNetworkConnection(LDAP_HOST, LDAP_PORT);
        connection.bind(new Dn(LDAP_USER), LDAP_PASS);
        return connection;
    }
}

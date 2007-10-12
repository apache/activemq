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
package org.apache.activemq.network;

import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * class to create dynamic network connectors listed in an directory
 * server using the LDAP v3 protocol as defined in RFC 2251, the
 * entries listed in the directory server must implement the ipHost
 * and ipService objectClasses as defined in RFC 2307.
 * 
 * @author Trevor Pounds
 * @see <a href="http://www.faqs.org/rfcs/rfc2251.html">RFC 2251</a>
 * @see <a href="http://www.faqs.org/rfcs/rfc2307.html">RFC 2307</a>
 *
 * @org.apache.xbean.XBean element="ldapNetworkConnector"
 */
public class LdapNetworkConnector extends NetworkConnector {
    private static final Log LOG = LogFactory.getLog(LdapNetworkConnector.class);

    // TODO: future >> LDAP JNDI event handling to update connectors?

    // force returned entries to implement the ipHost and ipService objectClasses (RFC 2307)
    private static final String REQUIRED_OBJECT_CLASS_FILTER  = "(&(objectClass=ipHost)(objectClass=ipService))";

    // required
    private URI    ldapURI;
    private String ldapBase;
    private String ldapUser;
    private String ldapPassword;

    // optional
    private int    ldapSearchScope  =  SearchControls.OBJECT_SCOPE;
    private String ldapSearchFilter =  REQUIRED_OBJECT_CLASS_FILTER;

    // internal configurables
    private DirContext ldapContext;
    private List<NetworkConnector> connectors = new CopyOnWriteArrayList<NetworkConnector>();

    /**
     * default constructor
     */
    public LdapNetworkConnector() {
    }

    /**
     * sets the LDAP server URI
     *
     * @param uri LDAP server URI
     */
    public void setUri(URI uri) {
        ldapURI = uri;
    }

    /**
     * sets the base LDAP dn used for lookup operations
     *
     * @param base LDAP base dn
     */
    public void setBase(String base) {
        ldapBase = base;
    }

    /**
     * sets the LDAP user for access credentials
     *
     * @param user LDAP dn of user
     */
    public void setUser(String user) {
        ldapUser = user;
    }

    /**
     * sets the LDAP password for access credentials
     *
     * @param password user password
     */
    public void setPassword(String password) {
        ldapPassword = password;
    }

    /**
     * sets the LDAP search scope
     *
     * @param searchScope LDAP JNDI search scope
     */
    public void setSearchScope(String searchScope) throws Exception {
        if(searchScope.equals("OBJECT_SCOPE")) {
            ldapSearchScope = SearchControls.OBJECT_SCOPE;
        }
        else if(searchScope.equals("ONELEVEL_SCOPE")) {
            ldapSearchScope = SearchControls.ONELEVEL_SCOPE;
        }
        else if(searchScope.equals("SUBTREE_SCOPE")) {
            ldapSearchScope = SearchControls.SUBTREE_SCOPE;
        }
        else {
          throw new Exception("ERR: unknown LDAP search scope specified: " + searchScope);
        }
    }

    /**
     * sets the LDAP search filter as defined in RFC 2254
     *
     * @param searchFilter LDAP search filter
     * @see <a href="http://www.faqs.org/rfcs/rfc2254.html">RFC 2254</a>
     */
    public void setSearchFilter(String searchFilter) {
        ldapSearchFilter = "(&" + REQUIRED_OBJECT_CLASS_FILTER + "(" + searchFilter + "))";
    }

    /**
     * start the connector
     */
    // XXX: this method seems awfully redundant when looking through the
    //      call stack when used in NetworkConnector based objects. I don't
    //      see why derived classes shouldn't just override the start/stop methods
    protected void handleStart() throws Exception {
        initLdapContext();
        for(URI uri : getLdapUris()) {
            NetworkConnector connector = getBrokerService().addNetworkConnector(uri);
            connector.start();
            connectors.add(connector);
        }
        super.handleStart();
    }

    /**
     * stop the connector
     *
     * @param stopper service stopper object
     */
    // XXX: this method seems awfully redundant when looking through the
    //      call stack when used in NetworkConnector based objects. I don't
    //      see why derived classes shouldn't just override the start/stop methods
    protected void handleStop(ServiceStopper stopper) throws Exception {
        for(NetworkConnector connector : connectors) {
            getBrokerService().removeNetworkConnector(connector);
            connector.stop();
        }
        ldapContext.close();
        super.handleStop(stopper);
    }

    /**
     * returns the name of the connector
     *
     * @return connector name
     */
    // XXX: this should probably be fixed elsewhere for all
    //      NetworkConnector derivatives...this impl does not
    //      seem to be well thought out?
    public String getName() {
        return toString();
    }

    /**
     * initializes the LDAP JNDI context with the configured parameters
     */
    protected void initLdapContext() throws Exception {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL,            ldapURI.toString());
        env.put(Context.SECURITY_PRINCIPAL,      ldapUser);
        env.put(Context.SECURITY_CREDENTIALS,    ldapPassword);
        ldapContext = new InitialDirContext(env);
    }

    /**
     * retrieves URIs matching the search filter via LDAP 
     * and creates network connectors based on the entries
     *
     * @returns list of retrieved URIs
     */
    protected List<URI> getLdapUris() throws Exception {
        SearchControls controls = new SearchControls();
        controls.setSearchScope(ldapSearchScope);
        NamingEnumeration<SearchResult> results = ldapContext.search(ldapBase, ldapSearchFilter, controls);

        List<URI> uriList = new ArrayList();
        while(results.hasMore()) {
            Attributes attributes = results.next().getAttributes();
            String address  = (String)attributes.get("iphostnumber").get();
            String port     = (String)attributes.get("ipserviceport").get();
            String protocol = (String)attributes.get("ipserviceprotocol").get();
            URI uri = new URI("static:(" + protocol + "://" + address + ":" + port + ")");
            LOG.info("Discovered URI " + uri);
            uriList.add(uri);
        }
        return uriList;
    }
}

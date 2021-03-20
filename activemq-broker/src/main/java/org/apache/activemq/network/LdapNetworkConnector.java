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
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.event.EventDirContext;
import javax.naming.event.NamespaceChangeListener;
import javax.naming.event.NamingEvent;
import javax.naming.event.NamingExceptionEvent;
import javax.naming.event.ObjectChangeListener;

import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.URISupport.CompositeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * class to create dynamic network connectors listed in an directory server
 * using the LDAP v3 protocol as defined in RFC 2251, the entries listed in the
 * directory server must implement the ipHost and ipService objectClasses as
 * defined in RFC 2307.
 *
 * @see <a href="http://www.faqs.org/rfcs/rfc2251.html">RFC 2251</a>
 * @see <a href="http://www.faqs.org/rfcs/rfc2307.html">RFC 2307</a>
 *
 * @org.apache.xbean.XBean element="ldapNetworkConnector"
 */
public class LdapNetworkConnector extends NetworkConnector implements NamespaceChangeListener, ObjectChangeListener {
    private static final Logger LOG = LoggerFactory.getLogger(LdapNetworkConnector.class);

    // force returned entries to implement the ipHost and ipService object classes (RFC 2307)
    private static final String REQUIRED_OBJECT_CLASS_FILTER =
            "(&(objectClass=ipHost)(objectClass=ipService))";

    // connection
    private URI[] availableURIs = null;
    private int availableURIsIndex = 0;
    private String base = null;
    private boolean failover = false;
    private long curReconnectDelay = 1000; /* 1 sec */
    private long maxReconnectDelay = 30000; /* 30 sec */

    // authentication
    private String user = null;
    private String password = null;
    private boolean anonymousAuthentication = false;

    // search
    private SearchControls searchControls = new SearchControls(/* ONELEVEL_SCOPE */);
    private String searchFilter = REQUIRED_OBJECT_CLASS_FILTER;
    private boolean searchEventListener = false;

    // connector management
    private Map<URI, NetworkConnector> connectorMap = new ConcurrentHashMap<URI, NetworkConnector>();
    private Map<URI, Integer> referenceMap = new ConcurrentHashMap<URI, Integer>();
    private Map<String, URI> uuidMap = new ConcurrentHashMap<String, URI>();

    // local context
    private DirContext context = null;
    // currently in use URI
    private URI ldapURI = null;

    /**
     * returns the next URI from the configured list
     *
     * @return random URI from the configured list
     */
    public URI getUri() {
        return availableURIs[++availableURIsIndex % availableURIs.length];
    }

    /**
     * sets the LDAP server URI
     *
     * @param uri
     *            LDAP server URI
     */
    public void setUri(URI uri) throws Exception {
        CompositeData data = URISupport.parseComposite(uri);
        if (data.getScheme().equals("failover")) {
            availableURIs = data.getComponents();
            failover = true;
        } else {
            availableURIs = new URI[] { uri };
        }
    }

    /**
     * sets the base LDAP dn used for lookup operations
     *
     * @param base
     *            LDAP base dn
     */
    public void setBase(String base) {
        this.base = base;
    }

    /**
     * sets the LDAP user for access credentials
     *
     * @param user
     *            LDAP dn of user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * sets the LDAP password for access credentials
     *
     * @param password
     *            user password
     */
    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * sets LDAP anonymous authentication access credentials
     *
     * @param anonymousAuthentication
     *            set to true to use anonymous authentication
     */
    public void setAnonymousAuthentication(boolean anonymousAuthentication) {
        this.anonymousAuthentication = anonymousAuthentication;
    }

    /**
     * sets the LDAP search scope
     *
     * @param searchScope
     *            LDAP JNDI search scope
     */
    public void setSearchScope(String searchScope) throws Exception {
        int scope;
        if (searchScope.equals("OBJECT_SCOPE")) {
            scope = SearchControls.OBJECT_SCOPE;
        } else if (searchScope.equals("ONELEVEL_SCOPE")) {
            scope = SearchControls.ONELEVEL_SCOPE;
        } else if (searchScope.equals("SUBTREE_SCOPE")) {
            scope = SearchControls.SUBTREE_SCOPE;
        } else {
            throw new Exception("ERR: unknown LDAP search scope specified: " + searchScope);
        }
        searchControls.setSearchScope(scope);
    }

    /**
     * sets the LDAP search filter as defined in RFC 2254
     *
     * @param searchFilter
     *            LDAP search filter
     * @see <a href="http://www.faqs.org/rfcs/rfc2254.html">RFC 2254</a>
     */
    public void setSearchFilter(String searchFilter) {
        this.searchFilter = "(&" + REQUIRED_OBJECT_CLASS_FILTER + "(" + searchFilter + "))";
    }

    /**
     * enables/disable a persistent search to the LDAP server as defined in
     * draft-ietf-ldapext-psearch-03.txt (2.16.840.1.113730.3.4.3)
     *
     * @param searchEventListener
     *            enable = true, disable = false (default)
     * @see <a
     *      href="http://www.ietf.org/proceedings/01mar/I-D/draft-ietf-ldapext-psearch-03.txt">draft-ietf-ldapext-psearch-03.txt</a>
     */
    public void setSearchEventListener(boolean searchEventListener) {
        this.searchEventListener = searchEventListener;
    }

    /**
     * start the connector
     */
    @Override
    public void start() throws Exception {
        LOG.info("connecting...");
        Hashtable<String, String> env = new Hashtable<String, String>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        this.ldapURI = getUri();
        LOG.debug("    URI [{}]", this.ldapURI);
        env.put(Context.PROVIDER_URL, this.ldapURI.toString());
        if (anonymousAuthentication) {
            LOG.debug("    login credentials [anonymous]");
            env.put(Context.SECURITY_AUTHENTICATION, "none");
        } else {
            LOG.debug("    login credentials [{}:******]", user);
            if (user != null && !"".equals(user)) {
                env.put(Context.SECURITY_PRINCIPAL, user);
            } else {
                throw new Exception("Empty username is not allowed");
            }
            if (password != null && !"".equals(password)) {
                env.put(Context.SECURITY_CREDENTIALS, password);
            } else {
                throw new Exception("Empty password is not allowed");
            }
        }
        boolean isConnected = false;
        while (!isConnected) {
            try {
                context = new InitialDirContext(env);
                isConnected = true;
            } catch (CommunicationException err) {
                if (failover) {
                    this.ldapURI = getUri();
                    LOG.error("connection error [{}], failover connection to [{}]", env.get(Context.PROVIDER_URL), this.ldapURI.toString());
                    env.put(Context.PROVIDER_URL, this.ldapURI.toString());
                    Thread.sleep(curReconnectDelay);
                    curReconnectDelay = Math.min(curReconnectDelay * 2, maxReconnectDelay);
                } else {
                    throw err;
                }
            }
        }

        // add connectors from search results
        LOG.info("searching for network connectors...");
        LOG.debug("    base   [{}]", base);
        LOG.debug("    filter [{}]", searchFilter);
        LOG.debug("    scope  [{}]", searchControls.getSearchScope());
        NamingEnumeration<SearchResult> results = context.search(base, searchFilter, searchControls);
        while (results.hasMore()) {
            addConnector(results.next());
        }

        // register persistent search event listener
        if (searchEventListener) {
            LOG.info("registering persistent search listener...");
            EventDirContext eventContext = (EventDirContext) context.lookup("");
            eventContext.addNamingListener(base, searchFilter, searchControls, this);
        } else { // otherwise close context (i.e. connection as it is no longer needed)
            context.close();
        }
    }

    /**
     * stop the connector
     */
    @Override
    public void stop() throws Exception {
        LOG.info("stopping context...");
        for (NetworkConnector connector : connectorMap.values()) {
            connector.stop();
        }
        connectorMap.clear();
        referenceMap.clear();
        uuidMap.clear();
        context.close();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + getName() + "[" + ldapURI.toString() + "]";
    }

    /**
     * add connector of the given URI
     *
     * @param result
     *            search result of connector to add
     */
    protected synchronized void addConnector(SearchResult result) throws Exception {
        String uuid = toUUID(result);
        if (uuidMap.containsKey(uuid)) {
            LOG.warn("connector already registered for UUID [{}]", uuid);
            return;
        }

        URI connectorURI = toURI(result);
        if (connectorMap.containsKey(connectorURI)) {
            int referenceCount = referenceMap.get(connectorURI) + 1;
            LOG.warn("connector reference added for URI [{}], UUID [{}], total reference(s) [{}]",connectorURI, uuid, referenceCount);
            referenceMap.put(connectorURI, referenceCount);
            uuidMap.put(uuid, connectorURI);
            return;
        }

        // FIXME: disable JMX listing of LDAP managed connectors, we will
        // want to map/manage these differently in the future
        // boolean useJMX = getBrokerService().isUseJmx();
        // getBrokerService().setUseJmx(false);
        NetworkConnector connector = getBrokerService().addNetworkConnector(connectorURI);
        // getBrokerService().setUseJmx(useJMX);

        // Propagate standard connector properties that may have been set via XML
        connector.setDynamicOnly(isDynamicOnly());
        connector.setDecreaseNetworkConsumerPriority(isDecreaseNetworkConsumerPriority());
        connector.setNetworkTTL(getNetworkTTL());
        connector.setConsumerTTL(getConsumerTTL());
        connector.setMessageTTL(getMessageTTL());
        connector.setConduitSubscriptions(isConduitSubscriptions());
        connector.setExcludedDestinations(getExcludedDestinations());
        connector.setDynamicallyIncludedDestinations(getDynamicallyIncludedDestinations());
        connector.setDuplex(isDuplex());

        // XXX: set in the BrokerService.startAllConnectors method and is
        // required to prevent remote broker exceptions upon connection
        connector.setLocalUri(getBrokerService().getVmConnectorURI());
        connector.setBrokerName(getBrokerService().getBrokerName());
        connector.setDurableDestinations(getBrokerService().getBroker().getDurableDestinations());

        // start network connector
        connectorMap.put(connectorURI, connector);
        referenceMap.put(connectorURI, 1);
        uuidMap.put(uuid, connectorURI);
        connector.start();
        LOG.info("connector added with URI [{}]", connectorURI);
    }

    /**
     * remove connector of the given URI
     *
     * @param result
     *            search result of connector to remove
     */
    protected synchronized void removeConnector(SearchResult result) throws Exception {
        String uuid = toUUID(result);
        if (!uuidMap.containsKey(uuid)) {
            LOG.warn("connector not registered for UUID [{}]", uuid);
            return;
        }

        URI connectorURI = uuidMap.get(uuid);
        if (!connectorMap.containsKey(connectorURI)) {
            LOG.warn("connector not registered for URI [{}]", connectorURI);
            return;
        }

        int referenceCount = referenceMap.get(connectorURI) - 1;
        referenceMap.put(connectorURI, referenceCount);
        uuidMap.remove(uuid);
        LOG.debug("connector referenced removed for URI [{}], UUID[{}], remaining reference(s) [{}]", connectorURI, uuid, referenceCount);

        if (referenceCount > 0) {
            return;
        }

        NetworkConnector connector = connectorMap.remove(connectorURI);
        connector.stop();
        LOG.info("connector removed with URI [{}]", connectorURI);
    }

    /**
     * convert search result into URI
     *
     * @param result
     *            search result to convert to URI
     */
    protected URI toURI(SearchResult result) throws Exception {
        Attributes attributes = result.getAttributes();
        String address = (String) attributes.get("iphostnumber").get();
        String port = (String) attributes.get("ipserviceport").get();
        String protocol = (String) attributes.get("ipserviceprotocol").get();
        URI connectorURI = new URI("static:(" + protocol + "://" + address + ":" + port + ")");
        LOG.debug("retrieved URI from SearchResult [{}]", connectorURI);
        return connectorURI;
    }

    /**
     * convert search result into URI
     *
     * @param result
     *            search result to convert to URI
     */
    protected String toUUID(SearchResult result) {
        String uuid = result.getNameInNamespace();
        LOG.debug("retrieved UUID from SearchResult [{}]", uuid);
        return uuid;
    }

    /**
     * invoked when an entry has been added during a persistent search
     */
    @Override
    public void objectAdded(NamingEvent event) {
        LOG.debug("entry added");
        try {
            addConnector((SearchResult) event.getNewBinding());
        } catch (Exception err) {
            LOG.error("ERR: caught unexpected exception", err);
        }
    }

    /**
     * invoked when an entry has been removed during a persistent search
     */
    @Override
    public void objectRemoved(NamingEvent event) {
        LOG.debug("entry removed");
        try {
            removeConnector((SearchResult) event.getOldBinding());
        } catch (Exception err) {
            LOG.error("ERR: caught unexpected exception", err);
        }
    }

    /**
     * invoked when an entry has been renamed during a persistent search
     */
    @Override
    public void objectRenamed(NamingEvent event) {
        LOG.debug("entry renamed");
        // XXX: getNameInNamespace method does not seem to work properly,
        // but getName seems to provide the result we want
        String uuidOld = event.getOldBinding().getName();
        String uuidNew = event.getNewBinding().getName();
        URI connectorURI = uuidMap.remove(uuidOld);
        uuidMap.put(uuidNew, connectorURI);
        LOG.debug("connector reference renamed for URI [{}], Old UUID [{}], New UUID [{}]", connectorURI, uuidOld, uuidNew);
    }

    /**
     * invoked when an entry has been changed during a persistent search
     */
    @Override
    public void objectChanged(NamingEvent event) {
        LOG.debug("entry changed");
        try {
            SearchResult result = (SearchResult) event.getNewBinding();
            removeConnector(result);
            addConnector(result);
        } catch (Exception err) {
            LOG.error("ERR: caught unexpected exception", err);
        }
    }

    /**
     * invoked when an exception has occurred during a persistent search
     */
    @Override
    public void namingExceptionThrown(NamingExceptionEvent event) {
        LOG.error("ERR: caught unexpected exception", event.getException());
    }
}

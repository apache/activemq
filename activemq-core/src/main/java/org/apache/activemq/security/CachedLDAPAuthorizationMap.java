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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.jaas.GroupPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.naming.event.*;
import java.util.*;

/**
 * A {@link DefaultAuthorizationMap} implementation which uses LDAP to initialize and update
 *
 * @org.apache.xbean.XBean
 *
 */
public class CachedLDAPAuthorizationMap extends DefaultAuthorizationMap implements NamespaceChangeListener,
        ObjectChangeListener, InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(CachedLDAPAuthorizationMap.class);


    private String initialContextFactory = "com.sun.jndi.ldap.LdapCtxFactory";
    private String connectionURL = "ldap://localhost:1024";
    private String connectionUsername = "uid=admin,ou=system";
    private String connectionPassword = "secret";
    private String connectionProtocol = "s";
    private String authentication = "simple";

    private String baseDn = "ou=system";
    private int cnsLength = 5;

    private int refreshInterval = -1;
    private long lastUpdated;

    private static String ANY_DESCENDANT = "\\$";

    private DirContext context;
    private EventDirContext eventContext;

    protected DirContext open() throws NamingException {
        if (context != null) {
            return context;
        }

        try {
            Hashtable<String, String> env = new Hashtable<String, String>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
            if (connectionUsername != null || !"".equals(connectionUsername)) {
                env.put(Context.SECURITY_PRINCIPAL, connectionUsername);
            }
            if (connectionPassword != null || !"".equals(connectionPassword)) {
                env.put(Context.SECURITY_CREDENTIALS, connectionPassword);
            }
            env.put(Context.SECURITY_PROTOCOL, connectionProtocol);
            env.put(Context.PROVIDER_URL, connectionURL);
            env.put(Context.SECURITY_AUTHENTICATION, authentication);
            context = new InitialDirContext(env);


            if (refreshInterval == -1) {
                eventContext = ((EventDirContext)context.lookup(""));
                final SearchControls constraints = new SearchControls();
                constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
                LOG.debug("Listening for: " + "'ou=Destination,ou=ActiveMQ," + baseDn + "'");
                eventContext.addNamingListener("ou=Destination,ou=ActiveMQ," + baseDn, "cn=*", constraints, this);
            }
        } catch (NamingException e) {
            LOG.error(e.toString());
            throw e;
        }
        return context;
    }

    HashMap<ActiveMQDestination, AuthorizationEntry> entries = new HashMap<ActiveMQDestination, AuthorizationEntry>();

    @SuppressWarnings("rawtypes")
    public void query() throws Exception {
        try {
            context = open();
        } catch (NamingException e) {
            LOG.error(e.toString());
        }

        final SearchControls constraints = new SearchControls();
        constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);

        NamingEnumeration<?> results = context.search("ou=Destination,ou=ActiveMQ," + baseDn, "(|(cn=admin)(cn=write)(cn=read))", constraints);
        while (results.hasMore()) {
            SearchResult result = (SearchResult) results.next();
            AuthorizationEntry entry = getEntry(result.getNameInNamespace());
            applyACL(entry, result);
        }

        setEntries(new ArrayList<DestinationMapEntry>(entries.values()));
        updated();
    }

    protected void updated() {
        lastUpdated = System.currentTimeMillis();
    }

    protected AuthorizationEntry getEntry(String name) {;
            String[] cns = name.split(",");

            // handle temp entry
            if (cns.length == cnsLength && cns[1].equals("ou=Temp")) {
                TempDestinationAuthorizationEntry tempEntry = getTempDestinationAuthorizationEntry();
                if (tempEntry == null) {
                    tempEntry = new TempDestinationAuthorizationEntry();
                    setTempDestinationAuthorizationEntry(tempEntry);
                }
                return tempEntry;
            }

            // handle regular destinations
            if (cns.length != (cnsLength + 1)) {
                LOG.warn("Policy not applied! Wrong cn for authorization entry " + name);
            }

            ActiveMQDestination dest = formatDestination(cns[1], cns[2]);

            if (dest != null) {
                AuthorizationEntry entry = entries.get(dest);
                if (entry == null) {
                    entry = new AuthorizationEntry();
                    entry.setDestination(dest);
                    entries.put(dest, entry);
                }
                return entry;
            } else {
                return null;
            }
    }

    protected ActiveMQDestination formatDestination(String destinationName, String destinationType) {
            ActiveMQDestination dest = null;
            if (destinationType.equalsIgnoreCase("ou=queue")) {
               dest = new ActiveMQQueue(formatDestinationName(destinationName));
            } else if (destinationType.equalsIgnoreCase("ou=topic")) {
               dest = new ActiveMQTopic(formatDestinationName(destinationName));
            } else {
                LOG.warn("Policy not applied! Unknown destination type " + destinationType);
            }
            return dest;
    }

    protected void applyACL(AuthorizationEntry entry, SearchResult result) throws NamingException {
        // find members
        Attribute cn = result.getAttributes().get("cn");
        Attribute member = result.getAttributes().get("member");
        NamingEnumeration<?> memberEnum = member.getAll();
        HashSet<Object> members = new HashSet<Object>();
        while (memberEnum.hasMoreElements()) {
            String elem = (String) memberEnum.nextElement();
            members.add(new GroupPrincipal(elem.replaceAll("cn=", "")));
        }

        // apply privilege
        if (cn.get().equals("admin")) {
            entry.setAdminACLs(members);
        } else if (cn.get().equals("write")) {
            entry.setWriteACLs(members);
        } else if (cn.get().equals("read")) {
            entry.setReadACLs(members);
        } else {
            LOG.warn("Policy not applied! Unknown privilege " + result.getName());
        }
    }

    protected String formatDestinationName(String cn) {
        return cn.replaceFirst("cn=", "").replaceAll(ANY_DESCENDANT, ">");
    }

    protected boolean isPriviledge(Binding binding) {
        String name = binding.getName();
        if (name.startsWith("cn=admin") || name.startsWith("cn=write") || name.startsWith("cn=read")) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected Set<AuthorizationEntry> getAllEntries(ActiveMQDestination destination) {
        if (refreshInterval != -1 && System.currentTimeMillis() >= lastUpdated + refreshInterval) {

            reset();
            entries.clear();

            LOG.debug("Updating authorization map!");
            try {
                query();
            } catch (Exception e) {
                LOG.error("Error updating authorization map", e);
            }
        }

        return super.getAllEntries(destination);
    }

    @Override
    public void objectAdded(NamingEvent namingEvent) {
        LOG.debug("Adding object: " + namingEvent.getNewBinding());
        SearchResult result = (SearchResult)namingEvent.getNewBinding();
        if (!isPriviledge(result)) return;
        AuthorizationEntry entry = getEntry(result.getName());
        if (entry != null) {
            try {
                applyACL(entry, result);
                if (!(entry instanceof TempDestinationAuthorizationEntry)) {
                    put(entry.getDestination(), entry);
                }
            } catch (NamingException ne) {
                LOG.warn("Unable to add entry", ne);
            }
        }
    }

    @Override
    public void objectRemoved(NamingEvent namingEvent) {
        LOG.debug("Removing object: " + namingEvent.getOldBinding());
        Binding result = namingEvent.getOldBinding();
        if (!isPriviledge(result)) return;
        AuthorizationEntry entry = getEntry(result.getName());
        String[] cns = result.getName().split(",");
        if (!isPriviledge(result)) return;
        if (cns[0].equalsIgnoreCase("cn=admin")) {
            entry.setAdminACLs(new HashSet<Object>());
        } else if (cns[0].equalsIgnoreCase("cn=write")) {
            entry.setWriteACLs(new HashSet<Object>());
        } else if (cns[0].equalsIgnoreCase("cn=read")) {
            entry.setReadACLs(new HashSet<Object>());
        } else {
            LOG.warn("Policy not removed! Unknown privilege " + result.getName());
        }
    }

    @Override
    public void objectRenamed(NamingEvent namingEvent) {
        Binding oldBinding = namingEvent.getOldBinding();
        Binding newBinding = namingEvent.getNewBinding();
        LOG.debug("Renaming object: " + oldBinding + " to " + newBinding);

        String[] oldCns = oldBinding.getName().split(",");
        ActiveMQDestination oldDest = formatDestination(oldCns[0], oldCns[1]);

        String[] newCns = newBinding.getName().split(",");
        ActiveMQDestination newDest = formatDestination(newCns[0], newCns[1]);

        if (oldDest != null && newDest != null) {
            AuthorizationEntry entry = entries.remove(oldDest);
            if (entry != null) {
                entry.setDestination(newDest);
                put(newDest, entry);
                remove(oldDest, entry);
            } else {
                LOG.warn("No authorization entry for " + oldDest);
            }
        }
    }

    @Override
    public void objectChanged(NamingEvent namingEvent) {
        LOG.debug("Changing object " + namingEvent.getOldBinding() + " to " + namingEvent.getNewBinding());
        objectRemoved(namingEvent);
        objectAdded(namingEvent);
    }

    @Override
    public void namingExceptionThrown(NamingExceptionEvent namingExceptionEvent) {
        LOG.error("Caught Unexpected Exception", namingExceptionEvent.getException());
    }

    // init

    @Override
    public void afterPropertiesSet() throws Exception {
        query();
    }

    // getters and setters

    public String getConnectionURL() {
        return connectionURL;
    }

    public void setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
    }

    public String getConnectionUsername() {
        return connectionUsername;
    }

    public void setConnectionUsername(String connectionUsername) {
        this.connectionUsername = connectionUsername;
    }

    public String getConnectionPassword() {
        return connectionPassword;
    }

    public void setConnectionPassword(String connectionPassword) {
        this.connectionPassword = connectionPassword;
    }

    public String getConnectionProtocol() {
        return connectionProtocol;
    }

    public void setConnectionProtocol(String connectionProtocol) {
        this.connectionProtocol = connectionProtocol;
    }

    public String getAuthentication() {
        return authentication;
    }

    public void setAuthentication(String authentication) {
        this.authentication = authentication;
    }

    public String getBaseDn() {
        return baseDn;
    }

    public void setBaseDn(String baseDn) {
        this.baseDn = baseDn;
        cnsLength = baseDn.split(",").length + 4;
    }

    public int getRefreshInterval() {
        return refreshInterval;
    }

    public void setRefreshInterval(int refreshInterval) {
        this.refreshInterval = refreshInterval;
    }
}


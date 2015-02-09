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

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.jaas.LDAPLoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AuthorizationMap} which uses LDAP
 *
 * @org.apache.xbean.XBean
 * @author ngcutura
 */
public class LDAPAuthorizationMap implements AuthorizationMap {

    public static final String INITIAL_CONTEXT_FACTORY = "initialContextFactory";
    public static final String CONNECTION_URL = "connectionURL";
    public static final String CONNECTION_USERNAME = "connectionUsername";
    public static final String CONNECTION_PASSWORD = "connectionPassword";
    public static final String CONNECTION_PROTOCOL = "connectionProtocol";
    public static final String AUTHENTICATION = "authentication";

    public static final String TOPIC_SEARCH_MATCHING = "topicSearchMatching";
    public static final String TOPIC_SEARCH_SUBTREE = "topicSearchSubtree";
    public static final String QUEUE_SEARCH_MATCHING = "queueSearchMatching";
    public static final String QUEUE_SEARCH_SUBTREE = "queueSearchSubtree";

    public static final String ADMIN_BASE = "adminBase";
    public static final String ADMIN_ATTRIBUTE = "adminAttribute";
    public static final String READ_BASE = "readBase";
    public static final String READ_ATTRIBUTE = "readAttribute";
    public static final String WRITE_BASE = "writeBAse";
    public static final String WRITE_ATTRIBUTE = "writeAttribute";

    private static final Logger LOG = LoggerFactory.getLogger(LDAPLoginModule.class);

    private String initialContextFactory;
    private String connectionURL;
    private String connectionUsername;
    private String connectionPassword;
    private String connectionProtocol;
    private String authentication;

    private DirContext context;

    private MessageFormat topicSearchMatchingFormat;
    private MessageFormat queueSearchMatchingFormat;
    private String advisorySearchBase = "uid=ActiveMQ.Advisory,ou=topics,ou=destinations,o=ActiveMQ,dc=example,dc=com";
    private String tempSearchBase = "uid=ActiveMQ.Temp,ou=topics,ou=destinations,o=ActiveMQ,dc=example,dc=com";

    private boolean topicSearchSubtreeBool = true;
    private boolean queueSearchSubtreeBool = true;
    private boolean useAdvisorySearchBase = true;

    private String adminBase;
    private String adminAttribute;
    private String readBase;
    private String readAttribute;
    private String writeBase;
    private String writeAttribute;

    public LDAPAuthorizationMap() {
        // lets setup some sensible defaults
        initialContextFactory = "com.sun.jndi.ldap.LdapCtxFactory";
        connectionURL = "ldap://localhost:10389";
        connectionUsername = "uid=admin,ou=system";
        connectionPassword = "secret";
        connectionProtocol = "s";
        authentication = "simple";

        topicSearchMatchingFormat = new MessageFormat("uid={0},ou=topics,ou=destinations,o=ActiveMQ,dc=example,dc=com");
        queueSearchMatchingFormat = new MessageFormat("uid={0},ou=queues,ou=destinations,o=ActiveMQ,dc=example,dc=com");


        adminBase = "(cn=admin)";
        adminAttribute = "uniqueMember";
        readBase = "(cn=read)";
        readAttribute = "uniqueMember";
        writeBase = "(cn=write)";
        writeAttribute = "uniqueMember";
    }

    public LDAPAuthorizationMap(Map<String,String> options) {
        initialContextFactory = options.get(INITIAL_CONTEXT_FACTORY);
        connectionURL = options.get(CONNECTION_URL);
        connectionUsername = options.get(CONNECTION_USERNAME);
        connectionPassword = options.get(CONNECTION_PASSWORD);
        connectionProtocol = options.get(CONNECTION_PROTOCOL);
        authentication = options.get(AUTHENTICATION);

        adminBase = options.get(ADMIN_BASE);
        adminAttribute = options.get(ADMIN_ATTRIBUTE);
        readBase = options.get(READ_BASE);
        readAttribute = options.get(READ_ATTRIBUTE);
        writeBase = options.get(WRITE_BASE);
        writeAttribute = options.get(WRITE_ATTRIBUTE);

        String topicSearchMatching = options.get(TOPIC_SEARCH_MATCHING);
        String topicSearchSubtree = options.get(TOPIC_SEARCH_SUBTREE);
        String queueSearchMatching = options.get(QUEUE_SEARCH_MATCHING);
        String queueSearchSubtree = options.get(QUEUE_SEARCH_SUBTREE);
        topicSearchMatchingFormat = new MessageFormat(topicSearchMatching);
        queueSearchMatchingFormat = new MessageFormat(queueSearchMatching);
        topicSearchSubtreeBool = Boolean.valueOf(topicSearchSubtree).booleanValue();
        queueSearchSubtreeBool = Boolean.valueOf(queueSearchSubtree).booleanValue();
    }

    public Set<GroupPrincipal> getTempDestinationAdminACLs() {
        try {
            context = open();
        } catch (NamingException e) {
            LOG.error(e.toString());
            return new HashSet<GroupPrincipal>();
        }
        SearchControls constraints = new SearchControls();
        constraints.setReturningAttributes(new String[] {adminAttribute});
        return getACLs(tempSearchBase, constraints, adminBase, adminAttribute);
    }

    public Set<GroupPrincipal> getTempDestinationReadACLs() {
        try {
            context = open();
        } catch (NamingException e) {
            LOG.error(e.toString());
            return new HashSet<GroupPrincipal>();
        }
        SearchControls constraints = new SearchControls();
        constraints.setReturningAttributes(new String[] {readAttribute});
        return getACLs(tempSearchBase, constraints, readBase, readAttribute);
    }

    public Set<GroupPrincipal> getTempDestinationWriteACLs() {
        try {
            context = open();
        } catch (NamingException e) {
            LOG.error(e.toString());
            return new HashSet<GroupPrincipal>();
        }
        SearchControls constraints = new SearchControls();
        constraints.setReturningAttributes(new String[] {writeAttribute});
        return getACLs(tempSearchBase, constraints, writeBase, writeAttribute);
    }

    public Set<GroupPrincipal> getAdminACLs(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            return getCompositeACLs(destination, adminBase, adminAttribute);
        }
        return getACLs(destination, adminBase, adminAttribute);
    }

    public Set<GroupPrincipal> getReadACLs(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            return getCompositeACLs(destination, readBase, readAttribute);
        }
        return getACLs(destination, readBase, readAttribute);
    }

    public Set<GroupPrincipal> getWriteACLs(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            return getCompositeACLs(destination, writeBase, writeAttribute);
        }
        return getACLs(destination, writeBase, writeAttribute);
    }

    // Properties
    // -------------------------------------------------------------------------

    public String getAdminAttribute() {
        return adminAttribute;
    }

    public void setAdminAttribute(String adminAttribute) {
        this.adminAttribute = adminAttribute;
    }

    public String getAdminBase() {
        return adminBase;
    }

    public void setAdminBase(String adminBase) {
        this.adminBase = adminBase;
    }

    public String getAuthentication() {
        return authentication;
    }

    public void setAuthentication(String authentication) {
        this.authentication = authentication;
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

    public DirContext getContext() {
        return context;
    }

    public void setContext(DirContext context) {
        this.context = context;
    }

    public String getInitialContextFactory() {
        return initialContextFactory;
    }

    public void setInitialContextFactory(String initialContextFactory) {
        this.initialContextFactory = initialContextFactory;
    }

    public MessageFormat getQueueSearchMatchingFormat() {
        return queueSearchMatchingFormat;
    }

    public void setQueueSearchMatchingFormat(MessageFormat queueSearchMatchingFormat) {
        this.queueSearchMatchingFormat = queueSearchMatchingFormat;
    }

    public boolean isQueueSearchSubtreeBool() {
        return queueSearchSubtreeBool;
    }

    public void setQueueSearchSubtreeBool(boolean queueSearchSubtreeBool) {
        this.queueSearchSubtreeBool = queueSearchSubtreeBool;
    }

    public String getReadAttribute() {
        return readAttribute;
    }

    public void setReadAttribute(String readAttribute) {
        this.readAttribute = readAttribute;
    }

    public String getReadBase() {
        return readBase;
    }

    public void setReadBase(String readBase) {
        this.readBase = readBase;
    }

    public MessageFormat getTopicSearchMatchingFormat() {
        return topicSearchMatchingFormat;
    }

    public void setTopicSearchMatchingFormat(MessageFormat topicSearchMatchingFormat) {
        this.topicSearchMatchingFormat = topicSearchMatchingFormat;
    }

    public boolean isTopicSearchSubtreeBool() {
        return topicSearchSubtreeBool;
    }

    public void setTopicSearchSubtreeBool(boolean topicSearchSubtreeBool) {
        this.topicSearchSubtreeBool = topicSearchSubtreeBool;
    }

    public String getWriteAttribute() {
        return writeAttribute;
    }

    public void setWriteAttribute(String writeAttribute) {
        this.writeAttribute = writeAttribute;
    }

    public String getWriteBase() {
        return writeBase;
    }

    public void setWriteBase(String writeBase) {
        this.writeBase = writeBase;
    }

    public boolean isUseAdvisorySearchBase() {
        return useAdvisorySearchBase;
    }

    public void setUseAdvisorySearchBase(boolean useAdvisorySearchBase) {
        this.useAdvisorySearchBase = useAdvisorySearchBase;
    }

    public String getAdvisorySearchBase() {
        return advisorySearchBase;
    }

    public void setAdvisorySearchBase(String advisorySearchBase) {
        this.advisorySearchBase = advisorySearchBase;
    }

    public String getTempSearchBase() {
        return tempSearchBase;
    }

    public void setTempSearchBase(String tempSearchBase) {
        this.tempSearchBase = tempSearchBase;
    }

    protected Set<GroupPrincipal> getCompositeACLs(ActiveMQDestination destination, String roleBase, String roleAttribute) {
        ActiveMQDestination[] dests = destination.getCompositeDestinations();
        Set<GroupPrincipal> acls = null;
        for (ActiveMQDestination dest : dests) {
            acls = DestinationMap.union(acls, getACLs(dest, roleBase, roleAttribute));
            if (acls == null || acls.isEmpty()) {
                break;
            }
        }
        return acls;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Set<GroupPrincipal> getACLs(ActiveMQDestination destination, String roleBase, String roleAttribute) {
        try {
            context = open();
        } catch (NamingException e) {
            LOG.error(e.toString());
            return new HashSet<GroupPrincipal>();
        }



        String destinationBase = "";
        SearchControls constraints = new SearchControls();
        if (AdvisorySupport.isAdvisoryTopic(destination) && useAdvisorySearchBase) {
            destinationBase = advisorySearchBase;
        } else {
            if ((destination.getDestinationType() & ActiveMQDestination.QUEUE_TYPE) == ActiveMQDestination.QUEUE_TYPE) {
                destinationBase = queueSearchMatchingFormat.format(new String[]{destination.getPhysicalName()});
                if (queueSearchSubtreeBool) {
                    constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
                } else {
                    constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
                }
            }
            if ((destination.getDestinationType() & ActiveMQDestination.TOPIC_TYPE) == ActiveMQDestination.TOPIC_TYPE) {
                destinationBase = topicSearchMatchingFormat.format(new String[]{destination.getPhysicalName()});
                if (topicSearchSubtreeBool) {
                    constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
                } else {
                    constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
                }
            }
        }

        constraints.setReturningAttributes(new String[] {roleAttribute});

        return getACLs(destinationBase, constraints, roleBase, roleAttribute);
    }

    protected Set<GroupPrincipal> getACLs(String destinationBase, SearchControls constraints, String roleBase, String roleAttribute) {
        try {
            Set<GroupPrincipal> roles = new HashSet<GroupPrincipal>();
            Set<String> acls = new HashSet<String>();
            NamingEnumeration<?> results = context.search(destinationBase, roleBase, constraints);
            while (results.hasMore()) {
                SearchResult result = (SearchResult)results.next();
                Attributes attrs = result.getAttributes();
                if (attrs == null) {
                    continue;
                }
                acls = addAttributeValues(roleAttribute, attrs, acls);
            }
            for (Iterator<String> iter = acls.iterator(); iter.hasNext();) {
                String roleName = iter.next();
                LdapName ldapname = new LdapName(roleName);
                Rdn rdn = ldapname.getRdn(ldapname.size() - 1);
                LOG.debug("Found role: [" + rdn.getValue().toString() + "]");
                roles.add(new GroupPrincipal(rdn.getValue().toString()));
            }
            return roles;
        } catch (NamingException e) {
            LOG.error(e.toString());
            return new HashSet<GroupPrincipal>();
        }
    }

    protected Set<String> addAttributeValues(String attrId, Attributes attrs, Set<String> values) throws NamingException {
        if (attrId == null || attrs == null) {
            return values;
        }
        if (values == null) {
            values = new HashSet<String>();
        }
        Attribute attr = attrs.get(attrId);
        if (attr == null) {
            return values;
        }
        NamingEnumeration<?> e = attr.getAll();
        while (e.hasMore()) {
            String value = (String)e.next();
            values.add(value);
        }
        return values;
    }

    protected DirContext open() throws NamingException {
        if (context != null) {
            return context;
        }

        try {
            Hashtable<String, String> env = new Hashtable<String, String>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
            if (connectionUsername != null && !"".equals(connectionUsername)) {
                env.put(Context.SECURITY_PRINCIPAL, connectionUsername);
            } else {
                throw new NamingException("Empty username is not allowed");
            }
            if (connectionPassword != null && !"".equals(connectionPassword)) {
                env.put(Context.SECURITY_CREDENTIALS, connectionPassword);
            } else {
                throw new NamingException("Empty password is not allowed");
            }
            env.put(Context.SECURITY_PROTOCOL, connectionProtocol);
            env.put(Context.PROVIDER_URL, connectionURL);
            env.put(Context.SECURITY_AUTHENTICATION, authentication);
            context = new InitialDirContext(env);

        } catch (NamingException e) {
            LOG.error(e.toString());
            throw e;
        }
        return context;
    }

}
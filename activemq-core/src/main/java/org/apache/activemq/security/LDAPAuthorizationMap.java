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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.jaas.LDAPLoginModule;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An {@link AuthorizationMap} which uses LDAP
 * 
 * @org.apache.xbean.XBean
 * 
 * @author ngcutura
 */
public class LDAPAuthorizationMap implements AuthorizationMap {

    private static Log log = LogFactory.getLog(LDAPLoginModule.class);

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

    private String initialContextFactory;
    private String connectionURL;
    private String connectionUsername;
    private String connectionPassword;
    private String connectionProtocol;
    private String authentication;

    private DirContext context;

    private MessageFormat topicSearchMatchingFormat;
    private MessageFormat queueSearchMatchingFormat;

    private boolean topicSearchSubtreeBool = true;
    private boolean queueSearchSubtreeBool = true;

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

    public LDAPAuthorizationMap(Map options) {
        initialContextFactory = (String) options.get(INITIAL_CONTEXT_FACTORY);
        connectionURL = (String) options.get(CONNECTION_URL);
        connectionUsername = (String) options.get(CONNECTION_USERNAME);
        connectionPassword = (String) options.get(CONNECTION_PASSWORD);
        connectionProtocol = (String) options.get(CONNECTION_PROTOCOL);
        authentication = (String) options.get(AUTHENTICATION);

        adminBase = (String) options.get(ADMIN_BASE);
        adminAttribute = (String) options.get(ADMIN_ATTRIBUTE);
        readBase = (String) options.get(READ_BASE);
        readAttribute = (String) options.get(READ_ATTRIBUTE);
        writeBase = (String) options.get(WRITE_BASE);
        writeAttribute = (String) options.get(WRITE_ATTRIBUTE);

        String topicSearchMatching = (String) options.get(TOPIC_SEARCH_MATCHING);
        String topicSearchSubtree = (String) options.get(TOPIC_SEARCH_SUBTREE);
        String queueSearchMatching = (String) options.get(QUEUE_SEARCH_MATCHING);
        String queueSearchSubtree = (String) options.get(QUEUE_SEARCH_SUBTREE);
        topicSearchMatchingFormat = new MessageFormat(topicSearchMatching);
        queueSearchMatchingFormat = new MessageFormat(queueSearchMatching);
        topicSearchSubtreeBool = Boolean.valueOf(topicSearchSubtree).booleanValue();
        queueSearchSubtreeBool = Boolean.valueOf(queueSearchSubtree).booleanValue();
    }

    public Set getTempDestinationAdminACLs() {
        //TODO insert implementation
    	
        return null;
    }    
    
    public Set getTempDestinationReadACLs() {
    	//    	TODO insert implementation
        return null;
    }
    
    public Set getTempDestinationWriteACLs() {
    	//    	TODO insert implementation
        return null;
    }      
    
    public Set getAdminACLs(ActiveMQDestination destination) {
        return getACLs(destination, adminBase, adminAttribute);
    }

    public Set getReadACLs(ActiveMQDestination destination) {
        return getACLs(destination, readBase, readAttribute);
    }

    public Set getWriteACLs(ActiveMQDestination destination) {
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

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Set getACLs(ActiveMQDestination destination, String roleBase, String roleAttribute) {
        try {
            context = open();
        }
        catch (NamingException e) {
            log.error(e);
            return new HashSet();
        }

        // if ((destination.getDestinationType() &
        // (ActiveMQDestination.QUEUE_TYPE | ActiveMQDestination.TOPIC_TYPE)) !=
        // 0)
        // return new HashSet();

        String destinationBase = "";
        SearchControls constraints = new SearchControls();

        if ((destination.getDestinationType() & ActiveMQDestination.QUEUE_TYPE) == ActiveMQDestination.QUEUE_TYPE) {
            destinationBase = queueSearchMatchingFormat.format(new String[] { destination.getPhysicalName() });
            if (queueSearchSubtreeBool) {
                constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
            }
            else {
                constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
            }
        }
        if ((destination.getDestinationType() & ActiveMQDestination.TOPIC_TYPE) == ActiveMQDestination.TOPIC_TYPE) {
            destinationBase = topicSearchMatchingFormat.format(new String[] { destination.getPhysicalName() });
            if (topicSearchSubtreeBool) {
                constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
            }
            else {
                constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
            }
        }

        constraints.setReturningAttributes(new String[] { roleAttribute });

        try {
            Set roles = new HashSet();
            Set acls = new HashSet();
            NamingEnumeration results = context.search(destinationBase, roleBase, constraints);
            while (results.hasMore()) {
                SearchResult result = (SearchResult) results.next();
                Attributes attrs = result.getAttributes();
                if (attrs == null) {
                    continue;
                }
                acls = addAttributeValues(roleAttribute, attrs, acls);
            }
            for (Iterator iter = acls.iterator(); iter.hasNext();) {
                String roleName = (String) iter.next();
                roles.add(new GroupPrincipal(roleName));
            }
            return roles;
        }
        catch (NamingException e) {
            log.error(e);
            return new HashSet();
        }
    }

    protected Set addAttributeValues(String attrId, Attributes attrs, Set values) throws NamingException {
        if (attrId == null || attrs == null) {
            return values;
        }
        if (values == null) {
            values = new HashSet();
        }
        Attribute attr = attrs.get(attrId);
        if (attr == null) {
            return (values);
        }
        NamingEnumeration e = attr.getAll();
        while (e.hasMore()) {
            String value = (String) e.next();
            values.add(value);
        }
        return values;
    }

    protected DirContext open() throws NamingException {
        if (context != null) {
            return context;
        }

        try {
            Hashtable env = new Hashtable();
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

        }
        catch (NamingException e) {
            log.error(e);
            throw e;
        }
        return context;
    }

}

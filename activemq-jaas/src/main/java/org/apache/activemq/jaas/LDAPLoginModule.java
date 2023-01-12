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
package org.apache.activemq.jaas;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.*;

import javax.naming.*;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Rev: $ $Date: $
 */
public class LDAPLoginModule implements LoginModule {

    private static final String INITIAL_CONTEXT_FACTORY = "initialContextFactory";
    private static final String CONNECTION_URL = "connectionURL";
    private static final String CONNECTION_USERNAME = "connectionUsername";
    private static final String CONNECTION_PASSWORD = "connectionPassword";
    private static final String CONNECTION_PROTOCOL = "connectionProtocol";
    private static final String AUTHENTICATION = "authentication";
    private static final String USER_BASE = "userBase";
    private static final String USER_SEARCH_MATCHING = "userSearchMatching";
    private static final String USER_SEARCH_SUBTREE = "userSearchSubtree";
    private static final String ROLE_BASE = "roleBase";
    private static final String ROLE_NAME = "roleName";
    private static final String ROLE_SEARCH_MATCHING = "roleSearchMatching";
    private static final String ROLE_SEARCH_SUBTREE = "roleSearchSubtree";
    private static final String USER_ROLE_NAME = "userRoleName";
    private static final String EXPAND_ROLES = "expandRoles";
    private static final String EXPAND_ROLES_MATCHING = "expandRolesMatching";

    private static Logger log = LoggerFactory.getLogger(LDAPLoginModule.class);

    protected DirContext context;

    private Subject subject;
    private CallbackHandler handler;
    private LDAPLoginProperty [] config;
    private Principal user;
    private Set<GroupPrincipal> groups = new HashSet<>();

    /** the authentication status*/
    private boolean succeeded = false;
    private boolean commitSucceeded = false;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {
        this.subject = subject;
        this.handler = callbackHandler;

        config = new LDAPLoginProperty [] {
        		new LDAPLoginProperty (INITIAL_CONTEXT_FACTORY, (String)options.get(INITIAL_CONTEXT_FACTORY)),
        		new LDAPLoginProperty (CONNECTION_URL, (String)options.get(CONNECTION_URL)),
        		new LDAPLoginProperty (CONNECTION_USERNAME, (String)options.get(CONNECTION_USERNAME)),
        		new LDAPLoginProperty (CONNECTION_PASSWORD, (String)options.get(CONNECTION_PASSWORD)),
        		new LDAPLoginProperty (CONNECTION_PROTOCOL, (String)options.get(CONNECTION_PROTOCOL)),
        		new LDAPLoginProperty (AUTHENTICATION, (String)options.get(AUTHENTICATION)),
        		new LDAPLoginProperty (USER_BASE, (String)options.get(USER_BASE)),
        		new LDAPLoginProperty (USER_SEARCH_MATCHING, (String)options.get(USER_SEARCH_MATCHING)),
        		new LDAPLoginProperty (USER_SEARCH_SUBTREE, (String)options.get(USER_SEARCH_SUBTREE)),
        		new LDAPLoginProperty (ROLE_BASE, (String)options.get(ROLE_BASE)),
        		new LDAPLoginProperty (ROLE_NAME, (String)options.get(ROLE_NAME)),
        		new LDAPLoginProperty (ROLE_SEARCH_MATCHING, (String)options.get(ROLE_SEARCH_MATCHING)),
        		new LDAPLoginProperty (ROLE_SEARCH_SUBTREE, (String)options.get(ROLE_SEARCH_SUBTREE)),
        		new LDAPLoginProperty (USER_ROLE_NAME, (String)options.get(USER_ROLE_NAME)),
                new LDAPLoginProperty (EXPAND_ROLES, (String) options.get(EXPAND_ROLES)),
                new LDAPLoginProperty (EXPAND_ROLES_MATCHING, (String) options.get(EXPAND_ROLES_MATCHING)),

        };
    }

    @Override
    public boolean login() throws LoginException {

        Callback[] callbacks = new Callback[2];

        callbacks[0] = new NameCallback("User name");
        callbacks[1] = new PasswordCallback("Password", false);
        try {
            handler.handle(callbacks);
        } catch (IOException | UnsupportedCallbackException ioe) {
            throw (LoginException)new LoginException().initCause(ioe);
        }

        String password;

        String username = ((NameCallback)callbacks[0]).getName();
        if (username == null)
        	return false;

        if (((PasswordCallback)callbacks[1]).getPassword() != null)
        	password = new String(((PasswordCallback)callbacks[1]).getPassword());
        else
        	password="";

        // authenticate will throw LoginException
        // in case of failed authentication
        authenticate(username, password);

        user = new UserPrincipal(username);
        succeeded = true;
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        subject.getPrincipals().remove(user);
        subject.getPrincipals().removeAll(groups);

        user = null;
        groups.clear();

        succeeded = false;
        commitSucceeded = false;
        return true;
    }

    @Override
    public boolean commit() throws LoginException {
        if (!succeeded) {
            user = null;
            groups.clear();
            return false;
        }

        Set<Principal> principals = subject.getPrincipals();
        principals.add(user);
        principals.addAll(groups);

        commitSucceeded = true;
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        if (!succeeded) {
            return false;
        } else if (commitSucceeded) {
            // we succeeded, but another required module failed
            logout();
        } else {
            // our commit failed
            user = null;
            groups.clear();
            succeeded = false;
        }
        return true;
    }

    protected void closeContext() {
        if (context == null) {
            return;
        }
        try {
            context.close();
        } catch (Exception e) {
            log.error(e.toString());
        } finally {
            context = null;
        }
    }

    protected boolean authenticate(String username, String password) throws LoginException {

        MessageFormat userSearchMatchingFormat;
        boolean userSearchSubtreeBool;

        if (log.isDebugEnabled()) {
            log.debug("Create the LDAP initial context.");
        }
        try {
            openContext();
        } catch (NamingException ne) {
            FailedLoginException ex = new FailedLoginException("Error opening LDAP connection");
            ex.initCause(ne);
            throw ex;
        }

        if (!isLoginPropertySet(USER_SEARCH_MATCHING))
        	return false;

        userSearchMatchingFormat = new MessageFormat(getLDAPPropertyValue(USER_SEARCH_MATCHING));
        userSearchSubtreeBool = Boolean.valueOf(getLDAPPropertyValue(USER_SEARCH_SUBTREE));

        try {

            String filter = userSearchMatchingFormat.format(new String[] {
                doRFC2254Encoding(username)
            });
            SearchControls constraints = new SearchControls();
            if (userSearchSubtreeBool) {
                constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
            } else {
                constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
            }

            // setup attributes
            List<String> list = new ArrayList<>();
            if (isLoginPropertySet(USER_ROLE_NAME)) {
                list.add(getLDAPPropertyValue(USER_ROLE_NAME));
            }
            String[] attribs = new String[list.size()];
            list.toArray(attribs);
            constraints.setReturningAttributes(attribs);

            if (log.isDebugEnabled()) {
                log.debug("Get the user DN.");
                log.debug("Looking for the user in LDAP with ");
                log.debug("  base DN: " + getLDAPPropertyValue(USER_BASE));
                log.debug("  filter: " + filter);
            }

            NamingEnumeration<SearchResult> results = context.search(getLDAPPropertyValue(USER_BASE), filter, constraints);

            if (results == null || !results.hasMore()) {
                log.warn("User " + username + " not found in LDAP.");
                throw new FailedLoginException("User " + username + " not found in LDAP.");
            }

            SearchResult result = results.next();

            if (results.hasMore()) {
                // ignore for now
            }

            String dn;
            if (result.isRelative()) {
                log.debug("LDAP returned a relative name: {}", result.getName());

                NameParser parser = context.getNameParser("");
                Name contextName = parser.parse(context.getNameInNamespace());
                Name baseName = parser.parse(getLDAPPropertyValue(USER_BASE));
                Name entryName = parser.parse(result.getName());
                Name name = contextName.addAll(baseName);
                name = name.addAll(entryName);
                dn = name.toString();
            } else {
                log.debug("LDAP returned an absolute name: {}", result.getName());

                try {
                    URI uri = new URI(result.getName());
                    String path = uri.getPath();

                    if (path.startsWith("/")) {
                        dn = path.substring(1);
                    } else {
                        dn = path;
                    }
                } catch (URISyntaxException e) {
                    closeContext();
                    FailedLoginException ex = new FailedLoginException("Error parsing absolute name as URI.");
                    ex.initCause(e);
                    throw ex;
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("Using DN [" + dn + "] for binding.");
            }

            Attributes attrs = result.getAttributes();
            if (attrs == null) {
                throw new FailedLoginException("User found, but LDAP entry malformed: " + username);
            }
            List<String> roles = null;
            if (isLoginPropertySet(USER_ROLE_NAME)) {
                roles = addAttributeValues(getLDAPPropertyValue(USER_ROLE_NAME), attrs, roles);
            }

            // check the credentials by binding to server
            if (bindUser(context, dn, password)) {
                // if authenticated add more roles
                roles = getRoles(context, dn, username, roles);
                if (log.isDebugEnabled()) {
                    log.debug("Roles " + roles + " for user " + username);
                }
                for (int i = 0; i < roles.size(); i++) {
                    groups.add(new GroupPrincipal(roles.get(i)));
                }
            } else {
                throw new FailedLoginException("Password does not match for user: " + username);
            }
        } catch (CommunicationException e) {
            FailedLoginException ex = new FailedLoginException("Error contacting LDAP");
            ex.initCause(e);
            throw ex;
        } catch (NamingException e) {
            FailedLoginException ex = new FailedLoginException("Error contacting LDAP");
            ex.initCause(e);
            throw ex;
        } finally {
            closeContext();
        }
        return true;
    }

    protected List<String> getRoles(DirContext context, String dn, String username, List<String> currentRoles) throws NamingException {
        List<String> list = currentRoles;
        MessageFormat roleSearchMatchingFormat;
        boolean roleSearchSubtreeBool;
        boolean expandRolesBool;
        roleSearchMatchingFormat = new MessageFormat(getLDAPPropertyValue(ROLE_SEARCH_MATCHING));
        roleSearchSubtreeBool = Boolean.parseBoolean(getLDAPPropertyValue(ROLE_SEARCH_SUBTREE));
        expandRolesBool = Boolean.parseBoolean(getLDAPPropertyValue(EXPAND_ROLES));

        if (list == null) {
            list = new ArrayList<>();
        }
        if (!isLoginPropertySet(ROLE_NAME)) {
            return list;
        }
        String filter = roleSearchMatchingFormat.format(new String[] {
            doRFC2254Encoding(dn), doRFC2254Encoding(username)
        });

        SearchControls constraints = new SearchControls();
        if (roleSearchSubtreeBool) {
            constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
        } else {
            constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
        }
        if (log.isDebugEnabled()) {
            log.debug("Get user roles.");
            log.debug("Looking for the user roles in LDAP with ");
            log.debug("  base DN: " + getLDAPPropertyValue(ROLE_BASE));
            log.debug("  filter: " + filter);
        }
        HashSet<String> haveSeenNames = new HashSet<>();
        Queue<String> pendingNameExpansion = new LinkedList<>();
        NamingEnumeration<SearchResult> results = context.search(getLDAPPropertyValue(ROLE_BASE), filter, constraints);
        while (results.hasMore()) {
            SearchResult result = results.next();
            Attributes attrs = result.getAttributes();
            if (expandRolesBool) {
                haveSeenNames.add(result.getNameInNamespace());
                pendingNameExpansion.add(result.getNameInNamespace());
            }
            if (attrs == null) {
                continue;
            }
            list = addAttributeValues(getLDAPPropertyValue(ROLE_NAME), attrs, list);
        }
        if (expandRolesBool) {
            MessageFormat expandRolesMatchingFormat = new MessageFormat(getLDAPPropertyValue(EXPAND_ROLES_MATCHING));
            while (!pendingNameExpansion.isEmpty()) {
                String name = pendingNameExpansion.remove();
                filter = expandRolesMatchingFormat.format(new String[]{name});
                results = context.search(getLDAPPropertyValue(ROLE_BASE), filter, constraints);
                while (results.hasMore()) {
                    SearchResult result = results.next();
                    name = result.getNameInNamespace();
                    if (!haveSeenNames.contains(name)) {
                        Attributes attrs = result.getAttributes();
                        list = addAttributeValues(getLDAPPropertyValue(ROLE_NAME), attrs, list);
                        haveSeenNames.add(name);
                        pendingNameExpansion.add(name);
                    }
                }
            }
        }
        return list;
    }

    protected String doRFC2254Encoding(String inputString) {
        StringBuffer buf = new StringBuffer(inputString.length());
        for (int i = 0; i < inputString.length(); i++) {
            char c = inputString.charAt(i);
            switch (c) {
            case '\\':
                buf.append("\\5c");
                break;
            case '*':
                buf.append("\\2a");
                break;
            case '(':
                buf.append("\\28");
                break;
            case ')':
                buf.append("\\29");
                break;
            case '\0':
                buf.append("\\00");
                break;
            default:
                buf.append(c);
                break;
            }
        }
        return buf.toString();
    }

    protected boolean bindUser(DirContext context, String dn, String password) throws NamingException {
        boolean isValid = false;
        if (log.isDebugEnabled()) {
            log.debug("Binding the user.");
        }
        context.addToEnvironment(Context.SECURITY_AUTHENTICATION, "simple");
        context.addToEnvironment(Context.SECURITY_PRINCIPAL, dn);
        context.addToEnvironment(Context.SECURITY_CREDENTIALS, password);
        try {
            context.getAttributes("", null);
            isValid = true;
            if (log.isDebugEnabled()) {
                log.debug("User " + dn + " successfully bound.");
            }
        } catch (AuthenticationException e) {
            if (log.isDebugEnabled()) {
                log.debug("Authentication failed for dn=" + dn);
            }
        }

        if (isLoginPropertySet(CONNECTION_USERNAME)) {
            context.addToEnvironment(Context.SECURITY_PRINCIPAL, getLDAPPropertyValue(CONNECTION_USERNAME));
        } else {
            context.removeFromEnvironment(Context.SECURITY_PRINCIPAL);
        }
        if (isLoginPropertySet(CONNECTION_PASSWORD)) {
            context.addToEnvironment(Context.SECURITY_CREDENTIALS, getLDAPPropertyValue(CONNECTION_PASSWORD));
        } else {
            context.removeFromEnvironment(Context.SECURITY_CREDENTIALS);
        }
        context.addToEnvironment(Context.SECURITY_AUTHENTICATION, getLDAPPropertyValue(AUTHENTICATION));
        return isValid;
    }

    private List<String> addAttributeValues(String attrId, Attributes attrs, List<String> values) throws NamingException {

        if (attrId == null || attrs == null) {
            return values;
        }
        if (values == null) {
            values = new ArrayList<>();
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

    protected void openContext() throws NamingException {
        if (context != null) {
            return;
        }
        try {
            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, getLDAPPropertyValue(INITIAL_CONTEXT_FACTORY));
            if (isLoginPropertySet(CONNECTION_USERNAME)) {
                env.put(Context.SECURITY_PRINCIPAL, getLDAPPropertyValue(CONNECTION_USERNAME));
            } else {
                throw new NamingException("Empty username is not allowed");
            }

            if (isLoginPropertySet(CONNECTION_PASSWORD)) {
                env.put(Context.SECURITY_CREDENTIALS, getLDAPPropertyValue(CONNECTION_PASSWORD));
            } else {
                throw new NamingException("Empty password is not allowed");
            }
            env.put(Context.SECURITY_PROTOCOL, getLDAPPropertyValue(CONNECTION_PROTOCOL));
            env.put(Context.PROVIDER_URL, getLDAPPropertyValue(CONNECTION_URL));
            env.put(Context.SECURITY_AUTHENTICATION, getLDAPPropertyValue(AUTHENTICATION));
            context = new InitialDirContext(env);

        } catch (NamingException e) {
            closeContext();
            log.error(e.toString());
            throw e;
        }
    }

    private String getLDAPPropertyValue (String propertyName){
        for (LDAPLoginProperty ldapLoginProperty : config)
            if (ldapLoginProperty.getPropertyName().equals(propertyName))
                return ldapLoginProperty.getPropertyValue();
    	return null;
    }

    private boolean isLoginPropertySet(String propertyName) {
        for (LDAPLoginProperty ldapLoginProperty : config) {
            if (ldapLoginProperty.getPropertyName().equals(propertyName) && (ldapLoginProperty.getPropertyValue() != null && !"".equals(ldapLoginProperty.getPropertyValue())))
                return true;
        }
    	return false;
    }

}

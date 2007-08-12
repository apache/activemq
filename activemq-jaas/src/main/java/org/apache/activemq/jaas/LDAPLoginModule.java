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
import java.security.Principal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.naming.AuthenticationException;
import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    private static Log log = LogFactory.getLog(LDAPLoginModule.class);

    protected DirContext context;

    private Subject subject;
    private CallbackHandler handler;
    private String initialContextFactory;
    private String connectionURL;
    private String connectionUsername;
    private String connectionPassword;
    private String connectionProtocol;
    private String authentication;
    private String userBase;
    private String roleBase;
    private String roleName;
    private String userRoleName;
    private String username;
    private MessageFormat userSearchMatchingFormat;
    private MessageFormat roleSearchMatchingFormat;
    private boolean userSearchSubtreeBool;
    private boolean roleSearchSubtreeBool;
    private Set<GroupPrincipal> groups = new HashSet<GroupPrincipal>();

    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {
        this.subject = subject;
        this.handler = callbackHandler;
        initialContextFactory = (String)options.get(INITIAL_CONTEXT_FACTORY);
        connectionURL = (String)options.get(CONNECTION_URL);
        connectionUsername = (String)options.get(CONNECTION_USERNAME);
        connectionPassword = (String)options.get(CONNECTION_PASSWORD);
        connectionProtocol = (String)options.get(CONNECTION_PROTOCOL);
        authentication = (String)options.get(AUTHENTICATION);
        userBase = (String)options.get(USER_BASE);
        String userSearchMatching = (String)options.get(USER_SEARCH_MATCHING);
        String userSearchSubtree = (String)options.get(USER_SEARCH_SUBTREE);
        roleBase = (String)options.get(ROLE_BASE);
        roleName = (String)options.get(ROLE_NAME);
        String roleSearchMatching = (String)options.get(ROLE_SEARCH_MATCHING);
        String roleSearchSubtree = (String)options.get(ROLE_SEARCH_SUBTREE);
        userRoleName = (String)options.get(USER_ROLE_NAME);
        userSearchMatchingFormat = new MessageFormat(userSearchMatching);
        roleSearchMatchingFormat = new MessageFormat(roleSearchMatching);
        userSearchSubtreeBool = Boolean.valueOf(userSearchSubtree).booleanValue();
        roleSearchSubtreeBool = Boolean.valueOf(roleSearchSubtree).booleanValue();
    }

    public boolean login() throws LoginException {
        Callback[] callbacks = new Callback[2];

        callbacks[0] = new NameCallback("User name");
        callbacks[1] = new PasswordCallback("Password", false);
        try {
            handler.handle(callbacks);
        } catch (IOException ioe) {
            throw (LoginException)new LoginException().initCause(ioe);
        } catch (UnsupportedCallbackException uce) {
            throw (LoginException)new LoginException().initCause(uce);
        }
        username = ((NameCallback)callbacks[0]).getName();
        String password = new String(((PasswordCallback)callbacks[1]).getPassword());

        if (username == null || "".equals(username) || password == null || "".equals(password)) {
            return false;
        }

        try {
            boolean result = authenticate(username, password);
            if (!result) {
                throw new FailedLoginException();
            } else {
                return true;
            }
        } catch (Exception e) {
            throw (LoginException)new LoginException("LDAP Error").initCause(e);
        }
    }

    public boolean logout() throws LoginException {
        username = null;
        return true;
    }

    public boolean commit() throws LoginException {
        Set<Principal> principals = subject.getPrincipals();
        principals.add(new UserPrincipal(username));
        Iterator<GroupPrincipal> iter = groups.iterator();
        while (iter.hasNext()) {
            principals.add(iter.next());
        }
        return true;
    }

    public boolean abort() throws LoginException {
        username = null;
        return true;
    }

    protected void close(DirContext context) {
        try {
            context.close();
        } catch (Exception e) {
            log.error(e);
        }
    }

    protected boolean authenticate(String username, String password) throws Exception {

        DirContext context = null;
        context = open();

        try {

            String filter = userSearchMatchingFormat.format(new String[] {
                username
            });
            SearchControls constraints = new SearchControls();
            if (userSearchSubtreeBool) {
                constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
            } else {
                constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
            }

            // setup attributes
            ArrayList<String> list = new ArrayList<String>();
            if (userRoleName != null) {
                list.add(userRoleName);
            }
            String[] attribs = new String[list.size()];
            list.toArray(attribs);
            constraints.setReturningAttributes(attribs);

            NamingEnumeration results = context.search(userBase, filter, constraints);

            if (results == null || !results.hasMore()) {
                return false;
            }

            SearchResult result = (SearchResult)results.next();

            if (results.hasMore()) {
                // ignore for now
            }
            NameParser parser = context.getNameParser("");
            Name contextName = parser.parse(context.getNameInNamespace());
            Name baseName = parser.parse(userBase);
            Name entryName = parser.parse(result.getName());
            Name name = contextName.addAll(baseName);
            name = name.addAll(entryName);
            String dn = name.toString();

            Attributes attrs = result.getAttributes();
            if (attrs == null) {
                return false;
            }
            ArrayList<String> roles = null;
            if (userRoleName != null) {
                roles = addAttributeValues(userRoleName, attrs, roles);
            }

            // check the credentials by binding to server
            if (bindUser(context, dn, password)) {
                // if authenticated add more roles
                roles = getRoles(context, dn, username, roles);
                for (int i = 0; i < roles.size(); i++) {
                    groups.add(new GroupPrincipal(roles.get(i)));
                }
            } else {
                return false;
            }
        } catch (CommunicationException e) {

        } catch (NamingException e) {
            if (context != null) {
                close(context);
            }
            return false;
        }

        return true;
    }

    protected ArrayList<String> getRoles(DirContext context, String dn, String username, ArrayList<String> currentRoles) throws NamingException {
        ArrayList<String> list = currentRoles;
        if (list == null) {
            list = new ArrayList<String>();
        }
        if (roleName == null || "".equals(roleName)) {
            return list;
        }
        String filter = roleSearchMatchingFormat.format(new String[] {
            doRFC2254Encoding(dn), username
        });

        SearchControls constraints = new SearchControls();
        if (roleSearchSubtreeBool) {
            constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
        } else {
            constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
        }
        NamingEnumeration results = context.search(roleBase, filter, constraints);
        while (results.hasMore()) {
            SearchResult result = (SearchResult)results.next();
            Attributes attrs = result.getAttributes();
            if (attrs == null) {
                continue;
            }
            list = addAttributeValues(roleName, attrs, list);
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

        context.addToEnvironment(Context.SECURITY_PRINCIPAL, dn);
        context.addToEnvironment(Context.SECURITY_CREDENTIALS, password);
        try {
            context.getAttributes("", null);
            isValid = true;
        } catch (AuthenticationException e) {
            isValid = false;
            log.debug("Authentication failed for dn=" + dn);
        }

        if (connectionUsername != null) {
            context.addToEnvironment(Context.SECURITY_PRINCIPAL, connectionUsername);
        } else {
            context.removeFromEnvironment(Context.SECURITY_PRINCIPAL);
        }

        if (connectionPassword != null) {
            context.addToEnvironment(Context.SECURITY_CREDENTIALS, connectionPassword);
        } else {
            context.removeFromEnvironment(Context.SECURITY_CREDENTIALS);
        }

        return isValid;
    }

    private ArrayList<String> addAttributeValues(String attrId, Attributes attrs, ArrayList<String> values) throws NamingException {

        if (attrId == null || attrs == null) {
            return values;
        }
        if (values == null) {
            values = new ArrayList<String>();
        }
        Attribute attr = attrs.get(attrId);
        if (attr == null) {
            return values;
        }
        NamingEnumeration e = attr.getAll();
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

        } catch (NamingException e) {
            log.error(e);
            throw e;
        }
        return context;
    }

}

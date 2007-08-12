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

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
public class PropertiesLoginModule implements LoginModule {

    private static final String USER_FILE = "org.apache.activemq.jaas.properties.user";
    private static final String GROUP_FILE = "org.apache.activemq.jaas.properties.group";

    private static final Log LOG = LogFactory.getLog(PropertiesLoginModule.class);

    private Subject subject;
    private CallbackHandler callbackHandler;

    private boolean debug;
    private String usersFile;
    private String groupsFile;
    private Properties users = new Properties();
    private Properties groups = new Properties();
    private String user;
    private Set<Principal> principals = new HashSet<Principal>();
    private File baseDir;

    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;

        if (System.getProperty("java.security.auth.login.config") != null) {
            baseDir = new File(System.getProperty("java.security.auth.login.config")).getParentFile();
        } else {
            baseDir = new File(".");
        }

        debug = "true".equalsIgnoreCase((String)options.get("debug"));
        usersFile = (String)options.get(USER_FILE) + "";
        groupsFile = (String)options.get(GROUP_FILE) + "";

        if (debug) {
            LOG.debug("Initialized debug=" + debug + " usersFile=" + usersFile + " groupsFile=" + groupsFile + " basedir=" + baseDir);
        }
    }

    public boolean login() throws LoginException {
        File f = new File(baseDir, usersFile);
        try {
            java.io.FileInputStream in = new java.io.FileInputStream(f);
            users.load(in);
            in.close();
        } catch (IOException ioe) {
            throw new LoginException("Unable to load user properties file " + f);
        }
        f = new File(baseDir, groupsFile);
        try {
            java.io.FileInputStream in = new java.io.FileInputStream(f);
            groups.load(in);
            in.close();
        } catch (IOException ioe) {
            throw new LoginException("Unable to load group properties file " + f);
        }

        Callback[] callbacks = new Callback[2];

        callbacks[0] = new NameCallback("Username: ");
        callbacks[1] = new PasswordCallback("Password: ", false);
        try {
            callbackHandler.handle(callbacks);
        } catch (IOException ioe) {
            throw new LoginException(ioe.getMessage());
        } catch (UnsupportedCallbackException uce) {
            throw new LoginException(uce.getMessage() + " not available to obtain information from user");
        }
        user = ((NameCallback)callbacks[0]).getName();
        char[] tmpPassword = ((PasswordCallback)callbacks[1]).getPassword();
        if (tmpPassword == null) {
            tmpPassword = new char[0];
        }
        String password = users.getProperty(user);

        if (password == null) {
            throw new FailedLoginException("User does exist");
        }
        if (!password.equals(new String(tmpPassword))) {
            throw new FailedLoginException("Password does not match");
        }
        users.clear();

        if (debug) {
            LOG.debug("login " + user);
        }
        return true;
    }

    public boolean commit() throws LoginException {
        principals.add(new UserPrincipal(user));

        for (Enumeration enumeration = groups.keys(); enumeration.hasMoreElements();) {
            String name = (String)enumeration.nextElement();
            String[] userList = ((String)groups.getProperty(name) + "").split(",");
            for (int i = 0; i < userList.length; i++) {
                if (user.equals(userList[i])) {
                    principals.add(new GroupPrincipal(name));
                    break;
                }
            }
        }

        subject.getPrincipals().addAll(principals);

        clear();

        if (debug) {
            LOG.debug("commit");
        }
        return true;
    }

    public boolean abort() throws LoginException {
        clear();

        if (debug) {
            LOG.debug("abort");
        }
        return true;
    }

    public boolean logout() throws LoginException {
        subject.getPrincipals().removeAll(principals);
        principals.clear();

        if (debug) {
            LOG.debug("logout");
        }
        return true;
    }

    private void clear() {
        groups.clear();
        user = null;
    }
}

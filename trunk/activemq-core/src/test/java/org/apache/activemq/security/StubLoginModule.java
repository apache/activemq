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

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.jaas.UserPrincipal;

public class StubLoginModule implements LoginModule {
    public static final String ALLOW_LOGIN_PROPERTY = "org.apache.activemq.jaas.stubproperties.allow_login";
    public static final String USERS_PROPERTY = "org.apache.activemq.jaas.stubproperties.users";
    public static final String GROUPS_PROPERTY = "org.apache.activemq.jaas.stubproperties.groups";

    private Subject subject;

    private String userNames[];
    private String groupNames[];
    private boolean allowLogin;

    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {
        String allowLoginString = (String)(options.get(ALLOW_LOGIN_PROPERTY));
        String usersString = (String)(options.get(USERS_PROPERTY));
        String groupsString = (String)(options.get(GROUPS_PROPERTY));

        this.subject = subject;

        allowLogin = Boolean.parseBoolean(allowLoginString);
        userNames = usersString.split(",");
        groupNames = groupsString.split(",");
    }

    public boolean login() throws LoginException {
        if (!allowLogin) {
            throw new FailedLoginException("Login was not allowed (as specified in configuration).");
        }

        return true;
    }

    public boolean commit() throws LoginException {
        if (!allowLogin) {
            throw new FailedLoginException("Login was not allowed (as specified in configuration).");
        }

        for (int i = 0; i < userNames.length; ++i) {
            if (userNames[i].length() > 0) {
                subject.getPrincipals().add(new UserPrincipal(userNames[i]));
            }
        }

        for (int i = 0; i < groupNames.length; ++i) {
            if (groupNames[i].length() > 0) {
                subject.getPrincipals().add(new GroupPrincipal(groupNames[i]));
            }
        }

        return true;
    }

    public boolean abort() throws LoginException {
        return true;
    }

    public boolean logout() throws LoginException {
        subject.getPrincipals().clear();

        return true;
    }

}

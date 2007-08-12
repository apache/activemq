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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

public class CertificateLoginModuleTest extends TestCase {

    private static final String USER_NAME = "testUser";
    private static final List<String> GROUP_NAMES = new Vector<String>();

    private StubCertificateLoginModule loginModule;

    private Subject subject;

    public CertificateLoginModuleTest() {
        GROUP_NAMES.add("testGroup1");
        GROUP_NAMES.add("testGroup2");
        GROUP_NAMES.add("testGroup3");
        GROUP_NAMES.add("testGroup4");
    }

    protected void setUp() throws Exception {
        subject = new Subject();
    }

    protected void tearDown() throws Exception {
    }

    private void loginWithCredentials(String userName, Set<String> groupNames) throws LoginException {
        loginModule = new StubCertificateLoginModule(userName, new HashSet<String>(groupNames));
        JaasCertificateCallbackHandler callbackHandler = new JaasCertificateCallbackHandler(null);

        loginModule.initialize(subject, callbackHandler, null, new HashMap());

        loginModule.login();
        loginModule.commit();
    }

    private void checkPrincipalsMatch(Subject subject) {
        boolean nameFound = false;
        boolean groupsFound[] = new boolean[GROUP_NAMES.size()];
        for (int i = 0; i < groupsFound.length; ++i) {
            groupsFound[i] = false;
        }

        for (Iterator iter = subject.getPrincipals().iterator(); iter.hasNext();) {
            Principal currentPrincipal = (Principal)iter.next();

            if (currentPrincipal instanceof UserPrincipal) {
                if (((UserPrincipal)currentPrincipal).getName().equals(USER_NAME)) {
                    if (!nameFound) {
                        nameFound = true;
                    } else {
                        fail("UserPrincipal found twice.");
                    }

                } else {
                    fail("Unknown UserPrincipal found.");
                }

            } else if (currentPrincipal instanceof GroupPrincipal) {
                int principalIdx = GROUP_NAMES.indexOf(((GroupPrincipal)currentPrincipal).getName());

                if (principalIdx < 0) {
                    fail("Unknown GroupPrincipal found.");
                }

                if (!groupsFound[principalIdx]) {
                    groupsFound[principalIdx] = true;
                } else {
                    fail("GroupPrincipal found twice.");
                }
            } else {
                fail("Unknown Principal type found.");
            }
        }
    }

    public void testLoginSuccess() throws IOException {
        try {
            loginWithCredentials(USER_NAME, new HashSet<String>(GROUP_NAMES));
        } catch (Exception e) {
            fail("Unable to login: " + e.getMessage());
        }

        checkPrincipalsMatch(subject);
    }

    public void testLoginFailure() throws IOException {
        boolean loginFailed = false;

        try {
            loginWithCredentials(null, new HashSet<String>());
        } catch (LoginException e) {
            loginFailed = true;
        }

        if (!loginFailed) {
            fail("Logged in with unknown certificate.");
        }
    }

    public void testLogOut() throws IOException {
        try {
            loginWithCredentials(USER_NAME, new HashSet<String>(GROUP_NAMES));
        } catch (Exception e) {
            fail("Unable to login: " + e.getMessage());
        }

        loginModule.logout();

        assertEquals("logout should have cleared Subject principals.", 0, subject.getPrincipals().size());
    }
}

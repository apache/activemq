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

import java.net.URL;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import org.apache.activemq.jaas.CertificateLoginModule;
import org.apache.activemq.jaas.JaasCertificateCallbackHandler;
import org.apache.activemq.jaas.PropertiesLoader;
import org.apache.activemq.jaas.TextFileCertificateLoginModule;
import org.apache.activemq.transport.tcp.StubX509Certificate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFileCertificateLoginModuleTest {

    private static final String CERT_USERS_FILE_SMALL = "cert-users-SMALL.properties";
    private static final String CERT_USERS_FILE_LARGE = "cert-users-LARGE.properties";
    private static final String CERT_USERS_FILE_REGEXP = "cert-users-REGEXP.properties";
    private static final String CERT_GROUPS_FILE = "cert-groups.properties";

    private static final Logger LOG = LoggerFactory.getLogger(TextFileCertificateLoginModuleTest.class);
    private static final int NUMBER_SUBJECTS = 10;

    static {
        String path = System.getProperty("java.security.auth.login.config");
        if (path == null) {
            URL resource = TextFileCertificateLoginModuleTest.class.getClassLoader().getResource("login.config");
            if (resource != null) {
                path = resource.getFile();
                System.setProperty("java.security.auth.login.config", path);
            }
        }
    }

    private CertificateLoginModule loginModule;

    @Before
    public void setUp() throws Exception {
        loginModule = new TextFileCertificateLoginModule();
    }

    @After
    public void tearDown() throws Exception {
        PropertiesLoader.resetUsersAndGroupsCache();
    }

    @Test
    public void testLoginWithSMALLUsersFile() throws Exception {
        loginTest(CERT_USERS_FILE_SMALL, CERT_GROUPS_FILE);
    }

    @Test
    public void testLoginWithLARGEUsersFile() throws Exception {
        loginTest(CERT_USERS_FILE_LARGE, CERT_GROUPS_FILE);
    }

    @Test
    public void testLoginWithREGEXPUsersFile() throws Exception {
        loginTest(CERT_USERS_FILE_REGEXP, CERT_GROUPS_FILE);
    }

    private void loginTest(String usersFiles, String groupsFile) throws LoginException {

        HashMap options = new HashMap<String, String>();
        options.put("org.apache.activemq.jaas.textfiledn.user", usersFiles);
        options.put("org.apache.activemq.jaas.textfiledn.group", groupsFile);
        options.put("reload", "true");

        JaasCertificateCallbackHandler[] callbackHandlers = new JaasCertificateCallbackHandler[NUMBER_SUBJECTS];
        Subject[] subjects = new Subject[NUMBER_SUBJECTS];

        for (int i = 0; i < callbackHandlers.length; i++) {
            callbackHandlers[i] = getJaasCertificateCallbackHandler("DN=TEST_USER_" + (i + 1));
        }

        long startTime = System.currentTimeMillis();

        for (int outer=0; outer<500;outer++) {
            for (int i = 0; i < NUMBER_SUBJECTS; i++) {
                Subject subject = doAuthenticate(options, callbackHandlers[i]);
                subjects[i] = subject;
            }
        }

        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;


        for (int i = 0; i < NUMBER_SUBJECTS; i++) {
            LOG.info("subject is: " + subjects[i].getPrincipals().toString());
        }

        LOG.info(usersFiles + ": Time taken is " + timeTaken);

    }

    private JaasCertificateCallbackHandler getJaasCertificateCallbackHandler(String user) {
        JMXPrincipal principal = new JMXPrincipal(user);
        X509Certificate cert = new StubX509Certificate(principal);
        return new JaasCertificateCallbackHandler(new X509Certificate[]{cert});
    }

    private Subject doAuthenticate(HashMap options, JaasCertificateCallbackHandler callbackHandler) throws LoginException {
        Subject mySubject = new Subject();
        loginModule.initialize(mySubject, callbackHandler, null, options);
        loginModule.login();
        loginModule.commit();
        return mySubject;

    }

}

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
package org.apache.activemq.karaf.itest;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.security.PrivilegedAction;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import org.apache.karaf.jaas.boot.principal.RolePrincipal;
import org.apache.karaf.jaas.boot.principal.UserPrincipal;
import org.apache.karaf.shell.api.console.Session;
import org.apache.karaf.shell.api.console.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KarafShellHelper {
    public static final Logger LOG = LoggerFactory.getLogger(KarafShellHelper.class);

    public static final String USER = "karaf";
    public static final String PASSWORD = "karaf";
    public static final long COMMAND_TIMEOUT = 30000L;

    public static String executeCommand(SessionFactory sessionFactory, final String command, final Long timeout, final Boolean silent) {
        String response;
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final PrintStream printStream = new PrintStream(byteArrayOutputStream);
        final Session commandSession = sessionFactory.create(System.in, printStream, printStream);
        commandSession.put("APPLICATION", System.getProperty("karaf.name", "root"));
        commandSession.put("USER", USER);
        FutureTask<String> commandFuture = new FutureTask<String>(
                new Callable<String>() {
                    @Override
                    public String call() {

                        Subject subject = new Subject();
                        subject.getPrincipals().add(new UserPrincipal("admin"));
                        subject.getPrincipals().add(new RolePrincipal("admin"));
                        subject.getPrincipals().add(new RolePrincipal("manager"));
                        subject.getPrincipals().add(new RolePrincipal("viewer"));
                        return Subject.doAs(subject, new PrivilegedAction<String>() {
                            @Override
                            public String run() {
                                try {
                                    if (!silent) {
                                        System.out.println(command);
                                        System.out.flush();
                                    }
                                    commandSession.execute(command);
                                } catch (Exception e) {
                                    e.printStackTrace(System.err);
                                }
                                printStream.flush();
                                return byteArrayOutputStream.toString();
                            }
                        });
                    }});

        try {
            Executors.newSingleThreadExecutor().submit(commandFuture);
            response = commandFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            response = "SHELL COMMAND TIMED OUT: ";
        }
        LOG.info("Execute: " + command + " - Response:" + response);
        return response;
    }

    public static String executeCommand(SessionFactory sessionFactory, final String command) {
        return executeCommand(sessionFactory, command, COMMAND_TIMEOUT, false);
    }
}

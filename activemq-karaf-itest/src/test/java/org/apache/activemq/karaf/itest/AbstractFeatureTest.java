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

import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.replaceConfigurationFile;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.security.PrivilegedAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.security.auth.Subject;

import org.apache.karaf.features.FeaturesService;
import org.apache.karaf.jaas.boot.principal.RolePrincipal;
import org.apache.karaf.jaas.boot.principal.UserPrincipal;
import org.apache.karaf.shell.api.console.Session;
import org.apache.karaf.shell.api.console.SessionFactory;
import org.junit.After;
import org.junit.Before;
import org.ops4j.pax.exam.MavenUtils;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.options.MavenUrlReference;
import org.ops4j.pax.exam.options.UrlReference;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFeatureTest {

    private static final String KARAF_MAJOR_VERSION = "4.0.0";
    public static final Logger LOG = LoggerFactory.getLogger(AbstractFeatureTest.class);
    public static final long ASSERTION_TIMEOUT = 30000L;
    public static final long COMMAND_TIMEOUT = 30000L;
    public static final String USER = "karaf";
    public static final String PASSWORD = "karaf";
    public static final String RESOURCE_BASE = "src/test/resources/org/apache/activemq/karaf/itest/";

    @Inject
    BundleContext bundleContext;

    @Inject
    FeaturesService featuresService;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }


    @ProbeBuilder
    public TestProbeBuilder probeConfiguration(TestProbeBuilder probe) {
        probe.setHeader(Constants.DYNAMICIMPORT_PACKAGE, "*,org.ops4j.pax.exam.options.*,org.apache.felix.service.*;status=provisional");
        return probe;
    }

    @Inject
    SessionFactory sessionFactory;

    ExecutorService executor = Executors.newCachedThreadPool();

    private String executeCommand(final String command, final Long timeout, final Boolean silent) {
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
            executor.submit(commandFuture);
            response = commandFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            response = "SHELL COMMAND TIMED OUT: ";
        }
        LOG.info("Execute: " + command + " - Response:" + response);
        return response;
    }

    protected String executeCommand(final String command) {
        return executeCommand(command, COMMAND_TIMEOUT, false);
    }

    /**
     * Installs a feature and asserts that feature is properly installed.
     * 
     * @param feature
     * @throws Exception
     */
    public void installAndAssertFeature(final String feature) throws Throwable {
        featuresService.installFeature(feature);
    }

    public void assertFeatureInstalled(final String feature) throws Throwable {
        withinReason(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                assertTrue("Expected " + feature + " feature to be installed.", featuresService.isInstalled(featuresService.getFeature(feature)));
                return true;
            }
        });
    }
    
    public Bundle getBundle(String symName) {
        for (Bundle bundle: bundleContext.getBundles()) {
            if (bundle.getSymbolicName().contains(symName)) {
                return bundle;
            }
        }
        throw new RuntimeException("Bundle " + symName + " not found");
    }

    public static Option configureBrokerStart(String xmlConfig) {
        return composite(
                replaceConfigurationFile("etc/activemq.xml", new File(RESOURCE_BASE + xmlConfig + ".xml")),
                replaceConfigurationFile("etc/org.apache.activemq.server-default.cfg", 
                                         new File(RESOURCE_BASE + "org.apache.activemq.server-default.cfg"))
                );
    }

    public static Option configureBrokerStart() {
        return configureBrokerStart("activemq");
    }

    public static Option configure(String... features) {
        MavenUrlReference karafUrl = maven().groupId("org.apache.karaf").artifactId("apache-karaf")
            .type("tar.gz").versionAsInProject();
        UrlReference camelUrl = maven().groupId("org.apache.camel.karaf")
            .artifactId("apache-camel").type("xml").classifier("features").versionAsInProject();
        UrlReference activeMQUrl = maven().groupId("org.apache.activemq").
            artifactId("activemq-karaf").versionAsInProject().type("xml").classifier("features").versionAsInProject();
        return composite(
         karafDistributionConfiguration().frameworkUrl(karafUrl).karafVersion(KARAF_MAJOR_VERSION)
             .name("Apache Karaf").unpackDirectory(new File("target/paxexam/unpack/")),
         keepRuntimeFolder(), //
         logLevel(LogLevelOption.LogLevel.WARN), //
         editConfigurationFilePut("etc/config.properties", "karaf.startlevel.bundle", "50"),
         // debugConfiguration("5005", true),
         features(activeMQUrl, features), //
         features(camelUrl)
        );
    }

    protected static String camelVersion() {
        return MavenUtils.getArtifactVersion("org.apache.camel.karaf", "apache-camel");
    }

    protected boolean withinReason(Callable<Boolean> callable) throws Throwable {
        long max = System.currentTimeMillis() + ASSERTION_TIMEOUT;
        while (true) {
            try {
                return callable.call();
            } catch (Throwable t) {
                if (System.currentTimeMillis() < max) {
                    TimeUnit.SECONDS.sleep(1);
                    continue;
                } else {
                    throw t;
                }
            }
        }
    }
    
    protected void withinReason(Runnable runable) throws Exception {
        long max = System.currentTimeMillis() + ASSERTION_TIMEOUT;
        while (true) {
            try {
                runable.run();
                return;
            } catch (Throwable t) {
                if (System.currentTimeMillis() < max) {
                    TimeUnit.SECONDS.sleep(1);
                    continue;
                } else {
                    throw t;
                }
            }
        }
    }
}

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

import javax.security.auth.Subject;

import org.apache.felix.service.command.CommandProcessor;
import org.apache.felix.service.command.CommandSession;
import org.apache.karaf.features.FeaturesService;
import org.apache.karaf.jaas.boot.principal.RolePrincipal;
import org.apache.karaf.jaas.boot.principal.UserPrincipal;
import org.apache.karaf.shell.api.console.Session;
import org.apache.karaf.shell.api.console.SessionFactory;
import org.junit.After;
import org.junit.Before;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.karaf.options.KarafDistributionOption;
import org.ops4j.pax.exam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.options.UrlReference;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.ops4j.pax.exam.CoreOptions.*;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.*;

public abstract class AbstractFeatureTest {

    public static final Logger LOG = LoggerFactory.getLogger(AbstractFeatureTest.class);
    public static final long ASSERTION_TIMEOUT = 30000L;
    public static final long COMMAND_TIMEOUT = 30000L;
    public static final String USER = "karaf";
    public static final String PASSWORD = "karaf";

    static String basedir;
    static {
        try {
            File location = new File(AbstractFeatureTest.class.getProtectionDomain().getCodeSource().getLocation().getFile());
            basedir = new File(location, "../..").getCanonicalPath();
            System.err.println("basedir=" + basedir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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

    protected String executeCommand(final String command, final Long timeout, final Boolean silent) {
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
	 * @param feature
	 * @throws Exception
	 */
	public void installAndAssertFeature(final String feature) throws Throwable {
        executeCommand("feature:list -i");
		executeCommand("feature:install " + feature);
		assertFeatureInstalled(feature);
	}

    public void assertFeatureInstalled(final String feature) throws Throwable {
        executeCommand("feature:list -i");
        withinReason(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                assertTrue("Expected " + feature + " feature to be installed.", featuresService.isInstalled(featuresService.getFeature(feature)));
                return true;
            }
        });
    }

    public boolean verifyBundleInstalled(final String bundleName) throws Exception {
        boolean found = false;
        for (Bundle bundle: bundleContext.getBundles()) {
            LOG.debug("Checking: " + bundle.getSymbolicName());
            if (bundle.getSymbolicName().contains(bundleName)) {
                found = true;
                break;
            }
        }
        return found;
    }

	public static String karafVersion() {
        return System.getProperty("karafVersion", "unknown-need-env-var");
    }

    public static UrlReference getActiveMQKarafFeatureUrl() {
        String type = "xml/features";
        UrlReference urlReference = mavenBundle().groupId("org.apache.activemq").
            artifactId("activemq-karaf").versionAsInProject().type(type);
        System.err.println("FeatureURL: " + urlReference.getURL());
        return urlReference;
    }

    // for use from a probe
    public String getCamelFeatureUrl() {
        return getCamelFeatureUrl(System.getProperty("camel.version", "unknown"));
    }

    public static String getCamelFeatureUrl(String ver) {
        return "mvn:org.apache.camel.karaf/apache-camel/"
        + ver
        + "/xml/features";
    }

    public static UrlReference getKarafFeatureUrl() {
        LOG.info("*** The karaf version is " + karafVersion() + " ***");

        String type = "xml/features";
        return mavenBundle().groupId("org.apache.karaf.assemblies.features").
            artifactId("standard").version(karafVersion()).type(type);
    }

    public static Option[] configureBrokerStart(Option[] existingOptions, String xmlConfig) {
        existingOptions = append(
                replaceConfigurationFile("etc/activemq.xml", new File(basedir + "/src/test/resources/org/apache/activemq/karaf/itest/" + xmlConfig + ".xml")),
                existingOptions);
        return append(
                replaceConfigurationFile("etc/org.apache.activemq.server-default.cfg", new File(basedir + "/src/test/resources/org/apache/activemq/karaf/itest/org.apache.activemq.server-default.cfg")),
                existingOptions);
    }

    public static Option[] configureBrokerStart(Option[] existingOptions) {
        final String xmlConfig = "activemq";
        return configureBrokerStart(existingOptions, xmlConfig);
    }

    public static Option[] append(Option toAdd, Option[] existingOptions) {
        ArrayList<Option> newOptions = new ArrayList<Option>();
        newOptions.addAll(Arrays.asList(existingOptions));
        newOptions.add(toAdd);
        return newOptions.toArray(new Option[]{});
    }

    public static Option[] configure(String ...features) {

        ArrayList<String> f = new ArrayList<String>();
        f.addAll(Arrays.asList(features));

        Option[] options =
            new Option[]{
                karafDistributionConfiguration().frameworkUrl(
                    maven().groupId("org.apache.karaf").artifactId("apache-karaf").type("tar.gz").versionAsInProject())
                    .karafVersion(karafVersion()).name("Apache Karaf")
                    .unpackDirectory(new File("target/paxexam/unpack/")),

                KarafDistributionOption.keepRuntimeFolder(),
                logLevel(LogLevelOption.LogLevel.WARN),
                editConfigurationFilePut("etc/config.properties", "karaf.startlevel.bundle", "50"),
                //debugConfiguration("5005", true),
                features(getActiveMQKarafFeatureUrl(), f.toArray(new String[f.size()]))};
        if (f.contains("activemq-camel")) {
            options = append(features(maven().groupId("org.apache.camel.karaf").artifactId("apache-camel")
                    .versionAsInProject()
                    .type("xml/features")), options);
        }

        return options;
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
}

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

import org.apache.felix.service.command.CommandProcessor;
import org.apache.felix.service.command.CommandSession;
import org.junit.After;
import org.junit.Before;
import org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.junit.ProbeBuilder;
import org.ops4j.pax.exam.options.UrlReference;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;


import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.replaceConfigurationFile;
import static org.ops4j.pax.exam.CoreOptions.*;

public abstract class AbstractFeatureTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFeatureTest.class);
    private static final long ASSERTION_TIMEOUT = 20000L;
    private static final long COMMAND_TIMEOUT = 10000L;
    public static final String USER = "karaf";
    public static final String PASSWORD = "karaf";

    static String basedir;
    static {
        try {
            File location = new File(AbstractFeatureTest.class.getProtectionDomain().getCodeSource().getLocation().getFile());
            basedir = new File(location, "../..").getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Inject
    protected BundleContext bundleContext;

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
    CommandProcessor commandProcessor;
    ExecutorService executor = Executors.newCachedThreadPool();

    protected String executeCommand(final String command, final Long timeout, final Boolean silent) {
            String response;
            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            final PrintStream printStream = new PrintStream(byteArrayOutputStream);
            final CommandSession commandSession = commandProcessor.createSession(System.in, printStream, printStream);
            commandSession.put("APPLICATION", System.getProperty("karaf.name", "root"));
            commandSession.put("USER", USER);
            FutureTask<String> commandFuture = new FutureTask<String>(
                    new Callable<String>() {
                        public String call() {
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

            try {
                executor.submit(commandFuture);
                response = commandFuture.get(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                e.printStackTrace(System.err);
                response = "SHELL COMMAND TIMED OUT: ";
            }

            return response;
        }

    protected String executeCommand(final String command) {
        return executeCommand(command, COMMAND_TIMEOUT, false);
    }


    public static String karafVersion() {
        return System.getProperty("karafVersion", "2.3.0");
    }

    public static String activemqVersion() {
        Package p = Package.getPackage("org.apache.activemq");
        String version=null;
        if (p != null) {
            version = p.getImplementationVersion();
        }
        return System.getProperty("activemqVersion", version);
    }


    public static UrlReference getActiveMQKarafFeatureUrl() {
        String type = "xml/features";
        return mavenBundle().groupId("org.apache.activemq").
            artifactId("activemq-karaf").versionAsInProject().type(type);
    }
    
    public static UrlReference getKarafFeatureUrl() {
        LOG.info("*** The karaf version is " + karafVersion() + " ***");

        String type = "xml/features";
        return mavenBundle().groupId("org.apache.karaf.assemblies.features").
            artifactId("standard").version(karafVersion()).type(type);
    }

    public static Option[] configureBrokerStart(Option[] existingOptions) {
        existingOptions = append(
                replaceConfigurationFile("etc/org.apache.activemq.server-default.cfg", new File(basedir + "/src/test/resources/org/apache/activemq/karaf/itest/org.apache.activemq.server-default.cfg")),
                existingOptions);
        return append(
                replaceConfigurationFile("etc/activemq.xml", new File(basedir + "/src/test/resources/org/apache/activemq/karaf/itest/activemq.xml")),
                existingOptions);
    }

    public static Option[] append(Option toAdd, Option[] existingOptions) {
        ArrayList<Option> newOptions = new ArrayList<Option>();
        newOptions.addAll(Arrays.asList(existingOptions));
        newOptions.add(toAdd);
        return newOptions.toArray(new Option[]{});
    }

    public static Option[] configure(String ...features) {

        ArrayList<String> f = new ArrayList<String>();
        // install the cxf jaxb spec as the karaf doesn't provide it by default
        // f.add("cxf-jaxb");
        f.addAll(Arrays.asList(features));

        Option[] options =
            new Option[]{
                karafDistributionConfiguration().frameworkUrl(
                    maven().groupId("org.apache.karaf").artifactId("apache-karaf").type("tar.gz").version(karafVersion()))
                    //This version doesn't affect the version of karaf we use 
                    .karafVersion(karafVersion()).name("Apache Karaf")
                    .unpackDirectory(new File("target/paxexam/unpack/")),
                
                KarafDistributionOption.keepRuntimeFolder(),
                // override the config.properties (to fix pax-exam bug)
                replaceConfigurationFile("etc/config.properties", new File(basedir+"/src/test/resources/org/apache/activemq/karaf/itest/config.properties")),
                replaceConfigurationFile("etc/custom.properties", new File(basedir+"/src/test/resources/org/apache/activemq/karaf/itest/custom.properties")),
                //replaceConfigurationFile("etc/org.ops4j.pax.logging.cfg", new File(basedir+"/src/test/resources/org/apache/activemq/karaf/itest/org.ops4j.pax.logging.cfg")),
                scanFeatures(getActiveMQKarafFeatureUrl(), f.toArray(new String[f.size()]))};

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

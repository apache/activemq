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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.replaceConfigurationFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.karaf.features.FeaturesService;
import org.apache.karaf.shell.api.console.SessionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.MavenUtils;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.options.MavenUrlReference;
import org.ops4j.pax.exam.options.UrlReference;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public abstract class AbstractFeatureTest {

    private static final String KARAF_MAJOR_VERSION = "4.0.0";
    public static final Logger LOG = LoggerFactory.getLogger(AbstractFeatureTest.class);
    public static final long ASSERTION_TIMEOUT = 30000L;
    public static final String RESOURCE_BASE = "src/test/resources/org/apache/activemq/karaf/itest/";

    @Inject
    BundleContext bundleContext;

    @Inject
    FeaturesService featuresService;
    
    @Inject
    SessionFactory sessionFactory;

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

    protected String executeCommand(String command) {
		return KarafShellHelper.executeCommand(sessionFactory, command);
	}

	protected void assertBrokerStarted() throws Exception {
		withinReason(new Runnable() {
	        public void run() {
	            assertEquals("brokerName = amq-broker", executeCommand("activemq:list").trim());
	            assertTrue(executeCommand("activemq:bstat").trim().contains("BrokerName = amq-broker"));
	        }
	    });
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

    public static boolean withinReason(Callable<Boolean> callable) throws Exception {
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
    
    public static void withinReason(Runnable runable) {
        long max = System.currentTimeMillis() + ASSERTION_TIMEOUT;
        while (true) {
            try {
                runable.run();
                return;
            } catch (Throwable t) {
                if (System.currentTimeMillis() < max) {
                    try {
						TimeUnit.SECONDS.sleep(1);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
                    continue;
                } else {
                    throw t;
                }
            }
        }
    }
    
    @SuppressWarnings("resource")
    public static void copyFile(File from, File to) throws IOException {
        if (!to.exists()) {
            System.err.println("Creating new file for: "+ to);
            to.createNewFile();
        }
        FileChannel in = new FileInputStream(from).getChannel();
        FileChannel out = new FileOutputStream(to).getChannel();
        try {
            long size = in.size();
            long position = 0;
            while (position < size) {
                position += in.transferTo(position, 8192, out);
            }
        } finally {
            try {
                in.close();
                out.force(true);
                out.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

}

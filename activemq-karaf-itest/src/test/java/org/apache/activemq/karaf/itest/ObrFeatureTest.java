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

import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.MavenUtils;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.osgi.framework.Bundle;
import org.osgi.framework.ServiceReference;


@RunWith(PaxExam.class)
public class ObrFeatureTest extends AbstractFeatureTest {

    @Configuration
    public static Option[] configure() {
        Option[] options = append(
                editConfigurationFilePut("etc/system.properties", "camel.version", MavenUtils.getArtifactVersion("org.apache.camel.karaf", "apache-camel")),
                configure("obr"));
        // can't see where these deps die in a paxexam container - vanilla distro unpack can install war feature ok
        options = append(CoreOptions.mavenBundle("org.apache.xbean", "xbean-bundleutils").versionAsInProject(), options);
        options = append(CoreOptions.mavenBundle("org.apache.xbean", "xbean-asm-util").versionAsInProject(), options);
        return append(CoreOptions.mavenBundle("org.apache.xbean", "xbean-finder").versionAsInProject(), options);
    }


    @Test(timeout=5 * 60 * 1000)
    public void testWar() throws Throwable {
        installAndAssertFeature("war");
    }

    @Test(timeout=5 * 60 * 1000)
    public void testClient() throws Throwable {
        installAndAssertFeature("activemq-client");
    }

    @Test(timeout=5 * 60 * 1000)
    public void testActiveMQ() throws Throwable {
        installAndAssertFeature("activemq");
    }

    @Test(timeout=5 * 60 * 1000)
    public void testBroker() throws Throwable {
        installAndAssertFeature("activemq-broker");
    }

    @Test(timeout=5 * 60 * 1000)
    public void testCamel() throws Throwable {
        executeCommand("feature:repo-add " + getCamelFeatureUrl());
        installAndAssertFeature("activemq-camel");
    }

    @Test(timeout=5 * 60 * 1000)
    public void testClientWithSpring31() throws Throwable {
        executeCommand("feature:install spring/3.1.4.RELEASE");
        installAndAssertFeature("activemq-client");
        verifyBundleInstalledAndRegisteredServices("activemq-osgi", 2);
    }

    @Test(timeout=5 * 60 * 1000)
    public void testClientWithSpring32() throws Throwable {
        executeCommand("feature:install spring/3.2.14.RELEASE_1");
        installAndAssertFeature("activemq-client");
        verifyBundleInstalledAndRegisteredServices("activemq-osgi", 2);
    }

    @Test(timeout=5 * 60 * 1000)
    public void testClientWithSpring40() throws Throwable {
        executeCommand("feature:install spring/4.0.7.RELEASE_3");
        installAndAssertFeature("activemq-client");
        verifyBundleInstalledAndRegisteredServices("activemq-osgi", 2);
    }

    @Test(timeout=5 * 60 * 1000)
    public void testClientWithSpring41() throws Throwable {
        executeCommand("feature:install spring/4.1.7.RELEASE_2");
        installAndAssertFeature("activemq-client");
        verifyBundleInstalledAndRegisteredServices("activemq-osgi", 2);
    }

    @Test(timeout=5 * 60 * 1000)
    public void testClientWithSpring42() throws Throwable {
        executeCommand("feature:install spring/4.2.2.RELEASE_1");
        installAndAssertFeature("activemq-client");
        verifyBundleInstalledAndRegisteredServices("activemq-osgi", 2);
    }

    public boolean verifyBundleInstalledAndRegisteredServices(final String bundleName, final int numberOfServices) throws Exception {
        boolean found = false;
        for (final Bundle bundle : bundleContext.getBundles()) {
            LOG.debug("Checking: " + bundle.getSymbolicName());
            if (bundle.getSymbolicName().contains(bundleName)) {
                Assert.assertEquals(Bundle.ACTIVE, bundle.getState());
                // Assert that the bundle has registered some services via blueprint
                Assert.assertNotNull(bundle.getRegisteredServices());
                // Assert that the bundle has registered the correct number of services
                Assert.assertEquals(numberOfServices, bundle.getRegisteredServices().length);
                found = true;
                break;
            }
        }
        return found;
    }
}

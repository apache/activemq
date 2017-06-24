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
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.osgi.framework.Bundle;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class ObrFeatureTest extends AbstractFeatureTest {

    @Configuration
    public static Option[] configure() {
        return new Option[] //
            {
             configure("obr"),
             editConfigurationFilePut("etc/system.properties", "camel.version", camelVersion()),
        };
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testClient() throws Throwable {
        installAndAssertFeature("activemq-client");
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testActiveMQ() throws Throwable {
        installAndAssertFeature("activemq");
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testBroker() throws Throwable {
        installAndAssertFeature("activemq-broker");
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testCamel() throws Throwable {
        installAndAssertFeature("activemq-camel");
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testClientWithSpring31() throws Throwable {
        testWithSpringVersion("3.1.4.RELEASE");
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testClientWithSpring32() throws Throwable {
        testWithSpringVersion("3.2.18.RELEASE_1");
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testClientWithSpring40() throws Throwable {
        testWithSpringVersion("4.0.7.RELEASE_3");
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testClientWithSpring41() throws Throwable {
        testWithSpringVersion("4.1.9.RELEASE_1");
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testClientWithSpring42() throws Throwable {
        testWithSpringVersion("4.2.9.RELEASE_1");
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testClientWithSpring43() throws Throwable {
        testWithSpringVersion("4.3.5.RELEASE_1");
    }

    private void testWithSpringVersion(String version) throws Exception, Throwable {
        featuresService.installFeature("spring", version);
        installAndAssertFeature("activemq-client");
        verifyBundleInstalledAndRegisteredServices("activemq-osgi", 2);
    }

    private void verifyBundleInstalledAndRegisteredServices(final String bundleName,
                                                           final int numberOfServices)
        throws Exception {
        Bundle bundle = getBundle(bundleName);
        Assert.assertEquals(Bundle.ACTIVE, bundle.getState());
        // Assert that the bundle has registered some services via blueprint
        Assert.assertNotNull(bundle.getRegisteredServices());
        // Assert that the bundle has registered the correct number of services
        Assert.assertEquals(numberOfServices, bundle.getRegisteredServices().length);
    }
}

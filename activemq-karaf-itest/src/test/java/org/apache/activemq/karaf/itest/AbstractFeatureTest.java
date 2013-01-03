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

import org.junit.After;
import org.junit.Before;
import org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.UrlReference;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.replaceConfigurationFile;
import static org.ops4j.pax.exam.CoreOptions.*;

public abstract class AbstractFeatureTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFeatureTest.class);
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

//    protected void testComponent(String component) throws Exception {
//        long max = System.currentTimeMillis() + 10000;
//        while (true) {
//            try {
//                assertNotNull("Cannot get component with name: " + component, createCamelContext().getComponent(component));
//                return;
//            } catch (Exception t) {
//                if (System.currentTimeMillis() < max) {
//                    Thread.sleep(1000);
//                } else {
//                    throw t;
//                }
//            }
//        }
//    }
//
//    protected void testDataFormat(String format) throws Exception {
//        long max = System.currentTimeMillis() + 10000;
//        while (true) {
//            try {
//                DataFormatDefinition dataFormatDefinition = createDataformatDefinition(format);
//                assertNotNull(dataFormatDefinition);
//                assertNotNull(dataFormatDefinition.getDataFormat(new DefaultRouteContext(createCamelContext())));
//                return;
//            } catch (Exception t) {
//                if (System.currentTimeMillis() < max) {
//                    Thread.sleep(1000);
//                    continue;
//                } else {
//                    throw t;
//                }
//            }
//        }
//    }
//
//    protected DataFormatDefinition createDataformatDefinition(String format) {
//        return null;
//    }

//    protected void testLanguage(String lang) throws Exception {
//        long max = System.currentTimeMillis() + 10000;
//        while (true) {
//            try {
//                assertNotNull(createCamelContext().resolveLanguage(lang));
//                return;
//            } catch (Exception t) {
//                if (System.currentTimeMillis() < max) {
//                    Thread.sleep(1000);
//                    continue;
//                } else {
//                    throw t;
//                }
//            }
//        }
//    }

//    protected CamelContext createCamelContext() throws Exception {
//        CamelContextFactory factory = new CamelContextFactory();
//        factory.setBundleContext(bundleContext);
//        LOG.info("Get the bundleContext is " + bundleContext);
//        return factory.createContext();
//    }


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
            artifactId("activemq-karaf").version(activemqVersion()).type(type);
    }
    
    public static UrlReference getKarafFeatureUrl() {
        LOG.info("*** The karaf version is " + karafVersion() + " ***");

        String type = "xml/features";
        return mavenBundle().groupId("org.apache.karaf.assemblies.features").
            artifactId("standard").version(karafVersion()).type(type);
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
                scanFeatures(getActiveMQKarafFeatureUrl(), f.toArray(new String[f.size()]))};

        return options;
    }

}

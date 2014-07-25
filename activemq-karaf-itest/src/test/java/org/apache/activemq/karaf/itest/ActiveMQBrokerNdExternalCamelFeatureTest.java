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

import java.io.File;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.MavenUtils;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.replaceConfigurationFile;
import static org.ops4j.pax.exam.CoreOptions.scanFeatures;

@RunWith(JUnit4TestRunner.class)
public class ActiveMQBrokerNdExternalCamelFeatureTest extends AbstractJmsFeatureTest {

    @Configuration
    public static Option[] configure() {
        // copy camel.xml into a temporary directory in karaf, so we later can hot-deploy it
        Option[] baseOptions = append(
                replaceConfigurationFile("data/tmp/camel.xml", new File(basedir + "/src/test/resources/org/apache/activemq/karaf/itest/camel.xml")),
                configure("activemq", "activemq-camel"));
        return configureBrokerStart(append(scanFeatures(getCamelFeatureUrl(
                MavenUtils.getArtifactVersion("org.apache.camel.karaf", "apache-camel")
        ), "activemq-camel"), baseOptions));
    }

    @Test
    public void test() throws Throwable {

        System.err.println(executeCommand("features:list").trim());
        System.err.println(executeCommand("osgi:list").trim());

        withinReason(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                assertEquals("brokerName = amq-broker", executeCommand("activemq:list").trim());
                return true;
            }
        });


        withinReason(new Callable<Boolean>(){
            @Override
            public Boolean call() throws Exception {
                assertTrue(executeCommand("activemq:bstat").trim().contains("BrokerName = amq-broker"));
                return true;
            }
        });

        System.err.println(executeCommand("activemq:bstat").trim());

        // hot deploy the camel.xml file by copying it to the deploy directory
        System.err.println("Karaf is running in dir: " + System.getProperty("karaf.base"));
        String karafDir = System.getProperty("karaf.base");
        System.err.println("Hot deploying Camel application");
        copyFile(new File(karafDir + "/data/tmp/camel.xml"), new File(karafDir + "/deploy/camel.xml"));
        Thread.sleep(3 * 1000);
        System.err.println("Continuing...");

        // produce and consume
        final String nameAndPayload = String.valueOf(System.currentTimeMillis());
        produceMessage("camel_in");
        assertEquals("got our message", "camel_in", consumeMessage("camel_out"));
    }

}

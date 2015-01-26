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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.MavenUtils;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.replaceConfigurationFile;

@RunWith(PaxExam.class)
public class ActiveMQBrokerNdExternalCamelFeatureTest extends AbstractJmsFeatureTest {

    @Configuration
    public static Option[] configure() {
        Option[] baseOptions = configure("activemq");
        // copy camel.xml into a temporary directory in karaf, so we later can hot-deploy it
        baseOptions = append(replaceConfigurationFile("data/tmp/camel.xml", new File(basedir + "/src/test/resources/org/apache/activemq/karaf/itest/camel.xml")), baseOptions);
        baseOptions = append(editConfigurationFilePut("etc/system.properties", "camel.version", MavenUtils.getArtifactVersion("org.apache.camel.karaf", "apache-camel")), baseOptions);
        return configureBrokerStart(baseOptions);
    }

    @Ignore("camel.xml from auto deploy directory does not seem to get picked up, no idea why atm")
    @Test(timeout = 2 * 60 * 1000)
    public void test() throws Throwable {

        assertFeatureInstalled("activemq");
        executeCommand("features:addurl " + getCamelFeatureUrl());
        installAndAssertFeature("camel");
        installAndAssertFeature("activemq-camel");

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

        withinReason(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                assertTrue("activemq-camel bundle installed", verifyBundleInstalled("org.apache.activemq.activemq-camel"));
                return true;
            }
        });

        assertFeatureInstalled("activemq-camel");

        System.err.println(executeCommand("activemq:bstat").trim());

        // hot deploy the camel.xml file by copying it to the deploy directory
        System.err.println("Karaf is running in dir: " + System.getProperty("karaf.base"));
        String karafDir = System.getProperty("karaf.base");
        System.err.println("Hot deploying Camel application");
        copyFile(new File(karafDir + "/data/tmp/camel.xml"), new File(karafDir + "/deploy/camel.xml"));

        withinReason(new Callable<Boolean>(){
            @Override
            public Boolean call() throws Exception {
                assertTrue("we have camel consumers", executeCommand("activemq:dstat").trim().contains("camel_in"));
                return true;
            }
        });

        // produce and consume
        produceMessage("camel_in");
        assertEquals("got our message", "camel_in", consumeMessage("camel_out"));
    }

}

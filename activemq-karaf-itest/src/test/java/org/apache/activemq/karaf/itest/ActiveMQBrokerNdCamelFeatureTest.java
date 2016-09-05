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
import java.util.Date;
import java.util.concurrent.Callable;

import org.apache.karaf.features.Feature;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.MavenUtils;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.junit.PaxExam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.replaceConfigurationFile;

@RunWith(PaxExam.class)
public class ActiveMQBrokerNdCamelFeatureTest extends AbstractJmsFeatureTest {

    @Configuration
    public static Option[] configure() {
        return append(
                editConfigurationFilePut("etc/system.properties", "camel.version", MavenUtils.getArtifactVersion("org.apache.camel.karaf", "apache-camel")),
                configure("activemq", "activemq-camel"));
    }

    @Test(timeout = 2 * 60 * 1000)
    public void test() throws Throwable {
        System.err.println(executeCommand("feature:list").trim());

        assertFeatureInstalled("activemq");

        assertTrue("activemq-camel bundle installed", verifyBundleInstalled("org.apache.activemq.activemq-camel"));

        withinReason(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                assertTrue("activemq-camel bundle installed", verifyBundleInstalled("org.apache.activemq.activemq-camel"));
                return true;
            }
        });

        // start broker with embedded camel route
        String karafDir = System.getProperty("karaf.base");
        File target = new File(karafDir + "/etc/activemq.xml");
        copyFile(new File(basedir + "/../../../src/test/resources/org/apache/activemq/karaf/itest/activemq-nd-camel.xml"), target);
        target = new File(karafDir + "/etc/org.apache.activemq.server-default.cfg");
        copyFile(new File(basedir + "/../../../src/test/resources/org/apache/activemq/karaf/itest/org.apache.activemq.server-default.cfg"), target);

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

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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.junit.PaxExam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.replaceConfigurationFile;

@RunWith(PaxExam.class)
public class ActiveMQBrokerNdCamelFeatureTest extends AbstractFeatureTest {

    @Configuration
    public static Option[] configure() {
        return new Option[] //
        {
         composite(configure("activemq-broker", "activemq-shell", "activemq-camel")),
         editConfigurationFilePut("etc/system.properties", "camel.version", camelVersion()),
         replaceConfigurationFile("etc/activemq.xml", new File(RESOURCE_BASE + "activemq-nd-camel.xml")),
         replaceConfigurationFile("etc/org.apache.activemq.server-default.cfg", new File(RESOURCE_BASE + "org.apache.activemq.server-default.cfg"))
        };
    }

    @Test(timeout = 2 * 60 * 1000)
    @Ignore("camel-jms should be used")
    public void test() throws Throwable {
        System.err.println(executeCommand("feature:list -i").trim());
        assertFeatureInstalled("activemq");
        assertFeatureInstalled("activemq-shell");
        assertBrokerStarted();
        withinReason(new Runnable() {
            public void run() {
                getBundle("org.apache.activemq.activemq-camel");
                assertTrue("we have camel consumers", executeCommand("activemq:dstat").trim().contains("camel_in"));
            }
        });

        // produce and consume
        JMSTester jms = new JMSTester();
        jms.produceMessage("camel_in");
        assertEquals("got our message", "camel_in", jms.consumeMessage("camel_out"));
        jms.close();
    }
}

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
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.MavenUtils;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.junit.PaxExam;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.replaceConfigurationFile;

@RunWith(PaxExam.class)
public class ActiveMQBrokerRuntimeConfigTest extends AbstractJmsFeatureTest {

    @Configuration
    public static Option[] configure() {
        return append(editConfigurationFilePut("etc/org.apache.activemq.server-default.cfg", "config.check", "false"),
                configureBrokerStart(
                        append(replaceConfigurationFile("data/tmp/modified-config.xml",
                                new File(basedir + "/src/test/resources/org/apache/activemq/karaf/itest/activemq-runtime-config-mod.xml")),
                                configure("activemq")), "activemq-runtime-config"));
    }

    @Test(timeout = 2 * 60 * 1000)
    public void test() throws Throwable {

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
                assertTrue("3MB limit", executeCommand("activemq:query").trim().contains("MemoryLimit = 3145728"));
                return true;
            }
        });

        // ensure update will be reflected in OS fs modified window
        TimeUnit.SECONDS.sleep(4);

        // increase from 3mb to 4mb and check
        String karafDir = System.getProperty("karaf.base");
        File target = new File(karafDir + "/etc/activemq.xml");
        System.err.println("Modifying configuration at: " + target + "last mod: " + new Date(target.lastModified()));
        copyFile(new File(karafDir + "/data/tmp/modified-config.xml"), target);
        System.err.println("new mod at: " + new Date(target.lastModified()));

        withinReason(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                assertTrue("4MB limit", executeCommand("activemq:query").trim().contains("MemoryLimit = 4194304"));
                return true;
            }
        });

    }
}

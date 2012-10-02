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
package org.apache.activemq.karaf;

import org.apache.karaf.testing.AbstractIntegrationTest;
import org.apache.karaf.testing.Helper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;

import static org.ops4j.pax.exam.CoreOptions.*;
import static org.ops4j.pax.exam.OptionUtils.combine;
import static org.ops4j.pax.exam.container.def.PaxRunnerOptions.scanFeatures;
import static org.ops4j.pax.exam.container.def.PaxRunnerOptions.workingDirectory;

@RunWith(JUnit4TestRunner.class)
public class KarafIntegrationTest extends AbstractIntegrationTest {

    @Configuration
    public static Option[] configuration() throws Exception {
        return combine(
                // Default karaf environment
                Helper.getDefaultOptions(
                        systemProperty("org.ops4j.pax.logging.DefaultServiceLog.level").value("DEBUG")),

                scanFeatures(
                        maven().groupId("org.apache.activemq").artifactId("activemq-karaf").type("xml").classifier("features").versionAsInProject(),
                        "activemq"
                ),

                workingDirectory("target/paxrunner/features/"),

                waitForFrameworkStartup(),

                // Test on both equinox and felix
                equinox(), felix()
        );
    }

    @Test
    public void testFeatures() throws Exception {
        // Run some commands to make sure they are installed properly
//        CommandProcessor cp = getOsgiService(CommandProcessor.class);
//        CommandSession cs = cp.createSession(System.in, System.out, System.err);
//        Object res = cs.execute("osgi:list");
//        System.out.println(res);
//        cs.close();
    }
}

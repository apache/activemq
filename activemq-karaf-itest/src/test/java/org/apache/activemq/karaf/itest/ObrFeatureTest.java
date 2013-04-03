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

import javax.jms.Destination;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.MavenUtils;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;


import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.editConfigurationFilePut;


@RunWith(JUnit4TestRunner.class)
public class ObrFeatureTest extends AbstractFeatureTest {

	@Configuration
	public static Option[] configure() {
		return append(
                editConfigurationFilePut("etc/system.properties", "camel.version", MavenUtils.getArtifactVersion("org.apache.camel.karaf", "apache-camel")),
                configure("obr"));
	}

	@Test
	public void testClient() throws Throwable {
		installAndAssertFeature("activemq-client");
	}

	@Test
	public void testActiveMQ() throws Throwable {
		installAndAssertFeature("activemq");
	}

    @Test
   	public void testBroker() throws Throwable {
   		installAndAssertFeature("activemq-broker");
   	}

    @Test
   	public void testCamel() throws Throwable {
        System.err.println(executeCommand("features:addurl " + getCamelFeatureUrl()));
   		installAndAssertFeature("activemq-camel");
   	}
}

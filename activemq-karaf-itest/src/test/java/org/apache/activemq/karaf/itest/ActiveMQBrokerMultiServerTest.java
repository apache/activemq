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

import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.replaceConfigurationFile;

import java.io.File;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;

@RunWith(PaxExam.class)
public class ActiveMQBrokerMultiServerTest extends AbstractFeatureTest {

    @Configuration
    public static Option[] configure() {
        return new Option[] //
        { //
                configure("activemq"), //
                editConfigurationFilePut("etc/org.apache.activemq.server-multibroker1.cfg", "config.check", "false"),
                editConfigurationFilePut("etc/org.apache.activemq.server-multibroker2.cfg", "config.check", "false"),
                editConfigurationFilePut("etc/org.apache.activemq.server-multibroker3.cfg", "config.check", "false"),
                configureBrokerStart("activemq-multi-broker") };
    }

    @Test(timeout = 2 * 60 * 1000)
    public void test() throws Throwable {
        assertBrokerStarted("multibroker1");
        assertBrokerStarted("multibroker2");
        assertBrokerStarted("multibroker3");

        // ensure update will be reflected in OS fs modified window
        TimeUnit.SECONDS.sleep(4);

        // TODO: Perform a _bundle_ shutdown to go through ActiveMQServiceFactory.destroy()

    }
}

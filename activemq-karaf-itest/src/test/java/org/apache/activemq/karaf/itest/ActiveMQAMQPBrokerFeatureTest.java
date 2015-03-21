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

import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.junit.PaxExam;

import javax.jms.Connection;

@RunWith(PaxExam.class)
public class ActiveMQAMQPBrokerFeatureTest extends ActiveMQBrokerFeatureTest {
    private static final Integer AMQP_PORT = 61636;

    @Configuration
    public static Option[] configure() {
        Option[] activeMQOptions = configure("activemq");
        final String fragmentHost = "qpid-amqp-jms-client";
        Option qpidClient = CoreOptions.wrappedBundle(CoreOptions.mavenBundle("org.apache.qpid", "qpid-amqp-1-0-client").versionAsInProject().getURL().toString() + "$Bundle-SymbolicName=qpid-amqp-client&Fragment-Host=" + fragmentHost);
        Option qpidClientJms = CoreOptions.wrappedBundle(CoreOptions.mavenBundle("org.apache.qpid", "qpid-amqp-1-0-client-jms").versionAsInProject().getURL().toString() + "$Bundle-SymbolicName=" + fragmentHost);
        Option qpidCommon = CoreOptions.wrappedBundle(CoreOptions.mavenBundle("org.apache.qpid", "qpid-amqp-1-0-common").versionAsInProject().getURL().toString());

        Option[] options = append(qpidClient, activeMQOptions);
        options = append(qpidClientJms, options);
        options = append(qpidCommon, options);

        Option[] configuredOptions = configureBrokerStart(options);
        return configuredOptions;
    }

    @Override
    protected Connection getConnection() throws Throwable {

        ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", AMQP_PORT, AbstractFeatureTest.USER, AbstractFeatureTest.PASSWORD);
        Connection connection = null;
        ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
        try {
            // ensure service loader uses a loader that can find the impl - not the system classpath
            Thread.currentThread().setContextClassLoader(factory.getClass().getClassLoader());
            connection = factory.createConnection();
            connection.start();
        } finally {
            Thread.currentThread().setContextClassLoader(originalLoader);
        }
        return connection;
    }

    @Ignore
    @Test(timeout = 5 * 60 * 1000)
    public void testTemporaryDestinations() throws Throwable {
        // ignore until we have temporary destination are working in amqp
    }
}

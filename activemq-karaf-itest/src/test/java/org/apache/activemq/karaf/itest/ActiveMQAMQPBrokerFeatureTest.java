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

import javax.jms.Connection;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;

@Ignore
@RunWith(PaxExam.class)
public class ActiveMQAMQPBrokerFeatureTest extends ActiveMQBrokerFeatureTest {
    private static final Integer AMQP_PORT = 61636;

    @Configuration
    public static Option[] configure() {
        Option[] activeMQOptions = configure("activemq");
        Option qpidClient = CoreOptions.wrappedBundle(CoreOptions.mavenBundle("org.apache.qpid", "qpid-jms-client").versionAsInProject().getURL().toString() + "$Bundle-SymbolicName=qpid-jms-client");

        Option[] options = append(qpidClient, activeMQOptions);

        Option[] configuredOptions = configureBrokerStart(options);
        return configuredOptions;
    }

    @Override
    protected Connection getConnection() throws Throwable {

        String amqpURI = "amqp://localhost" + AMQP_PORT;
        JmsConnectionFactory factory = new JmsConnectionFactory(amqpURI);

        factory.setUsername(AbstractFeatureTest.USER);
        factory.setPassword(AbstractFeatureTest.PASSWORD);

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

    @Override
    @Ignore
    @Test(timeout = 5 * 60 * 1000)
    public void testTemporaryDestinations() throws Throwable {
        // ignore until we have temporary destination are working in amqp
    }
}

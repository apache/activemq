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
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.ops4j.pax.exam.options.MavenArtifactProvisionOption;

import javax.jms.Connection;
import javax.jms.JMSException;

@RunWith(JUnit4TestRunner.class)
public class ActiveMQAMQPBrokerFeatureTest extends ActiveMQBrokerFeatureTest {
    private static final Integer AMQP_PORT = 61636;

    @Configuration
    public static Option[] configure() {
        Option[] activeMQOptions = configure("activemq");

        MavenArtifactProvisionOption qpidClient = CoreOptions.mavenBundle("org.apache.qpid", "qpid-amqp-1-0-client").versionAsInProject();
        MavenArtifactProvisionOption qpidClientJms = CoreOptions.mavenBundle("org.apache.qpid", "qpid-amqp-1-0-client-jms").versionAsInProject();
        MavenArtifactProvisionOption qpidCommon = CoreOptions.mavenBundle("org.apache.qpid", "qpid-amqp-1-0-common").versionAsInProject();
        MavenArtifactProvisionOption geronimoJms = CoreOptions.mavenBundle("org.apache.geronimo.specs", "geronimo-jms_1.1_spec").versionAsInProject();
        MavenArtifactProvisionOption geronimoJta = CoreOptions.mavenBundle("org.apache.geronimo.specs", "geronimo-jta_1.1_spec","1.1.1");

        Option[] options = append(qpidClient, activeMQOptions);
        options = append(qpidClientJms, options);
        options = append(qpidCommon, options);
        options = append(geronimoJms, options);
        options = append(geronimoJta, options);

        Option[] configuredOptions = configureBrokerStart(options);
        return configuredOptions;
    }

    @Override
    protected Connection getConnection() throws JMSException {
        ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", AMQP_PORT, AbstractFeatureTest.USER, AbstractFeatureTest.PASSWORD);
        Connection connection = factory.createConnection();
        connection.start();

        return connection;
    }
}

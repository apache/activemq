/*
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
import org.junit.Test;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;

public class ActiveMQAMQPBrokerFeatureTest extends AbstractFeatureTest {
    private static final Integer AMQP_PORT = 61636;

    @Configuration
    public static Option[] configure() {
        return new Option[] //
        {
         CoreOptions.mavenBundle("org.apache.geronimo.specs","geronimo-jms_2.0_spec").version("1.0-alpha-2"),
         configure("activemq", "activemq-amqp-client"), //
         configureBrokerStart()
        };
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testProduceConsume() throws Throwable {
    	JMSTester tester = new JMSTester(getQPIDConnection());
    	tester.produceAndConsume(sessionFactory);
    	tester.close();
    }

	protected Connection getQPIDConnection() throws Exception {
		assertBrokerStarted();
	    assertQpidClient();
	
	    JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + AMQP_PORT);
	    factory.setUsername(KarafShellHelper.USER);
	    factory.setPassword(KarafShellHelper.PASSWORD);
	    Connection connection = factory.createConnection();
	    connection.start();
	    return connection;
	}

	private void assertQpidClient() throws Exception {
		withinReason(new Runnable() {
	        public void run() {
	            getBundle("org.apache.qpid.jms.client");
	        }
	    });
	}
}

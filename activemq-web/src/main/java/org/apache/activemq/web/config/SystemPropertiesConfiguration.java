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
package org.apache.activemq.web.config;

import java.util.Collection;

import javax.jms.ConnectionFactory;
import javax.management.remote.JMXServiceURL;


/**
 * Configuration based on system-properties.
 * 
 * 
 */
public class SystemPropertiesConfiguration extends AbstractConfiguration {

	private static final String PROPERTY_JMS_URL = "webconsole.jms.url";
	private static final String PROPERTY_JMS_USER = "webconsole.jms.user";
	private static final String PROPERTY_JMS_PASSWORD = "webconsole.jms.password";

	private static final String PROPERTY_JMX_URL = "webconsole.jmx.url";
	private static final String PROPERTY_JMX_USER = "webconsole.jmx.user";
	private static final String PROPERTY_JMX_PASSWORD = "webconsole.jmx.password";

	public ConnectionFactory getConnectionFactory() {
		String jmsUrl = System.getProperty(PROPERTY_JMS_URL);
		if (jmsUrl == null)
			throw new IllegalArgumentException(
					"A JMS-url must be specified (system property "
							+ PROPERTY_JMS_URL);

		String jmsUser = System.getProperty(PROPERTY_JMS_USER);
		String jmsPassword = System.getProperty(PROPERTY_JMS_PASSWORD);
		return makeConnectionFactory(jmsUrl, jmsUser, jmsPassword);
	}

	public Collection<JMXServiceURL> getJmxUrls() {
		String jmxUrls = System.getProperty(PROPERTY_JMX_URL);
		return makeJmxUrls(jmxUrls);
	}

	public String getJmxPassword() {
		return System.getProperty(PROPERTY_JMX_PASSWORD);
	}

	public String getJmxUser() {
		return System.getProperty(PROPERTY_JMX_USER);
	}

}
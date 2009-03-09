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

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;

import javax.jms.ConnectionFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Base class for configurations.
 * 
 * @version $Revision: $
 */
public abstract class AbstractConfiguration implements WebConsoleConfiguration {

	public ConnectionFactory getConnectionFactory() {
		return null;
	}

	public String getJmxPassword() {
		return null;
	}

	public Collection<JMXServiceURL> getJmxUrls() {
		return null;
	}

	public String getJmxUser() {
		return null;
	}

	/**
	 * Creates the ActiveMQ-ConnectionFactory.
	 * 
	 * @param jmsUrl
	 *            not <code>null</code>
	 * @param jmsUser
	 *            <code>null</code> if no authentication
	 * @param jmsPassword
	 *            <code>null</code> is ok
	 * @return not <code>null</code>
	 */
	protected ConnectionFactory makeConnectionFactory(String jmsUrl, String jmsUser,
			String jmsPassword) {
				if (jmsUser != null && jmsUser.length() > 0)
					return new ActiveMQConnectionFactory(jmsUser, jmsPassword, jmsUrl);
				else
					return new ActiveMQConnectionFactory(jmsUrl);
			}

	/**
	 * Splits the JMX-Url string into a series of JMSServiceURLs.
	 * 
	 * @param jmxUrls
	 *            the JMX-url, multiple URLs are separated by commas.
	 * @return not <code>null</code>, contains at least one element.
	 */
	protected Collection<JMXServiceURL> makeJmxUrls(String jmxUrls) {
		String[] urls = jmxUrls.split(",");
		if (urls == null || urls.length == 0) {
			urls = new String[] { jmxUrls };
		}
	
		try {
			Collection<JMXServiceURL> result = new ArrayList<JMXServiceURL>(
					jmxUrls.length());
			for (String url : urls) {
				result.add(new JMXServiceURL(url));
			}
			return result;
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("Invalid JMX-url", e);
		}
	}

}
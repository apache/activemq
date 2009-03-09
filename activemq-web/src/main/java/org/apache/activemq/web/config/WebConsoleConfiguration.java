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
 * The configuration used for the web console.
 * 
 * @version $Revision: $
 */
public interface WebConsoleConfiguration {

	/**
	 * The connection factory to use for sending/receiving messages.
	 * 
	 * @return not <code>null</code>
	 */
	ConnectionFactory getConnectionFactory();

	/**
	 * The URL to the JMX connectors of the broker. The names of any failover
	 * (master-slave configuration) must also be specified.
	 * 
	 * @return not <code>null</code>, must contain at least one entry
	 */
	Collection<JMXServiceURL> getJmxUrls();

	/**
	 * The user that is used in case of authenticated JMX connections. The user
	 * must be the same for all the brokers.
	 * 
	 * @return <code>null</code> if no authentication should be used.
	 */
	String getJmxUser();

	/**
	 * Password for the JMX-user.
	 * 
	 * @see #getJmxUser()
	 * @return <code>null</code> if no authentication
	 */
	String getJmxPassword();

}

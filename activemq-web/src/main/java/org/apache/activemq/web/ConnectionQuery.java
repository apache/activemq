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
package org.apache.activemq.web;

import java.util.Collection;
import java.util.Collections;

import org.apache.activemq.broker.jmx.ConnectionViewMBean;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;

/**
 * Query for a single connection.
 * 
 * @author ms
 */
public class ConnectionQuery {

	private final BrokerFacade mBrokerFacade;
	private String mConnectionID;

	public ConnectionQuery(BrokerFacade brokerFacade) {
		mBrokerFacade = brokerFacade;
	}

	public void destroy() {
		// empty
	}

	public void setConnectionID(String connectionID) {
		mConnectionID = connectionID;
	}

	public String getConnectionID() {
		return mConnectionID;
	}

	public ConnectionViewMBean getConnection() throws Exception {
		String connectionID = getConnectionID();
		if (connectionID == null)
			return null;
		return mBrokerFacade.getConnection(connectionID);
	}

	public Collection<SubscriptionViewMBean> getConsumers() throws Exception {
		String connectionID = getConnectionID();
		if (connectionID == null)
			return Collections.emptyList();
		return mBrokerFacade.getConsumersOnConnection(connectionID);
	}

}
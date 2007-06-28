/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.jmx;

import org.apache.activemq.network.NetworkConnector;

public class NetworkConnectorView implements NetworkConnectorViewMBean {

    private final NetworkConnector connector;

    public NetworkConnectorView(NetworkConnector connector) {
        this.connector = connector;
    }
    
    public void start() throws Exception {
        connector.start();
    }

    public void stop() throws Exception {
        connector.stop();       
    }

	public String getName() {
		return connector.getName();
	}

	public int getNetworkTTL() {
		return connector.getNetworkTTL();
	}

	public int getPrefetchSize() {
		return connector.getPrefetchSize();
	}

	public String getUserName() {
		return connector.getUserName();
	}

	public boolean isBridgeTempDestinations() {
		return connector.isBridgeTempDestinations();
	}

	public boolean isConduitSubscriptions() {
		return connector.isConduitSubscriptions();
	}

	public boolean isDecreaseNetworkConsumerPriority() {
		return connector.isDecreaseNetworkConsumerPriority();
	}

	public boolean isDispatchAsync() {
		return connector.isDispatchAsync();
	}

	public boolean isDynamicOnly() {
		return connector.isDynamicOnly();
	}

	public void setBridgeTempDestinations(boolean bridgeTempDestinations) {
		connector.setBridgeTempDestinations(bridgeTempDestinations);
	}

	public void setConduitSubscriptions(boolean conduitSubscriptions) {
		connector.setConduitSubscriptions(conduitSubscriptions);
	}

	public void setDispatchAsync(boolean dispatchAsync) {
		connector.setDispatchAsync(dispatchAsync);
	}

	public void setDynamicOnly(boolean dynamicOnly) {
		connector.setDynamicOnly(dynamicOnly);
	}

	public void setNetworkTTL(int networkTTL) {
		connector.setNetworkTTL(networkTTL);
	}

	public void setPassword(String password) {
		connector.setPassword(password);
	}

	public void setPrefetchSize(int prefetchSize) {
		connector.setPrefetchSize(prefetchSize);
	}

	public void setUserName(String userName) {
		connector.setUserName(userName);
	}

	public String getPassword() {
		String pw = connector.getPassword();
		// Hide the password for security reasons.
		if( pw!= null ) 
			pw = pw.replaceAll(".", "*");
		return pw;
	}

	public void setDecreaseNetworkConsumerPriority(boolean decreaseNetworkConsumerPriority) {
		connector.setDecreaseNetworkConsumerPriority(decreaseNetworkConsumerPriority);
	}
}

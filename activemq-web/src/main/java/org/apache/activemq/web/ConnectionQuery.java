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
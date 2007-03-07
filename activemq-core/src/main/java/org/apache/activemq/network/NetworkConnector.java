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
package org.apache.activemq.network;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @version $Revision$
 */
public abstract class NetworkConnector extends ServiceSupport {

    protected static final Log log = LogFactory.getLog(NetworkConnector.class);
    protected URI localURI;
    private String brokerName = "localhost";

    private Set durableDestinations;
    private List excludedDestinations = new CopyOnWriteArrayList();
    private List dynamicallyIncludedDestinations = new CopyOnWriteArrayList();
    private List staticallyIncludedDestinations = new CopyOnWriteArrayList();
    protected boolean dynamicOnly = false;
    protected boolean conduitSubscriptions = true;
    private boolean decreaseNetworkConsumerPriority;
    private int networkTTL = 1;
    private String name = "bridge";
    private int prefetchSize = 1000;
    private boolean dispatchAsync = true;
    private String userName;
    private String password;
    private boolean bridgeTempDestinations=true;
    
    protected ConnectionFilter connectionFilter;

    public NetworkConnector() {
    }

    public NetworkConnector(URI localURI) {
        this.localURI = localURI;
    }

    public URI getLocalUri() throws URISyntaxException {
        return localURI;
    }

    public void setLocalUri(URI localURI) {
        this.localURI = localURI;
    }

    /**
     * @return Returns the name.
     */
    public String getName() {
        if (name == null) {
            name = createName();
        }
        return name;
    }

    /**
     * @param name
     *            The name to set.
     */
    public void setName(String name) {
        this.name = name;
    }

    public String getBrokerName() {
        return brokerName;
    }

    /**
     * @param brokerName
     *            The brokerName to set.
     */
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    /**
     * @return Returns the durableDestinations.
     */
    public Set getDurableDestinations() {
        return durableDestinations;
    }

    /**
     * @param durableDestinations
     *            The durableDestinations to set.
     */
    public void setDurableDestinations(Set durableDestinations) {
        this.durableDestinations = durableDestinations;
    }

    /**
     * @return Returns the dynamicOnly.
     */
    public boolean isDynamicOnly() {
        return dynamicOnly;
    }

    /**
     * @param dynamicOnly
     *            The dynamicOnly to set.
     */
    public void setDynamicOnly(boolean dynamicOnly) {
        this.dynamicOnly = dynamicOnly;
    }

    /**
     * @return Returns the conduitSubscriptions.
     */
    public boolean isConduitSubscriptions() {
        return conduitSubscriptions;
    }

    /**
     * @param conduitSubscriptions
     *            The conduitSubscriptions to set.
     */
    public void setConduitSubscriptions(boolean conduitSubscriptions) {
        this.conduitSubscriptions = conduitSubscriptions;
    }

    /**
     * @return Returns the decreaseNetworkConsumerPriority.
     */
    public boolean isDecreaseNetworkConsumerPriority() {
        return decreaseNetworkConsumerPriority;
    }

    /**
     * @param decreaseNetworkConsumerPriority
     *            The decreaseNetworkConsumerPriority to set.
     */
    public void setDecreaseNetworkConsumerPriority(boolean decreaseNetworkConsumerPriority) {
        this.decreaseNetworkConsumerPriority = decreaseNetworkConsumerPriority;
    }

    /**
     * @return Returns the networkTTL.
     */
    public int getNetworkTTL() {
        return networkTTL;
    }

    /**
     * @param networkTTL
     *            The networkTTL to set.
     */
    public void setNetworkTTL(int networkTTL) {
        this.networkTTL = networkTTL;
    }

    /**
     * @return Returns the excludedDestinations.
     */
    public List getExcludedDestinations() {
        return excludedDestinations;
    }

    /**
     * @param excludedDestinations
     *            The excludedDestinations to set.
     */
    public void setExcludedDestinations(List exludedDestinations) {
        this.excludedDestinations = exludedDestinations;
    }

    public void addExcludedDestination(ActiveMQDestination destiantion) {
        this.excludedDestinations.add(destiantion);
    }

    /**
     * @return Returns the staticallyIncludedDestinations.
     */
    public List getStaticallyIncludedDestinations() {
        return staticallyIncludedDestinations;
    }

    /**
     * @param staticallyIncludedDestinations
     *            The staticallyIncludedDestinations to set.
     */
    public void setStaticallyIncludedDestinations(List staticallyIncludedDestinations) {
        this.staticallyIncludedDestinations = staticallyIncludedDestinations;
    }

    public void addStaticallyIncludedDestination(ActiveMQDestination destiantion) {
        this.staticallyIncludedDestinations.add(destiantion);
    }

    /**
     * @return Returns the dynamicallyIncludedDestinations.
     */
    public List getDynamicallyIncludedDestinations() {
        return dynamicallyIncludedDestinations;
    }

    /**
     * @param dynamicallyIncludedDestinations
     *            The dynamicallyIncludedDestinations to set.
     */
    public void setDynamicallyIncludedDestinations(List dynamicallyIncludedDestinations) {
        this.dynamicallyIncludedDestinations = dynamicallyIncludedDestinations;
    }

    public void addDynamicallyIncludedDestination(ActiveMQDestination destiantion) {
        this.dynamicallyIncludedDestinations.add(destiantion);
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Bridge configureBridge(DemandForwardingBridgeSupport result) {
        result.setLocalBrokerName(getBrokerName());
        result.setName(getBrokerName());
        result.setNetworkTTL(getNetworkTTL());
        result.setUserName(userName);
        result.setPassword(password);
        result.setPrefetchSize(prefetchSize);
        result.setDispatchAsync(dispatchAsync);
        result.setDecreaseNetworkConsumerPriority(isDecreaseNetworkConsumerPriority());

        List destsList = getDynamicallyIncludedDestinations();
        ActiveMQDestination dests[] = (ActiveMQDestination[]) destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setDynamicallyIncludedDestinations(dests);

        destsList = getExcludedDestinations();
        dests = (ActiveMQDestination[]) destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setExcludedDestinations(dests);

        destsList = getStaticallyIncludedDestinations();
        dests = (ActiveMQDestination[]) destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setStaticallyIncludedDestinations(dests);
        
        result.setBridgeTempDestinations(bridgeTempDestinations);

        if (durableDestinations != null) {
            ActiveMQDestination[] dest = new ActiveMQDestination[durableDestinations.size()];
            dest = (ActiveMQDestination[]) durableDestinations.toArray(dest);
            result.setDurableDestinations(dest);
        }
        return result;
    }

    protected abstract String createName();

    protected void doStart() throws Exception {
        if (localURI == null) {
            throw new IllegalStateException("You must configure the 'localURI' property");
        }
        log.info("Network Connector "+getName()+" Started");
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        log.info("Network Connector "+getName()+" Stopped");
    }

    protected Transport createLocalTransport() throws Exception {
        return TransportFactory.connect(localURI);
    }

    public boolean isDispatchAsync() {
        return dispatchAsync;
    }

    public void setDispatchAsync(boolean dispatchAsync) {
        this.dispatchAsync = dispatchAsync;
    }

    public int getPrefetchSize() {
        return prefetchSize;
    }

    public void setPrefetchSize(int prefetchSize) {
        this.prefetchSize = prefetchSize;
    }

    public ConnectionFilter getConnectionFilter() {
        return connectionFilter;
    }

    public void setConnectionFilter(ConnectionFilter connectionFilter) {
        this.connectionFilter = connectionFilter;
    }

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public boolean isBridgeTempDestinations() {
		return bridgeTempDestinations;
	}

	public void setBridgeTempDestinations(boolean bridgeTempDestinations) {
		this.bridgeTempDestinations = bridgeTempDestinations;
	}
}

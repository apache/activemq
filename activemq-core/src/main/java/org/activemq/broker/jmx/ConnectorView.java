/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
package org.activemq.broker.jmx;

import org.activemq.broker.Connector;
import org.activemq.command.BrokerInfo;
import org.activemq.command.RedeliveryPolicy;

public class ConnectorView implements ConnectorViewMBean {

    private final Connector connector;

    public ConnectorView(Connector connector) {
        this.connector = connector;
    }

    public void start() throws Exception {
        connector.start();
    }

    public String getBrokerName() {
        return getBrokerInfo().getBrokerName();
    }

    public void stop() throws Exception {
        connector.stop();
    }

    public String getBrokerURL() {
        return getBrokerInfo().getBrokerURL();
    }

    public BrokerInfo getBrokerInfo() {
        return connector.getBrokerInfo();
    }

    public short getBackOffMultiplier() {
        return getRedeliveryPolicy().getBackOffMultiplier();
    }

    public long getInitialRedeliveryDelay() {
        return getRedeliveryPolicy().getInitialRedeliveryDelay();
    }

    public int getMaximumRedeliveries() {
        return getRedeliveryPolicy().getMaximumRedeliveries();
    }

    public boolean isUseExponentialBackOff() {
        return getRedeliveryPolicy().isUseExponentialBackOff();
    }

    public void setBackOffMultiplier(short backOffMultiplier) {
        getRedeliveryPolicy().setBackOffMultiplier(backOffMultiplier);
    }

    public void setInitialRedeliveryDelay(long initialRedeliveryDelay) {
        getRedeliveryPolicy().setInitialRedeliveryDelay(initialRedeliveryDelay);
    }

    public void setMaximumRedeliveries(int maximumRedeliveries) {
        getRedeliveryPolicy().setMaximumRedeliveries(maximumRedeliveries);
    }

    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        getRedeliveryPolicy().setUseExponentialBackOff(useExponentialBackOff);
    }

    public RedeliveryPolicy getRedeliveryPolicy() {
        RedeliveryPolicy redeliveryPolicy = getBrokerInfo().getRedeliveryPolicy();
        if (redeliveryPolicy == null) {
            redeliveryPolicy = new RedeliveryPolicy();
            getBrokerInfo().setRedeliveryPolicy(redeliveryPolicy);
        }
        return redeliveryPolicy;
    }

}

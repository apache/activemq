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

import org.apache.activemq.broker.Connector;
import org.apache.activemq.command.BrokerInfo;

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
    
    /**
     * Resets the statistics
     */
    public void resetStatistics() {
        connector.getStatistics().reset();
    }

    /**
     * Returns the number of messages enqueued on this connector
     * 
     * @return the number of messages enqueued on this connector
     */
    public long getEnqueueCount() {
        return connector.getStatistics().getEnqueues().getCount();
    
    }

    /**
     * Returns the number of messages dequeued on this connector
     * 
     * @return the number of messages dequeued on this connector
     */
    public long getDequeueCount() {
        return connector.getStatistics().getDequeues().getCount();
    }

}

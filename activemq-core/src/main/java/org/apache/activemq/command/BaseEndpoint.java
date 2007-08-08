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
package org.apache.activemq.command;

/**
 * A default endpoint.
 * 
 * @version $Revision$
 */
public class BaseEndpoint implements Endpoint {

    private String name;
    BrokerInfo brokerInfo;

    public BaseEndpoint(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String toString() {
        String brokerText = "";
        BrokerId brokerId = getBrokerId();
        if (brokerId != null) {
            brokerText = " broker: " + brokerId;
        }
        return "Endpoint[name:" + name + brokerText + "]";
    }

    /**
     * Returns the broker ID for this endpoint, if the endpoint is a broker or
     * null
     */
    public BrokerId getBrokerId() {
        if (brokerInfo != null) {
            return brokerInfo.getBrokerId();
        }
        return null;
    }

    /**
     * Returns the broker information for this endpoint, if the endpoint is a
     * broker or null
     */
    public BrokerInfo getBrokerInfo() {
        return brokerInfo;
    }

    public void setBrokerInfo(BrokerInfo brokerInfo) {
        this.brokerInfo = brokerInfo;
    }

}

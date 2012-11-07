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
 * Represents the logical endpoint where commands come from or are sent to.
 * 
 * For connection based transports like TCP / VM then there is a single endpoint
 * for all commands. For transports like multicast there could be different
 * endpoints being used on the same transport.
 * 
 * 
 */
public interface Endpoint {
    
    /**
     * Returns the name of the endpoint.
     */
    String getName();

    /**
     * Returns the broker ID for this endpoint, if the endpoint is a broker or
     * null
     */
    BrokerId getBrokerId();

    /**
     * Returns the broker information for this endpoint, if the endpoint is a
     * broker or null
     */
    BrokerInfo getBrokerInfo();

    void setBrokerInfo(BrokerInfo brokerInfo);

}

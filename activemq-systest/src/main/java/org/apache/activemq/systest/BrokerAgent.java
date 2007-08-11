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
package org.apache.activemq.systest;

import javax.jms.ConnectionFactory;

/**
 * 
 * @version $Revision: 1.1 $
 */
public interface BrokerAgent extends Agent {

    /**
     * Kills the given broker, if possible avoiding a clean shutdown
     * 
     * @throws Exception
     */
    void kill() throws Exception;

    /**
     * Returns the connection factory to connect to this broker
     */
    ConnectionFactory getConnectionFactory();

    /**
     * Returns the connection URI to use to connect to this broker
     */
    String getConnectionURI();

    /**
     * Sets up a network connection to the given broker
     * @throws Exception 
     */
    void connectTo(BrokerAgent broker) throws Exception;

}

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
package org.apache.activemq.transport.discovery;

import java.io.IOException;

import javax.jms.JMSException;

import org.apache.activemq.Service;
import org.apache.activemq.command.DiscoveryEvent;

/**
 * An agent used to discover other instances of a service. 
 * 
 * We typically use a discovery agent to auto-discover JMS clients and JMS brokers on a network
 *
 * @version $Revision$
 */
public interface DiscoveryAgent extends Service {

    /**
     * Sets the discovery listener
     * @param listener
     */
    void setDiscoveryListener(DiscoveryListener listener);

    /**
     * register a service
     * @param name
     * @param details
     * @throws JMSException
     */
    void registerService(String name) throws IOException;
    
    /**
     * A process actively using a service may see it go down before the DiscoveryAgent notices the
     * service's failure.  That process can use this method to notify the DiscoveryAgent of the failure
     * so that other listeners of this DiscoveryAgent can also be made aware of the failure.
     */
    void serviceFailed(DiscoveryEvent event) throws IOException;
    
}

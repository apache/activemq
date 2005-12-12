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
package org.activemq.transport.discovery;

import java.io.IOException;

import javax.jms.JMSException;

import org.activemq.Service;

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
    public void setDiscoveryListener(DiscoveryListener listener);

    /**
     * register a service
     * @param name
     * @param details
     * @throws JMSException
     */
    void registerService(String name) throws IOException;
    

    String getGroup();    
    
    void setGroup(String group);

    public void setBrokerName(String brokerName);
    
}

/** 
 * 
 * Copyright 2004 Protique Ltd
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
package org.activemq.gbean;

import java.util.Properties;

import org.activemq.broker.BrokerAdmin;
import org.activemq.broker.BrokerContainer;
import org.apache.geronimo.management.geronimo.JMSBroker;

/**
 * An interface to the ActiveMQContainerGBean for use by the 
 * ActiveMQConnectorGBean.
 *
 * @version $Revision: 1.1.1.1 $
 */
public interface ActiveMQContainer extends ActiveMQBroker {
	
	public abstract BrokerContainer getBrokerContainer();
	public abstract BrokerAdmin getBrokerAdmin();
	
	public String getBrokerName();
	public String getJaasConfiguration();
	public Properties getSecurityRoles();
	
}
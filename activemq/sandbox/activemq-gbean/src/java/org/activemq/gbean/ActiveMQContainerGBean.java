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
import javax.jms.JMSException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.geronimo.gbean.GBeanInfo;
import org.apache.geronimo.gbean.GBeanInfoBuilder;
import org.apache.geronimo.gbean.GBeanLifecycle;
import org.activemq.broker.BrokerAdmin;
import org.activemq.broker.BrokerContainer;
import org.activemq.broker.BrokerContext;
import org.activemq.broker.impl.BrokerContainerImpl;
import org.activemq.security.jassjacc.JassJaccSecurityAdapter;
import org.activemq.security.jassjacc.PropertiesConfigLoader;
import org.activemq.store.PersistenceAdapter;

/**
 * Default implementation of the ActiveMQ Message Server
 *
 * @version $Revision: 1.1.1.1 $
 */
public class ActiveMQContainerGBean implements GBeanLifecycle, ActiveMQContainer {

    private Log log = LogFactory.getLog(getClass().getName());

    private final String brokerName;

    private BrokerContext context = BrokerContext.getInstance();
    private BrokerContainer container;

    private final PersistenceAdapter persistenceAdapter;
	private final String jaasConfiguration;
	private final Properties securityRoles;

    //default constructor for use as gbean endpoint.
    public ActiveMQContainerGBean() {
        brokerName = null;
        jaasConfiguration = null;
        securityRoles = null;
        persistenceAdapter=null;
    }
	
    public ActiveMQContainerGBean(String brokerName, PersistenceAdapter persistenceAdapter,  String jaasConfiguration, Properties securityRoles) {
		
        assert brokerName != null;
		assert persistenceAdapter != null;
		
        this.brokerName = brokerName;
        this.jaasConfiguration=jaasConfiguration;
		this.persistenceAdapter = persistenceAdapter;
        this.securityRoles = securityRoles;
    }

    public synchronized BrokerContainer getBrokerContainer() {
        return container;
    }

    /**
     * @see org.activemq.gbean.ActiveMQContainer#getBrokerAdmin()
     */
    public BrokerAdmin getBrokerAdmin() {
        return container.getBroker().getBrokerAdmin();
    }

    public synchronized void doStart() throws Exception {
    	ClassLoader old = Thread.currentThread().getContextClassLoader();
    	Thread.currentThread().setContextClassLoader(ActiveMQContainerGBean.class.getClassLoader());
    	try {
	        if (container == null) {
	            container = createContainer();
	            container.start();
	        }
    	} finally {
        	Thread.currentThread().setContextClassLoader(old);
    	}
    }

    public synchronized void doStop() throws Exception {
        if (container != null) {
            BrokerContainer temp = container;
            container = null;
            temp.stop();
        }
    }

    public synchronized void doFail() {
        if (container != null) {
            BrokerContainer temp = container;
            container = null;
            try {
                temp.stop();
            }
            catch (JMSException e) {
                log.info("Caught while closing due to failure: " + e, e);
            }
        }
    }

    protected BrokerContainer createContainer() throws Exception {
    	BrokerContainerImpl answer = new BrokerContainerImpl(brokerName, context);
    	answer.setPersistenceAdapter( persistenceAdapter );
    	if( jaasConfiguration != null ) {
    		answer.setSecurityAdapter(new JassJaccSecurityAdapter(jaasConfiguration));
    	}
    	if( securityRoles != null ) {
    		// Install JACC configuration.
    		PropertiesConfigLoader loader = new PropertiesConfigLoader(brokerName, securityRoles);
    		loader.installSecurity();
    	}
    	return answer;
    }

    public static final GBeanInfo GBEAN_INFO;

    static {
        GBeanInfoBuilder infoFactory = new GBeanInfoBuilder("ActiveMQ Message Broker", ActiveMQContainerGBean.class, "JMSServer");
        infoFactory.addAttribute("brokerName", String.class, true);
        infoFactory.addReference("persistenceAdapter", PersistenceAdapter.class);
        infoFactory.addAttribute("jaasConfiguration", String.class, true);
        infoFactory.addAttribute("securityRoles", Properties.class, true);
        infoFactory.addInterface(ActiveMQContainer.class);
        infoFactory.setConstructor(new String[]{"brokerName", "persistenceAdapter", "jaasConfiguration", "securityRoles"});
        GBEAN_INFO = infoFactory.getBeanInfo();
    }

    public static GBeanInfo getGBeanInfo() {
        return GBEAN_INFO;
    }

	/**
	 * @return Returns the brokerName.
	 */
	public String getBrokerName() {
		return brokerName;
	}
	
	/**
	 * @return Returns the jassConfiguration.
	 */
	public String getJaasConfiguration() {
		return jaasConfiguration;
	}
	
	/**
	 * @return Returns the securityRoles.
	 */
	public Properties getSecurityRoles() {
		return securityRoles;
	}
	
}
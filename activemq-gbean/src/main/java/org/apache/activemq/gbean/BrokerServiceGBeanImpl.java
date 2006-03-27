/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.activemq.gbean;

import java.net.URI;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.geronimo.gbean.GBeanInfo;
import org.apache.geronimo.gbean.GBeanInfoBuilder;
import org.apache.geronimo.gbean.GBeanLifecycle;

/**
 * Default implementation of the ActiveMQ Message Server
 *
 * @version $Revision: 1.1.1.1 $
 */
public class BrokerServiceGBeanImpl implements GBeanLifecycle, BrokerServiceGBean {

    private Log log = LogFactory.getLog(getClass().getName());

    private String brokerName;
    private String brokerUri;
    private BrokerService brokerService;

    public BrokerServiceGBeanImpl() {
    }

    public synchronized BrokerService getBrokerContainer() {
        return brokerService;
    }

    public synchronized void doStart() throws Exception {
        	ClassLoader old = Thread.currentThread().getContextClassLoader();
        	Thread.currentThread().setContextClassLoader(BrokerServiceGBeanImpl.class.getClassLoader());
        	try {
    	        if (brokerService == null) {
    	            brokerService = createContainer();
    	            brokerService.start();
    	        }
        	} finally {
            	Thread.currentThread().setContextClassLoader(old);
        	}
    }

    protected BrokerService createContainer() throws Exception {
        if( brokerUri!=null ) {
            BrokerService answer = BrokerFactory.createBroker(new URI(brokerUri));
            brokerName = answer.getBrokerName();
            return answer;
        } else {
            BrokerService answer = new BrokerService();
            answer.setBrokerName(brokerName);
            return answer;
        }
    }

    public synchronized void doStop() throws Exception {
        if (brokerService != null) {
            BrokerService temp = brokerService;
            brokerService = null;
            temp.stop();
        }
    }

    public synchronized void doFail() {
        if (brokerService != null) {
            BrokerService temp = brokerService;
            brokerService = null;
            try {
                temp.stop();
            } catch (Exception e) {
                log.info("Caught while closing due to failure: " + e, e);
            }
        }
    }

    public static final GBeanInfo GBEAN_INFO;

    static {
        GBeanInfoBuilder infoFactory = new GBeanInfoBuilder("ActiveMQ Message Broker", BrokerServiceGBeanImpl.class, "JMSServer");
        infoFactory.addAttribute("brokerName", String.class, true);
        infoFactory.addAttribute("brokerUri", String.class, true);
        infoFactory.addInterface(BrokerServiceGBean.class);
        // infoFactory.setConstructor(new String[]{"brokerName, brokerUri"});
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

    public String getBrokerUri() {
        return brokerUri;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public void setBrokerUri(String brokerUri) {
        this.brokerUri = brokerUri;
    }

	
}
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
package org.apache.activemq.xbean;

import java.io.IOException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.framework.BundleException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.osgi.context.support.OsgiBundleXmlApplicationContext;

/**
 * An ActiveMQ Message Broker. It consists of a number of transport
 * connectors, network connectors and a bunch of properties which can be used to
 * configure the broker as its lazily created.
 * 
 * @org.apache.xbean.XBean element="broker" rootElement="true"
 * @org.apache.xbean.Defaults {code:xml} 
 * <broker test="foo.bar">
 *   lets.
 *   see what it includes.
 * </broker>   
 * {code}
 * @version $Revision: 1.1 $
 */
public class XBeanBrokerService extends BrokerService implements InitializingBean, DisposableBean, ApplicationContextAware {
    private static final transient Log LOG = LogFactory.getLog(XBeanBrokerService.class);
    
    private boolean start = true;
    private ApplicationContext applicationContext = null;
    private boolean destroyApplicationContextOnShutdown = false;

    public XBeanBrokerService() {
    }

    public void afterPropertiesSet() throws Exception {
        ensureSystemUsageHasStore();
        if (start) {
            start();
        }
        if (destroyApplicationContextOnShutdown) {
            addShutdownHook(new Runnable() {
                public void run() {
                    if (applicationContext instanceof ConfigurableApplicationContext) {
	                    ((ConfigurableApplicationContext) applicationContext).close();
                    }
                    if (applicationContext instanceof OsgiBundleXmlApplicationContext){
                        try {
                            ((OsgiBundleXmlApplicationContext)applicationContext).getBundle().stop();
                        } catch (BundleException e) {
                            LOG.info("Error stopping OSGi bundle " + e, e);
                        }
                    }

                }
            });
        }
    }

    private void ensureSystemUsageHasStore() throws IOException {
        SystemUsage usage = getSystemUsage();
        if (usage.getStoreUsage().getStore() == null) {
            usage.getStoreUsage().setStore(getPersistenceAdapter());
        }
        if (usage.getTempUsage().getStore() == null) {
            usage.getTempUsage().setStore(getTempDataStore());
        }
    }

    public void destroy() throws Exception {
        stop();
    }

    public boolean isStart() {
        return start;
    }

    /**
     * Sets whether or not the broker is started along with the ApplicationContext it is defined within.
     * Normally you would want the broker to start up along with the ApplicationContext but sometimes when working
     * with JUnit tests you may wish to start and stop the broker explicitly yourself.
     */
    public void setStart(boolean start) {
        this.start = start;
    }

    /**
     * Sets whether the broker should shutdown the ApplicationContext when the broker is stopped.
     * The broker can be stopped because the underlying JDBC store is unavailable for example.
     */
    public void setDestroyApplicationContextOnShutdown(boolean destroy) {
        this.destroyApplicationContextOnShutdown = destroy;
    }

	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}
    
    
}

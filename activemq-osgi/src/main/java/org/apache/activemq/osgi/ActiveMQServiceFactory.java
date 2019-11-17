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
package org.apache.activemq.osgi;

import java.util.Collections;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.SpringBrokerContext;
import org.apache.activemq.spring.Utils;
import org.apache.camel.blueprint.CamelContextFactoryBean;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;

public class ActiveMQServiceFactory implements ManagedServiceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQServiceFactory.class);

    BundleContext bundleContext;
    Map<String, BrokerService> brokers = new HashMap<>();
    Map<String, ServiceRegistration<BrokerService>> brokerRegs = new HashMap<>();

    @Override
    public String getName() {
        return "ActiveMQ Server Controller";
    }

    public Map<String, BrokerService> getBrokersMap() {
        return Collections.unmodifiableMap(brokers);
    }

    @Override
    synchronized public void updated(String pid, Dictionary<String, ?> properties) throws ConfigurationException {

        // First stop currently running broker (if any)
        deleted(pid);

        String config = (String) properties.get("config");
        if (config == null) {
            throw new ConfigurationException("config", "Property must be set");
        }
        String name = (String) properties.get("broker-name");
        if (name == null) {
            throw new ConfigurationException("broker-name", "Property must be set");
        }

        LOG.info("Starting broker " + name);

        try {
            Thread.currentThread().setContextClassLoader(BrokerService.class.getClassLoader());
            Resource resource = Utils.resourceFromString(config);

            ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
                    new String[]{resource.getURL().toExternalForm()}, false);

            if (isCamelContextFactoryBeanExist()) {

                ctx.addBeanFactoryPostProcessor(new BeanFactoryPostProcessor() {

                    @Override
                    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

                        beanFactory.addBeanPostProcessor(new BeanPostProcessor() {

                            @Override
                            public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
                                if (bean instanceof CamelContextFactoryBean) {
                                    ((CamelContextFactoryBean) bean).setBundleContext(bundleContext);
                                }
                                return bean;
                            }

                            @Override
                            public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
                                return bean;
                            }
                        });
                    }
                });
            }

            // Handle properties in configuration
            PropertyPlaceholderConfigurer configurator = new PropertyPlaceholderConfigurer();

            // convert dictionary to properties. Is there a better way?
            Properties props = new Properties();
            Enumeration<?> elements = properties.keys();
            while (elements.hasMoreElements()) {
                Object key = elements.nextElement();
                props.put(key, properties.get(key));
            }

            configurator.setProperties(props);
            configurator.setIgnoreUnresolvablePlaceholders(true);

            ctx.addBeanFactoryPostProcessor(configurator);

            ctx.refresh();

            // Start the broker
            BrokerService broker = ctx.getBean(BrokerService.class);
            if (broker == null) {
                throw new ConfigurationException(null, "Broker not defined");
            }
            // TODO deal with multiple brokers

            SpringBrokerContext brokerContext = new SpringBrokerContext();
            brokerContext.setConfigurationUrl(resource.getURL().toExternalForm());
            brokerContext.setApplicationContext(ctx);
            broker.setBrokerContext(brokerContext);

            broker.setStartAsync(true);
            broker.start();

            if (!broker.isSlave())
                broker.waitUntilStarted();
            brokers.put(pid, broker);
            brokerRegs.put(pid, bundleContext.registerService(BrokerService.class, broker, properties));
        } catch (Exception e) {
            throw new ConfigurationException(null, "Cannot start the broker", e);
        }
    }

    private boolean isCamelContextFactoryBeanExist() {
        try {
            Class.forName("org.apache.camel.osgi.CamelContextFactoryBean");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    synchronized public void deleted(String pid) {
        ServiceRegistration<BrokerService> reg = brokerRegs.remove(pid);
        if (reg != null) {
            reg.unregister();
        }
        BrokerService broker = brokers.remove(pid);
        if (broker != null) {
            stop(pid, broker);
        }
    }

	private void stop(String pid, BrokerService broker) {
		try {
            LOG.info("Stopping broker " + pid);
            broker.stop();
            broker.waitUntilStopped();
        } catch (Exception e) {
            LOG.error("Exception on stopping broker", e);
        }
	}

    synchronized public void destroy() {
        for (String broker : brokers.keySet()) {
            deleted(broker);
        }
    }

    public BundleContext getBundleContext() {
        return bundleContext;
    }

    public void setBundleContext(BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }
}

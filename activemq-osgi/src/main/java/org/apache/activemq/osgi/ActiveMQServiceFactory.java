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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.Utils;
import org.apache.xbean.spring.context.ResourceXmlApplicationContext;
import org.osgi.framework.BundleContext;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.Resource;

import java.util.*;

public class ActiveMQServiceFactory implements ManagedServiceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQServiceFactory.class);

    BundleContext bundleContext;
    HashMap<String, BrokerService> brokers = new HashMap<String, BrokerService>();

    @Override
    public String getName() {
        return "ActiveMQ Server Controller";
    }

    @Override
    synchronized public void updated(String pid, Dictionary properties) throws ConfigurationException {

        // First stop currently running broker (if any)
        deleted(pid);

        String config = (String)properties.get("config");
        if (config == null) {
            throw new ConfigurationException("config", "Property must be set");
        }
        String name = (String)properties.get("broker-name");
        if (name == null) {
            throw new ConfigurationException("broker-name", "Property must be set");
        }

        LOG.info("Starting broker " + name);

        try {
            Thread.currentThread().setContextClassLoader(BrokerService.class.getClassLoader());
            Resource resource = Utils.resourceFromString(config);

            ResourceXmlApplicationContext ctx = new ResourceXmlApplicationContext(resource, Collections.EMPTY_LIST, null, Collections.EMPTY_LIST, false) {
                protected void initBeanDefinitionReader(XmlBeanDefinitionReader reader) {
                    reader.setValidating(false);
                }
            };

            // Handle properties in configuration
            PropertySourcesPlaceholderConfigurer configurator =
                        new PropertySourcesPlaceholderConfigurer();

            //convert dictionary to properties. Is there a better way?
            Properties props = new Properties();
            Enumeration elements = properties.keys();
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
            //TODO deal with multiple brokers


            broker.start();
            broker.waitUntilStarted();
            brokers.put(pid, broker);


        } catch (Exception e) {
            throw new ConfigurationException(null, "Cannot start the broker", e);
        }
    }

    @Override
    synchronized public void deleted(String pid) {
        BrokerService broker = brokers.get(pid);
        if (broker == null) {
            return;
        }
        try {
            LOG.info("Stopping broker " + pid);
            broker.stop();
            broker.waitUntilStopped();
        } catch (Exception e) {
            LOG.error("Exception on stopping broker", e);
        }
    }

    synchronized public void destroy() {
        for (String broker: brokers.keySet()) {
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

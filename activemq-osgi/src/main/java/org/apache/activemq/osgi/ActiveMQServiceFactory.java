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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.SpringBrokerContext;
import org.apache.activemq.spring.Utils;
import org.apache.camel.blueprint.CamelContextFactoryBean;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.Resource;

@Component(name = "ActiveMQ Server Controller", configurationPid = "org.apache.activemq.server", configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ActiveMQServiceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQServiceFactory.class);

    BundleContext bundleContext;
    HashMap<String, BrokerService> brokers = new HashMap<>();
    List<ServiceTracker> trackers = new ArrayList<>();

    @Activate
    public void activate(BundleContext bctx, Map<String, ?> properties) throws Exception {
        bundleContext = bctx;
        updated(properties);
    }

    @Deactivate
    public void deactivate() throws Exception {
        trackers.forEach((tracker) -> {
            tracker.close();
        });
        destroy();
    }

    public Map<String, BrokerService> getBrokersMap() {
        return Collections.unmodifiableMap(brokers);
    }

    @SuppressWarnings("rawtypes")
    public void updated(Map<String, ?> properties) throws ConfigurationException {
        String pid = (String) properties.get("service.pid");

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

        Set<String> serviceNames = Collections.emptySet();
        String services = (String) properties.get("services");
        if (services != null) {
            serviceNames = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(services.split(";"))));
        }
        Map<String, Object> serviceObjects = new HashMap<>();

        BrokerCallback callback = new BrokerCallback() {
            @Override
            public synchronized void addService(String serviceName, Object serviceObject) {
                LOG.info("Add service dependency " + serviceName + " for "+ name);
                serviceObjects.put(serviceName, serviceObject);
            }

            @Override
            public synchronized void removeService(String serviceName) {
                LOG.info("Remove service dependency " + serviceName + " for "+ name);
                serviceObjects.remove(serviceName);
            }

            @Override
            public synchronized void startBroker() throws ConfigurationException {
                LOG.info("Starting broker " + name);

                try {
                    Thread.currentThread().setContextClassLoader(BrokerService.class.getClassLoader());
                    Resource resource = Utils.resourceFromString(config);

                    ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(new GenericApplicationContext());

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

                    Properties props = new Properties();
                    props.putAll(properties);

                    configurator.setProperties(props);
                    configurator.setIgnoreUnresolvablePlaceholders(true);

                    ctx.addBeanFactoryPostProcessor(configurator);
 
                    GenericApplicationContext parentCtx = GenericApplicationContext.class.cast(ctx.getParent());
                    ConfigurableListableBeanFactory beanFactory = parentCtx.getBeanFactory();
                    serviceObjects.entrySet().forEach((entry) -> {
                        beanFactory.registerSingleton(entry.getKey(), entry.getValue());
                    });
                    ctx.setConfigLocation(resource.getURL().toExternalForm());

                    parentCtx.refresh();
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
                } catch (Exception e) {
                    throw new ConfigurationException(null, "Cannot start the broker", e);
                }
            }

            @Override
            public synchronized void stopBroker() {
                deleted(pid);
            }
        };

        if (serviceNames.isEmpty()) {
            callback.startBroker();
        } else {
            LOG.info("Waiting for dependencies");

            DependencyTrackerCustomizer dtc = new DependencyTrackerCustomizer(bundleContext, serviceNames, callback);
            for (String key : serviceNames) {
                try {
                    String filter = "(osgi.jndi.service.name=" + key + ")";
                    ServiceTracker tracker = new ServiceTracker(bundleContext, bundleContext.createFilter(filter), dtc);
                    tracker.open();
                    trackers.add(tracker);
                } catch (InvalidSyntaxException e) {
                    throw new ConfigurationException(null, "Cannot start the broker", e);
                }
            }
>>>>>>> AMQ-6805: Add new implementation based on OSGi Declarative Services to make OSGi services available for broker configuration
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

    public void deleted(String pid) {
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

    public void destroy() {
        brokers.keySet().forEach((broker) -> {
            deleted(broker);
        });
    }
}

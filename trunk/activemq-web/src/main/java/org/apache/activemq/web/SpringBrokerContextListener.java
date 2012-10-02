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
package org.apache.activemq.web;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.Resource;
import org.springframework.web.context.support.ServletContextResource;

/**
 * Used to configure and instance of ActiveMQ <tt>BrokerService</tt> using
 * ActiveMQ/Spring's xml configuration. <p/> The configuration file is specified
 * via the context init parameter <tt>brokerURI</tt>, typically: <code>
 * &lt;context-param&gt;
 * &lt;param-name&gt;brokerURI&lt;/param-name&gt;
 * &lt;param-value&gt;/WEB-INF/activemq.xml&lt;/param-value&gt;
 * &lt;/context-param&gt;
 * </code>
 * As a a default, if a <tt>brokerURI</tt> is not specified it will look up
 * for <tt>activemq.xml</tt>
 * 
 * 
 */
public class SpringBrokerContextListener implements ServletContextListener {

    /** broker uri context parameter name: <tt>brokerURI</tt> */
    public static final String INIT_PARAM_BROKER_URI = "brokerURI";

    /** the broker container instance */
    private BrokerService brokerContainer;

    /**
     * Set the broker container to be used by this listener
     * 
     * @param container the container to be used.
     */
    protected void setBrokerService(BrokerService container) {
        this.brokerContainer = container;
    }

    /**
     * Return the broker container.
     */
    protected BrokerService getBrokerService() {
        return this.brokerContainer;
    }

    public void contextInitialized(ServletContextEvent event) {
        ServletContext context = event.getServletContext();
        context.log("Creating ActiveMQ Broker...");
        brokerContainer = createBroker(context);

        context.log("Starting ActiveMQ Broker");
        try {
            brokerContainer.start();

            context.log("Started ActiveMQ Broker");
        } catch (Exception e) {
            context.log("Failed to start ActiveMQ broker: " + e, e);
        }
    }

    public void contextDestroyed(ServletContextEvent event) {
        ServletContext context = event.getServletContext();
        if (brokerContainer != null) {
            try {
                brokerContainer.stop();
            } catch (Exception e) {
                context.log("Failed to stop the ActiveMQ Broker: " + e, e);
            }
            brokerContainer = null;
        }
    }

    /**
     * Factory method to create a new ActiveMQ Broker
     */
    protected BrokerService createBroker(ServletContext context) {
        String brokerURI = context.getInitParameter(INIT_PARAM_BROKER_URI);
        if (brokerURI == null) {
            brokerURI = "activemq.xml";
        }
        context.log("Loading ActiveMQ Broker configuration from: " + brokerURI);
        Resource resource = new ServletContextResource(context, brokerURI);
        BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        try {
            factory.afterPropertiesSet();
        } catch (Exception e) {
            context.log("Failed to create broker: " + e, e);
        }
        return factory.getBroker();
    }
}

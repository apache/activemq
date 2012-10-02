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

import java.beans.PropertyEditorManager;
import java.net.URI;

import org.apache.activemq.broker.BrokerService;
import org.apache.xbean.spring.context.ResourceXmlApplicationContext;
import org.apache.xbean.spring.context.impl.URIEditor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.Resource;

/**
 * A Spring {@link FactoryBean} which creates an embedded broker inside a Spring
 * XML using an external <a href="http://gbean.org/Custom+XML">XBean Spring XML
 * configuration file</a> which provides a much neater and more concise XML
 * format.
 * 
 * 
 */
public class BrokerFactoryBean implements FactoryBean, InitializingBean, DisposableBean, ApplicationContextAware {

    static {
        PropertyEditorManager.registerEditor(URI.class, URIEditor.class);
    }

    private Resource config;
    private XBeanBrokerService broker;
    private boolean start;
    private ResourceXmlApplicationContext context;
    private ApplicationContext parentContext;
    
    private boolean systemExitOnShutdown;
    private int systemExitOnShutdownExitCode;

    public BrokerFactoryBean() {
    }

    public BrokerFactoryBean(Resource config) {
        this.config = config;
    }

    public Object getObject() throws Exception {
        return broker;
    }

    public Class getObjectType() {
        return BrokerService.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void setApplicationContext(ApplicationContext parentContext) throws BeansException {
        this.parentContext = parentContext;
    }

    public void afterPropertiesSet() throws Exception {
        if (config == null) {
            throw new IllegalArgumentException("config property must be set");
        }
        context = new ResourceXmlApplicationContext(config, parentContext);

        try {
            broker = (XBeanBrokerService)context.getBean("broker");
        } catch (BeansException e) {
            // ignore...
            // log.trace("No bean named broker available: " + e, e);
        }
        if (broker == null) {
            // lets try find by type
            String[] names = context.getBeanNamesForType(BrokerService.class);
            for (int i = 0; i < names.length; i++) {
                String name = names[i];
                broker = (XBeanBrokerService)context.getBean(name);
                if (broker != null) {
                    break;
                }
            }
        }
        if (broker == null) {
            throw new IllegalArgumentException("The configuration has no BrokerService instance for resource: " + config);
        }
        
        if( systemExitOnShutdown ) {
            broker.addShutdownHook(new Runnable(){
                public void run() {
                    System.exit(systemExitOnShutdownExitCode);
                }
            });
        }
        if (start) {
            broker.start();
        }
    }

    public void destroy() throws Exception {
        if (context != null) {
            context.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    public Resource getConfig() {
        return config;
    }

    public void setConfig(Resource config) {
        this.config = config;
    }

    public BrokerService getBroker() {
        return broker;
    }

    public boolean isStart() {
        return start;
    }

    public void setStart(boolean start) {
        this.start = start;
    }

    public boolean isSystemExitOnStop() {
        return systemExitOnShutdown;
    }

    public void setSystemExitOnStop(boolean systemExitOnStop) {
        this.systemExitOnShutdown = systemExitOnStop;
    }

    public boolean isSystemExitOnShutdown() {
        return systemExitOnShutdown;
    }

    public void setSystemExitOnShutdown(boolean systemExitOnShutdown) {
        this.systemExitOnShutdown = systemExitOnShutdown;
    }

    public int getSystemExitOnShutdownExitCode() {
        return systemExitOnShutdownExitCode;
    }

    public void setSystemExitOnShutdownExitCode(int systemExitOnShutdownExitCode) {
        this.systemExitOnShutdownExitCode = systemExitOnShutdownExitCode;
    }

}

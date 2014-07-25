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
import java.net.MalformedURLException;
import java.net.URI;

import org.apache.activemq.broker.BrokerContextAware;
import org.apache.activemq.broker.BrokerFactoryHandler;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.SpringBrokerContext;
import org.apache.activemq.spring.Utils;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.xbean.spring.context.ResourceXmlApplicationContext;
import org.apache.xbean.spring.context.impl.URIEditor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.Resource;

/**
 * 
 */
public class XBeanBrokerFactory implements BrokerFactoryHandler {
    private static final transient Logger LOG = LoggerFactory.getLogger(XBeanBrokerFactory.class);

    static {
        PropertyEditorManager.registerEditor(URI.class, URIEditor.class);
    }

    private boolean validate = true;
    public boolean isValidate() {
        return validate;
    }

    public void setValidate(boolean validate) {
        this.validate = validate;
    }

    public BrokerService createBroker(URI config) throws Exception {
        String uri = config.getSchemeSpecificPart();
        if (uri.lastIndexOf('?') != -1) {
            IntrospectionSupport.setProperties(this, URISupport.parseQuery(uri));
            uri = uri.substring(0, uri.lastIndexOf('?'));
        }

        ApplicationContext context = createApplicationContext(uri);

        BrokerService broker = null;
        try {
            broker = (BrokerService)context.getBean("broker");
        } catch (BeansException e) {
        }

        if (broker == null) {
            // lets try find by type
            String[] names = context.getBeanNamesForType(BrokerService.class);
            for (int i = 0; i < names.length; i++) {
                String name = names[i];
                broker = (BrokerService)context.getBean(name);
                if (broker != null) {
                    break;
                }
            }
        }
        if (broker == null) {
            throw new IllegalArgumentException("The configuration has no BrokerService instance for resource: " + config);
        }
        
        SpringBrokerContext springBrokerContext = new SpringBrokerContext();
        springBrokerContext.setApplicationContext(context);
        springBrokerContext.setConfigurationUrl(uri);
        broker.setBrokerContext(springBrokerContext);

        // TODO warning resources from the context may not be closed down!

        return broker;
    }

    protected ApplicationContext createApplicationContext(String uri) throws MalformedURLException {
        Resource resource = Utils.resourceFromString(uri);
        LOG.debug("Using " + resource + " from " + uri);
        try {
            return new ResourceXmlApplicationContext(resource) {
                @Override
                protected void initBeanDefinitionReader(XmlBeanDefinitionReader reader) {
                    reader.setValidating(isValidate());
                }
            };
        } catch (FatalBeanException errorToLog) {
            LOG.error("Failed to load: " + resource + ", reason: " + errorToLog.getLocalizedMessage(), errorToLog);
            throw errorToLog;
        }
    }

}

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
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Map;

import org.apache.activemq.broker.BrokerFactoryHandler;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.xbean.spring.context.ResourceXmlApplicationContext;
import org.apache.xbean.spring.context.impl.URIEditor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

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
        
        Map map = URISupport.parseParameters(config);
        if (!map.isEmpty()) {
            IntrospectionSupport.setProperties(this, map);
            config = URISupport.removeQuery(config);
        }

        String uri = config.getSchemeSpecificPart();
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
        
        if (broker instanceof ApplicationContextAware) {
        	((ApplicationContextAware)broker).setApplicationContext(context);
        }
        
        // TODO warning resources from the context may not be closed down!

        return broker;
    }

    protected ApplicationContext createApplicationContext(String uri) throws MalformedURLException {
        LOG.debug("Now attempting to figure out the type of resource: " + uri);

        Resource resource;
        File file = new File(uri);
        if (file.exists()) {
            resource = new FileSystemResource(uri);
        } else if (ResourceUtils.isUrl(uri)) {
            resource = new UrlResource(uri);
        } else {
            resource = new ClassPathResource(uri);
        }
        return new ResourceXmlApplicationContext(resource) {
            @Override
            protected void initBeanDefinitionReader(XmlBeanDefinitionReader reader) {
                reader.setValidating(isValidate());
            }
        };
    }
}

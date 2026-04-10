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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.activemq.broker.BrokerContextAware;
import org.apache.activemq.broker.BrokerFactoryHandler;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.SpringBrokerContext;
import org.apache.activemq.spring.Utils;
import org.apache.activemq.transport.stomp.FrameTranslator;
import org.apache.activemq.transport.stomp.JmsFrameTranslator;
import org.apache.activemq.transport.stomp.LegacyFrameTranslator;
import org.apache.activemq.util.FactoryFinder;
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
    private static final Logger LOG = LoggerFactory.getLogger(XBeanBrokerFactory.class);

    public static final String XBEAN_BROKER_FACTORY_PROTOCOLS_PROP =
            "org.apache.activemq.xbean.XBEAN_BROKER_FACTORY_PROTOCOLS";
    public static final String DEFAULT_ALLOWED_PROTOCOLS =
            String.join(",", Set.of(Utils.FILE_PROTOCOL, Utils.CLASSPATH_PROTOCOL));

    private final Set<String> allowedProtocols;

    static {
        PropertyEditorManager.registerEditor(URI.class, URIEditor.class);
    }

    public XBeanBrokerFactory() {
        final String allowedProtocols = System.getProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP,
                DEFAULT_ALLOWED_PROTOCOLS);

        // Asterisk will map to null which will allow all and skip checking
        // Empty string will map to an empty set and will deny all
        this.allowedProtocols = !allowedProtocols.equals("*") ?
                Arrays.stream(allowedProtocols.split("\\s*,\\s*"))
                .filter(s -> !s.isBlank())
                .collect(Collectors.toUnmodifiableSet()) : null;
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
            for (String name : names) {
                // No need to check for null, this will throw an exception if not found
                broker = (BrokerService) context.getBean(name);
                break;
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
        Resource resource = Utils.resourceFromString(uri, allowedProtocols);
        LOG.debug("Using {} from {}", resource, uri);
        try {
            return new ResourceXmlApplicationContext(resource) {
                @Override
                protected void initBeanDefinitionReader(XmlBeanDefinitionReader reader) {
                    reader.setValidating(isValidate());
                }
            };
        } catch (FatalBeanException errorToLog) {
            LOG.error("Failed to load: {}, reason: {}", resource, errorToLog.getLocalizedMessage(),
                    errorToLog);
            throw errorToLog;
        }
    }

    // Package scope for testing
    Set<String> getAllowedProtocols() {
        return allowedProtocols;
    }
}

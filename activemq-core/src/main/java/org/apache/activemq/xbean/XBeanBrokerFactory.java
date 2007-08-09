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

import org.apache.activemq.broker.BrokerFactoryHandler;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.xbean.spring.context.ResourceXmlApplicationContext;
import org.apache.xbean.spring.context.impl.URIEditor;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

/**
 * @version $Revision$
 */
public class XBeanBrokerFactory implements BrokerFactoryHandler {
    private static final transient Log log = LogFactory.getLog(XBeanBrokerFactory.class);

    static {
        PropertyEditorManager.registerEditor(URI.class, URIEditor.class);
    }

    public BrokerService createBroker(URI config) throws Exception {

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

        // TODO warning resources from the context may not be closed down!

        return broker;
    }

    protected ApplicationContext createApplicationContext(String uri) throws MalformedURLException {
        log.debug("Now attempting to figure out the type of resource: " + uri);

        Resource resource;
        File file = new File(uri);
        if (file.exists()) {
            resource = new FileSystemResource(uri);
        } else if (ResourceUtils.isUrl(uri)) {
            resource = new UrlResource(uri);
        } else {
            resource = new ClassPathResource(uri);
        }
        return new ResourceXmlApplicationContext(resource);
    }
}

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
package org.apache.activemq.plugin;

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.spring.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertiesPlaceHolderUtil {

    public static final Logger LOG = LoggerFactory.getLogger(PropertiesPlaceHolderUtil.class);

    static final Pattern pattern = Pattern.compile("\\$\\{([^\\}]+)\\}");
    final Properties properties;

    public PropertiesPlaceHolderUtil(Properties properties) {
        this.properties = properties;
    }

    public void filter(Properties toFilter) {
        for (Map.Entry<Object, Object> entry : toFilter.entrySet()) {
            String val = (String) entry.getValue();
            String newVal = filter(val);
            if (!val.equals(newVal)) {
                toFilter.put(entry.getKey(), newVal);
            }
        }
    }

    public String filter(String str) {
        int start = 0;
        while (true) {
            Matcher matcher = pattern.matcher(str);
            if (!matcher.find(start)) {
                break;
            }
            String group = matcher.group(1);
            String property = properties.getProperty(group);
            if (property != null) {
                str = matcher.replaceFirst(Matcher.quoteReplacement(property));
            } else {
                start = matcher.end();
            }
        }
        return replaceBytePostfix(str);
    }

    static Pattern[] byteMatchers = new Pattern[] {
            Pattern.compile("^\\s*(\\d+)\\s*(b)?\\s*$", Pattern.CASE_INSENSITIVE),
            Pattern.compile("^\\s*(\\d+)\\s*k(b)?\\s*$", Pattern.CASE_INSENSITIVE),
            Pattern.compile("^\\s*(\\d+)\\s*m(b)?\\s*$", Pattern.CASE_INSENSITIVE),
            Pattern.compile("^\\s*(\\d+)\\s*g(b)?\\s*$", Pattern.CASE_INSENSITIVE)};

    // xbean can Xb, Xkb, Xmb, Xg etc
    private String replaceBytePostfix(String str) {
        try {
            for (int i=0; i< byteMatchers.length; i++) {
                Matcher matcher = byteMatchers[i].matcher(str);
                if (matcher.matches()) {
                    long value = Long.parseLong(matcher.group(1));
                    for (int j=1; j<=i; j++) {
                        value *= 1024;
                    }
                    return String.valueOf(value);
                }
            }
        } catch (NumberFormatException ignored) {
            LOG.debug("nfe on: " + str, ignored);
        }
        return str;
    }

    public void mergeProperties(Document doc, Properties initialProperties, BrokerContext brokerContext) {
        // find resources
        //        <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">
        //            <property name="locations" || name="properties">
        //              ...
        //            </property>
        //          </bean>
        LinkedList<String> resources = new LinkedList<String>();
        LinkedList<String> propertiesClazzes = new LinkedList<String>();
        NodeList beans = doc.getElementsByTagNameNS("*", "bean");
        for (int i = 0; i < beans.getLength(); i++) {
            Node bean = beans.item(0);
            if (bean.hasAttributes() && bean.getAttributes().getNamedItem("class").getTextContent().contains("PropertyPlaceholderConfigurer")) {
                if (bean.hasChildNodes()) {
                    NodeList beanProps = bean.getChildNodes();
                    for (int j = 0; j < beanProps.getLength(); j++) {
                        Node beanProp = beanProps.item(j);
                        if (Node.ELEMENT_NODE == beanProp.getNodeType() && beanProp.hasAttributes() && beanProp.getAttributes().getNamedItem("name") != null) {
                            String propertyName = beanProp.getAttributes().getNamedItem("name").getTextContent();
                            if ("locations".equals(propertyName)) {

                                // interested in value or list/value of locations property
                                Element beanPropElement = (Element) beanProp;
                                NodeList values = beanPropElement.getElementsByTagNameNS("*", "value");
                                for (int k = 0; k < values.getLength(); k++) {
                                    Node value = values.item(k);
                                    resources.add(value.getFirstChild().getTextContent());
                                }
                            } else if ("properties".equals(propertyName)) {

                                // bean or beanFactory
                                Element beanPropElement = (Element) beanProp;
                                NodeList values = beanPropElement.getElementsByTagNameNS("*", "bean");
                                for (int k = 0; k < values.getLength(); k++) {
                                    Node value = values.item(k);
                                    if (value.hasAttributes()) {
                                        Node beanClassTypeNode = value.getAttributes().getNamedItem("class");
                                        if (beanClassTypeNode != null) {
                                            propertiesClazzes.add(beanClassTypeNode.getFirstChild().getTextContent());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        for (String value : propertiesClazzes) {
            try {
                Object springBean = getClass().getClassLoader().loadClass(value).newInstance();
                if (springBean instanceof FactoryBean) {
                    // can't access the factory or created properties from spring context so we got to recreate
                    initialProperties.putAll((Properties) FactoryBean.class.getMethod("getObject", (Class<?>[]) null).invoke(springBean));
                }
            } catch (Throwable e) {
                LOG.debug("unexpected exception processing properties bean class: " + propertiesClazzes, e);
            }
        }
        List<Resource> propResources = new LinkedList<Resource>();
        for (String value : resources) {
            try {
                if (!value.isEmpty()) {
                    propResources.add(Utils.resourceFromString(filter(value)));
                }
            } catch (MalformedURLException e) {
                LOG.info("failed to resolve resource: " + value, e);
            }
        }
        for (Resource resource : propResources) {
            Properties properties = new Properties();
            try {
                properties.load(resource.getInputStream());
            } catch (IOException e) {
                LOG.info("failed to load properties resource: " + resource, e);
            }
            initialProperties.putAll(properties);
        }
    }

}

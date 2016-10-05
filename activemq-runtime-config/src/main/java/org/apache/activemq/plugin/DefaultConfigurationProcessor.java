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

import org.apache.activemq.util.IntrospectionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBElement;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.activemq.schema.core.DtoBroker;

public class DefaultConfigurationProcessor implements ConfigurationProcessor {

    public static final Logger LOG = LoggerFactory.getLogger(DefaultConfigurationProcessor.class);
    RuntimeConfigurationBroker plugin;
    Class configurationClass;

    Pattern matchPassword = Pattern.compile("password=.*,");

    public DefaultConfigurationProcessor(RuntimeConfigurationBroker plugin, Class configurationClass) {
        this.plugin = plugin;
        this.configurationClass = configurationClass;
    }

    @Override
    public void processChanges(DtoBroker currentConfiguration, DtoBroker modifiedConfiguration) {
        List current = filter(currentConfiguration, configurationClass);
        List modified = filter(modifiedConfiguration, configurationClass);

        if (current.equals(modified)) {
            plugin.debug("no changes to " + configurationClass.getSimpleName());
            return;
        } else {
            plugin.info("changes to " + configurationClass.getSimpleName());
        }

        processChanges(current, modified);
    }

    public void processChanges(List current, List modified) {
        int modIndex = 0, currentIndex = 0;
        for (; modIndex < modified.size() && currentIndex < current.size(); modIndex++, currentIndex++) {
            // walk the list for mods
            applyModifications(getContents(current.get(currentIndex)),
                    getContents(modified.get(modIndex)));
        }

        for (; modIndex < modified.size(); modIndex++) {
            // new element; add all
            for (Object nc : getContents(modified.get(modIndex))) {
                ConfigurationProcessor processor = findProcessor(nc);
                if (processor != null) {
                    processor.addNew(nc);
                } else {
                    addNew(nc);
                }
            }
        }

        for (; currentIndex < current.size(); currentIndex++) {
            // removal of element; remove all
            for (Object nc : getContents(current.get(currentIndex))) {
                ConfigurationProcessor processor = findProcessor(nc);
                if (processor != null) {
                    processor.remove(nc);
                } else {
                    remove(nc);
                }
            }
        }
    }

    protected void applyModifications(List<Object> current, List<Object> modification) {
        int modIndex = 0, currentIndex = 0;
        for (; modIndex < modification.size() && currentIndex < current.size(); modIndex++, currentIndex++) {
            Object existing = current.get(currentIndex);
            Object candidate = modification.get(modIndex);
            if (!existing.equals(candidate)) {
                plugin.debug("modification to:" + existing + " , with: " + candidate);
                ConfigurationProcessor processor = findProcessor(existing);
                if (processor != null) {
                    processor.modify(existing, candidate);
                } else {
                    modify(existing, candidate);
                }
            }
        }
        for (; modIndex < modification.size(); modIndex++) {
            Object mod = modification.get(modIndex);
            ConfigurationProcessor processor = findProcessor(mod);
            if (processor != null) {
                processor.addNew(mod);
            } else {
                addNew(mod);
            }
        }
        for (; currentIndex < current.size(); currentIndex++) {
            Object mod = current.get(currentIndex);
            ConfigurationProcessor processor = findProcessor(mod);
            if (processor != null) {
                processor.remove(mod);
            } else {
                remove(mod);
            }
        }
    }

    public void modify(Object existing, Object candidate) {
        remove(existing);
        addNew(candidate);
    }

    public void addNew(Object o) {
        plugin.info("No runtime support for additions of " + o);
    }

    public void remove(Object o) {
        plugin.info("No runtime support for removal of: " + o);
    }

    @Override
    public ConfigurationProcessor findProcessor(Object o) {
        plugin.info("No processor for " + o);
        return null;
    }

    // mapping all supported updatable elements to support getContents
    protected List<Object> getContents(Object o) {
        List<Object> answer = new ArrayList<Object>();
        try {
            Object val = o.getClass().getMethod("getContents", new Class[]{}).invoke(o, new Object[]{});
            if (val instanceof List) {
                answer = (List<Object>) val;
            } else {
                answer.add(val);
            }
        } catch (NoSuchMethodException mappingIncomplete) {
            plugin.debug(filterPasswords(o) + " has no modifiable elements");
        } catch (Exception e) {
            plugin.info("Failed to access getContents for " + o + ", runtime modifications not supported", e);
        }
        return answer;
    }

    protected String filterPasswords(Object toEscape) {
        return matchPassword.matcher(toEscape.toString()).replaceAll("password=???,");
    }

    protected <T> List<Object> filter(Object obj, Class<T> type) {
        return filter(getContents(obj), type);
    }

    protected <T> List<Object> filter(List<Object> objectList, Class<T> type) {
        List<Object> result = new LinkedList<Object>();
        for (Object o : objectList) {
            if (o instanceof JAXBElement) {
                JAXBElement element = (JAXBElement) o;
                if (type.isAssignableFrom(element.getDeclaredType())) {
                    result.add((T) element.getValue());
                }
            } else if (type.isAssignableFrom(o.getClass())) {
                result.add((T) o);
            }
        }
        return result;
    }

    protected <T> T fromDto(Object dto, T instance) {
        Properties properties = new Properties();
        IntrospectionSupport.getProperties(dto, properties, null);
        plugin.placeHolderUtil.filter(properties);
        LOG.trace("applying props: " + filterPasswords(properties) + ", to " + instance.getClass().getSimpleName());
        IntrospectionSupport.setProperties(instance, properties);

        // deal with nested elements
        for (Object nested : filter(dto, Object.class)) {
            String elementName = nested.getClass().getSimpleName();
            Method setter = JAXBUtils.findSetter(instance, elementName);
            if (setter != null) {
                List<Object> argument = new LinkedList<Object>();
                for (Object elementContent : filter(nested, Object.class)) {
                    argument.add(fromDto(elementContent, JAXBUtils.inferTargetObject(elementContent)));
                }
                try {
                    setter.invoke(instance, JAXBUtils.matchType(argument, setter.getParameterTypes()[0]));
                } catch (Exception e) {
                    plugin.info("failed to invoke " + setter + " on " + instance, e);
                }
            } else {
                plugin.info("failed to find setter for " + elementName + " on :" + instance);
            }
        }
        return instance;
    }
}

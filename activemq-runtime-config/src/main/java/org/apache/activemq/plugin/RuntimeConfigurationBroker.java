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

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.schema.core.Broker;
import org.apache.activemq.schema.core.NetworkConnector;
import org.apache.activemq.spring.Utils;
import org.apache.activemq.util.IntrospectionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class RuntimeConfigurationBroker extends BrokerFilter {

    public static final Logger LOG = LoggerFactory.getLogger(RuntimeConfigurationBroker.class);
    private long checkPeriod;
    private long lastModified = -1;
    private Resource configToMonitor;
    private Broker currentConfiguration;
    private Runnable monitorTask;

    public RuntimeConfigurationBroker(org.apache.activemq.broker.Broker next) {
        super(next);
    }

    @Override
    public void start() throws Exception {
        super.start();
        try {
            configToMonitor = Utils.resourceFromString(next.getBrokerService().getConfigurationUrl());
        } catch (Exception error) {
            LOG.error("failed to determine configuration url resource from broker, updates cannot be tracked", error);
        }

        currentConfiguration = loadConfiguration(configToMonitor);
        monitorModification(configToMonitor);
    }

    @Override
    public void stop() throws Exception {
        if (monitorTask != null) {
            try {
                this.getBrokerService().getScheduler().cancel(monitorTask);
            } catch (Exception letsNotStopStop) {
                LOG.warn("Failed to cancel config monitor task", letsNotStopStop);
            }
        }
        super.stop();
    }

    private void monitorModification(final Resource configToMonitor) {
        Runnable monitorTask = new Runnable() {
            @Override
            public void run() {
                try {
                    if (configToMonitor.lastModified() > lastModified) {
                        applyModifications(configToMonitor);
                    }
                } catch (IOException e) {
                    LOG.error("Failed to determine lastModified time on configuration: " + configToMonitor, e);
                }
            }
        };
        if (lastModified > 0) {
            this.getBrokerService().getScheduler().executePeriodically(monitorTask, checkPeriod);
            LOG.info("Monitoring for updates (every " + checkPeriod + "millis) : " + configToMonitor);
        }
    }

    private void applyModifications(Resource configToMonitor) {
        Broker changed = loadConfiguration(configToMonitor);
        if (!currentConfiguration.equals(changed)) {
            LOG.info("configuration change in " + configToMonitor + " at: " + new Date(lastModified));
            LOG.info("current:" + currentConfiguration);
            LOG.info("new    :" + changed);
            processNetworkConnectors(currentConfiguration, changed);
            currentConfiguration = changed;
        } else {
            LOG.info("file modification but no material change to configuration in " + configToMonitor + " at: " + new Date(lastModified));
        }
    }

    private void processNetworkConnectors(Broker currentConfiguration, Broker modifiedConfiguration) {
        List<Broker.NetworkConnectors> currentNc = filterElement(
                currentConfiguration.getContents(), Broker.NetworkConnectors.class);
        List<Broker.NetworkConnectors> modNc = filterElement(
                modifiedConfiguration.getContents(), Broker.NetworkConnectors.class);

        int modIndex = 0, currentIndex = 0;
        for (; modIndex < modNc.size() && currentIndex < currentNc.size(); modIndex++, currentIndex++) {
            if (!modNc.get(modIndex).getContents().get(0).equals(
                    currentNc.get(currentIndex).getContents().get(0))) {
                // change in order will fool this logic
                LOG.error("not supported: mod to existing network Connector, new: "
                        + modNc.get(modIndex).getContents().get(0));
            }
        }

        for (; modIndex < modNc.size(); modIndex++) {
            // additions
            addNetworkConnector(modNc.get(modIndex).getContents().get(0));
        }
    }

    private void addNetworkConnector(Object o) {
        if (o instanceof NetworkConnector) {
            NetworkConnector networkConnector = (NetworkConnector) o;
            if (networkConnector.getUri() != null) {
                try {
                    org.apache.activemq.network.NetworkConnector nc =
                            getBrokerService().addNetworkConnector(networkConnector.getUri());
                    Properties properties = new Properties();
                    IntrospectionSupport.getProperties(networkConnector, properties, null);
                    properties.remove("uri");
                    LOG.trace("Applying props: " + properties);
                    IntrospectionSupport.setProperties(nc, properties);
                    nc.start();
                    LOG.info("started new network connector: " + nc);
                } catch (Exception e) {
                    LOG.error("Failed to add new networkConnector " + networkConnector, e);
                }
            }
        } else {
            LOG.info("No runtime support for modifications to " + o);
        }
    }

    private <T> List<T> filterElement(List<Object> objectList, Class<T> type) {
        List<T> result = new LinkedList<T>();
        for (Object o : objectList) {
            if (o instanceof JAXBElement) {
                JAXBElement element = (JAXBElement) o;
                if (element.getDeclaredType() == type) {
                    result.add((T) element.getValue());
                }
            }
        }
        return result;
    }

    private Broker loadConfiguration(Resource configToMonitor) {
        Broker jaxbConfig = null;
        if (configToMonitor != null) {
            try {
                JAXBContext context = JAXBContext.newInstance(Broker.class);
                Unmarshaller unMarshaller = context.createUnmarshaller();

                // skip beans and pull out the broker node to validate
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                dbf.setNamespaceAware(true);
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.parse(configToMonitor.getInputStream());
                Node brokerRootNode = doc.getElementsByTagName("broker").item(0);

                JAXBElement<Broker> brokerJAXBElement =
                        unMarshaller.unmarshal(brokerRootNode, Broker.class);
                jaxbConfig = brokerJAXBElement.getValue();

                // if we can parse we can track mods
                lastModified = configToMonitor.lastModified();

            } catch (IOException e) {
                LOG.error("Failed to access: " + configToMonitor, e);
            } catch (JAXBException e) {
                LOG.error("Failed to parse: " + configToMonitor, e);
            } catch (ParserConfigurationException e) {
                LOG.error("Failed to document parse: " + configToMonitor, e);
            } catch (SAXException e) {
                LOG.error("Failed to find broker element in: " + configToMonitor, e);
            }
        }
        return jaxbConfig;
    }

    public void setCheckPeriod(long checkPeriod) {
        this.checkPeriod = checkPeriod;
    }
}
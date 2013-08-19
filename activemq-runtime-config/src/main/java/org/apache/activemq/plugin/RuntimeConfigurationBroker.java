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
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
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
        List<Broker.NetworkConnectors> currentNCsElems = filterElement(
                currentConfiguration.getContents(), Broker.NetworkConnectors.class);
        List<Broker.NetworkConnectors> modifiedNCsElems = filterElement(
                modifiedConfiguration.getContents(), Broker.NetworkConnectors.class);

        int modIndex = 0, currentIndex = 0;
        for (; modIndex < modifiedNCsElems.size() && currentIndex < currentNCsElems.size(); modIndex++, currentIndex++) {
            // walk the list of individual nc's...
            applyModifications(currentNCsElems.get(currentIndex).getContents(),
                    modifiedNCsElems.get(modIndex).getContents());
        }

        for (; modIndex < modifiedNCsElems.size(); modIndex++) {
            // new networkConnectors element; add all
            for (Object nc : modifiedNCsElems.get(modIndex).getContents()) {
                addNetworkConnector(nc);
            }
        }

        for (; currentIndex < currentNCsElems.size(); currentIndex++) {
            // removal of networkConnectors element; remove all
            for (Object nc : modifiedNCsElems.get(modIndex).getContents()) {
                removeNetworkConnector(nc);
            }
        }
    }

    private void applyModifications(List<Object> current, List<Object> modification) {
        int modIndex = 0, currentIndex = 0;
        for (; modIndex < modification.size() && currentIndex < current.size(); modIndex++, currentIndex++) {
            Object currentNc = current.get(currentIndex);
            Object candidateNc = modification.get(modIndex);
            if (! currentNc.equals(candidateNc)) {
                LOG.info("modification to:" + currentNc + " , with: " + candidateNc);
                removeNetworkConnector(currentNc);
                addNetworkConnector(candidateNc);
            }
        }

        for (; modIndex < modification.size(); modIndex++) {
            addNetworkConnector(modification.get(modIndex));
        }

        for (; currentIndex < current.size(); currentIndex++) {
            removeNetworkConnector(current.get(currentIndex));
        }
    }

    private void removeNetworkConnector(Object o) {
        if (o instanceof NetworkConnector) {
            NetworkConnector toRemove = (NetworkConnector) o;
            for (org.apache.activemq.network.NetworkConnector existingCandidate :
                    getBrokerService().getNetworkConnectors()) {
                if (configMatch(toRemove, existingCandidate)) {
                    if (getBrokerService().removeNetworkConnector(existingCandidate)) {
                        try {
                            existingCandidate.stop();
                            LOG.info("stopped and removed networkConnector: " + existingCandidate);
                        } catch (Exception e) {
                            LOG.error("Failed to stop removed network connector: " + existingCandidate);
                        }
                    }
                }
            }
        }
    }

    private boolean configMatch(NetworkConnector dto, org.apache.activemq.network.NetworkConnector candidate) {
        TreeMap<String, String> dtoProps = new TreeMap<String, String>();
        IntrospectionSupport.getProperties(dto, dtoProps, null);

        TreeMap<String, String> candidateProps = new TreeMap<String, String>();
        IntrospectionSupport.getProperties(candidate, candidateProps, null);

        // every dto prop must be present in the candidate
        for (String key : dtoProps.keySet()) {
            if (!candidateProps.containsKey(key) || !candidateProps.get(key).equals(dtoProps.get(key))) {
                return false;
            }
        }
        return true;
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
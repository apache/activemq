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
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import javax.management.JMException;
import javax.management.ObjectName;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.plugin.jmx.RuntimeConfigurationView;
import org.apache.activemq.schema.core.DtoBroker;
import org.apache.activemq.spring.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class RuntimeConfigurationBroker extends AbstractRuntimeConfigurationBroker {

    public static final Logger LOG = LoggerFactory.getLogger(RuntimeConfigurationBroker.class);
    public static final String objectNamePropsAppendage = ",service=RuntimeConfiguration,name=Plugin";
    PropertiesPlaceHolderUtil placeHolderUtil = null;
    private long checkPeriod;
    private long lastModified = -1;
    private Resource configToMonitor;
    private DtoBroker currentConfiguration;
    private Schema schema;

    public RuntimeConfigurationBroker(Broker next) {
        super(next);
    }

    @Override
    public void start() throws Exception {
        super.start();
        try {
            BrokerContext brokerContext = next.getBrokerService().getBrokerContext();
            if (brokerContext != null) {
                configToMonitor = Utils.resourceFromString(brokerContext.getConfigurationUrl());
                info("Configuration " + configToMonitor);
            } else {
                LOG.error("Null BrokerContext; impossible to determine configuration url resource from broker, updates cannot be tracked");
            }
        } catch (Exception error) {
            LOG.error("failed to determine configuration url resource from broker, updates cannot be tracked", error);
        }

        currentConfiguration = loadConfiguration(configToMonitor);
        monitorModification(configToMonitor);
        registerMbean();
    }

    @Override
    protected void registerMbean() {
        if (getBrokerService().isUseJmx()) {
            ManagementContext managementContext = getBrokerService().getManagementContext();
            try {
                objectName = new ObjectName(getBrokerService().getBrokerObjectName().toString() + objectNamePropsAppendage);
                managementContext.registerMBean(new RuntimeConfigurationView(this), objectName);
            } catch (Exception ignored) {
                LOG.debug("failed to register RuntimeConfigurationMBean", ignored);
            }
        }
    }

    @Override
    protected void unregisterMbean() {
        if (objectName != null) {
            try {
                getBrokerService().getManagementContext().unregisterMBean(objectName);
            } catch (JMException ignored) {
            }
        }
    }

    public String updateNow() {
        LOG.info("Manual configuration update triggered");
        infoString = "";
        applyModifications(configToMonitor);
        String result = infoString;
        infoString = null;
        return result;
    }

    private void monitorModification(final Resource configToMonitor) {
        monitorTask = new Runnable() {
            @Override
            public void run() {
                try {
                    if (configToMonitor.lastModified() > lastModified) {
                        applyModifications(configToMonitor);
                    }
                } catch (Throwable e) {
                    LOG.error("Failed to determine lastModified time on configuration: " + configToMonitor, e);
                }
            }
        };
        if (lastModified > 0 && checkPeriod > 0) {
            this.getBrokerService().getScheduler().executePeriodically(monitorTask, checkPeriod);
            info("Monitoring for updates (every " + checkPeriod + "millis) : " + configToMonitor + ", lastUpdate: " + new Date(lastModified));
        }
    }



    private void applyModifications(Resource configToMonitor) {
        DtoBroker changed = loadConfiguration(configToMonitor);
        if (changed != null && !currentConfiguration.equals(changed)) {
            LOG.info("change in " + configToMonitor + " at: " + new Date(lastModified));
            LOG.debug("current:" + filterPasswords(currentConfiguration));
            LOG.debug("new    :" + filterPasswords(changed));
            processSelectiveChanges(currentConfiguration, changed);
            currentConfiguration = changed;
        } else {
            info("No material change to configuration in " + configToMonitor + " at: " + new Date(lastModified));
        }
    }

    private void processSelectiveChanges(DtoBroker currentConfiguration, DtoBroker modifiedConfiguration) {

        for (Class upDatable : new Class[]{
                DtoBroker.DestinationPolicy.class,
                DtoBroker.NetworkConnectors.class,
                DtoBroker.DestinationInterceptors.class,
                DtoBroker.Plugins.class,
                DtoBroker.Destinations.class}) {
            processChanges(currentConfiguration, modifiedConfiguration, upDatable);
        }
    }

    private void processChanges(DtoBroker currentConfiguration, DtoBroker modifiedConfiguration, Class upDatable) {
        ConfigurationProcessor processor = ProcessorFactory.createProcessor(this, upDatable);
        processor.processChanges(currentConfiguration, modifiedConfiguration);
    }



    private DtoBroker loadConfiguration(Resource configToMonitor) {
        DtoBroker jaxbConfig = null;
        if (configToMonitor != null) {
            try {
                JAXBContext context = JAXBContext.newInstance(DtoBroker.class);
                Unmarshaller unMarshaller = context.createUnmarshaller();
                unMarshaller.setSchema(getSchema());

                // skip beans and pull out the broker node to validate
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                dbf.setNamespaceAware(true);
                dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, Boolean.TRUE);
                dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.parse(configToMonitor.getInputStream());
                Node brokerRootNode = doc.getElementsByTagNameNS("*","broker").item(0);

                if (brokerRootNode != null) {

                    JAXBElement<DtoBroker> brokerJAXBElement =
                            unMarshaller.unmarshal(brokerRootNode, DtoBroker.class);
                    jaxbConfig = brokerJAXBElement.getValue();

                    // if we can parse we can track mods
                    lastModified = configToMonitor.lastModified();

                    loadPropertiesPlaceHolderSupport(doc);

                } else {
                    info("Failed to find 'broker' element by tag in: " + configToMonitor);
                }

            } catch (IOException e) {
                info("Failed to access: " + configToMonitor, e);
            } catch (JAXBException e) {
                info("Failed to parse: " + configToMonitor, e);
            } catch (ParserConfigurationException e) {
                info("Failed to document parse: " + configToMonitor, e);
            } catch (SAXException e) {
                info("Failed to find broker element in: " + configToMonitor, e);
            } catch (Exception e) {
                info("Unexpected exception during load of: " + configToMonitor, e);
            }
        }
        return jaxbConfig;
    }

    private void loadPropertiesPlaceHolderSupport(Document doc) {
        BrokerContext brokerContext = getBrokerService().getBrokerContext();
        if (brokerContext != null) {
            Properties initialProperties = new Properties(System.getProperties());
            placeHolderUtil = new PropertiesPlaceHolderUtil(initialProperties);
            placeHolderUtil.mergeProperties(doc, initialProperties, brokerContext);
        }
    }

    private Schema getSchema() throws SAXException, IOException {
        if (schema == null) {
            SchemaFactory schemaFactory = SchemaFactory.newInstance(
                    XMLConstants.W3C_XML_SCHEMA_NS_URI);

            ArrayList<StreamSource> schemas = new ArrayList<StreamSource>();
            schemas.add(new StreamSource(getClass().getResource("/activemq.xsd").toExternalForm()));
            if (getClass().getResource("/org/springframework/beans/factory/xml/spring-beans-3.0.xsd") != null) {
                schemas.add(new StreamSource(getClass().getResource("/org/springframework/beans/factory/xml/spring-beans-3.0.xsd").toExternalForm()));
            } else {
                schemas.add(new StreamSource(getClass().getResource("/org/springframework/beans/factory/xml/spring-beans.xsd").toExternalForm()));
            }
            schema = schemaFactory.newSchema(schemas.toArray(new Source[]{}));
        }
        return schema;
    }

    public long getLastModified() {
        return lastModified;
    }

    public Resource getConfigToMonitor() {
        return configToMonitor;
    }

    public long getCheckPeriod() {
        return checkPeriod;
    }

    public void setCheckPeriod(long checkPeriod) {
        this.checkPeriod = checkPeriod;
    }

}

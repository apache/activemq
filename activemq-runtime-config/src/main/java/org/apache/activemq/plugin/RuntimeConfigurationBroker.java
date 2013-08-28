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
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.CompositeQueue;
import org.apache.activemq.broker.region.virtual.CompositeTopic;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.plugin.jmx.RuntimeConfigurationView;
import org.apache.activemq.schema.core.DtoAuthorizationEntry;
import org.apache.activemq.schema.core.DtoAuthorizationMap;
import org.apache.activemq.schema.core.DtoAuthorizationPlugin;
import org.apache.activemq.schema.core.DtoBroker;
import org.apache.activemq.schema.core.DtoCompositeQueue;
import org.apache.activemq.schema.core.DtoCompositeTopic;
import org.apache.activemq.schema.core.DtoNetworkConnector;
import org.apache.activemq.schema.core.DtoPolicyEntry;
import org.apache.activemq.schema.core.DtoPolicyMap;
import org.apache.activemq.schema.core.DtoVirtualDestinationInterceptor;
import org.apache.activemq.schema.core.DtoVirtualTopic;
import org.apache.activemq.security.AuthorizationBroker;
import org.apache.activemq.security.AuthorizationMap;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.apache.activemq.security.XBeanAuthorizationEntry;
import org.apache.activemq.security.XBeanAuthorizationMap;
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
    public static final String objectNamePropsAppendage = ",service=RuntimeConfiguration,name=Plugin";
    private final ReentrantReadWriteLock addDestinationBarrier = new ReentrantReadWriteLock();
    private long checkPeriod;
    private long lastModified = -1;
    private Resource configToMonitor;
    private DtoBroker currentConfiguration;
    private Runnable monitorTask;
    private ConcurrentLinkedQueue<Runnable> destinationInterceptorUpdateWork = new ConcurrentLinkedQueue<Runnable>();
    private ObjectName objectName;
    private String infoString;
    private Schema schema;

    public RuntimeConfigurationBroker(Broker next) {
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
        registerMbean();
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
        unregisterMbean();
        super.stop();
    }

    private void registerMbean() {
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

    private void unregisterMbean() {
        if (objectName != null) {
            try {
                getBrokerService().getManagementContext().unregisterMBean(objectName);
            } catch (JMException ignored) {
            }
        }
    }

    // modification to virtual destinations interceptor needs exclusive access to destination add
    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemporary) throws Exception {
        Runnable work = destinationInterceptorUpdateWork.poll();
        if (work != null) {
            try {
                addDestinationBarrier.writeLock().lockInterruptibly();
                do {
                    work.run();
                    work = destinationInterceptorUpdateWork.poll();
                } while (work != null);
                return super.addDestination(context, destination, createIfTemporary);
            } finally {
                addDestinationBarrier.writeLock().unlock();
            }
        } else {
            try {
                addDestinationBarrier.readLock().lockInterruptibly();
                return super.addDestination(context, destination, createIfTemporary);
            } finally {
                addDestinationBarrier.readLock().unlock();
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
                } catch (IOException e) {
                    LOG.error("Failed to determine lastModified time on configuration: " + configToMonitor, e);
                }
            }
        };
        if (lastModified > 0 && checkPeriod > 0) {
            this.getBrokerService().getScheduler().executePeriodically(monitorTask, checkPeriod);
            info("Monitoring for updates (every " + checkPeriod + "millis) : " + configToMonitor);
        }
    }

    private void info(String s) {
        LOG.info(s);
        if (infoString != null) {
            infoString += s;
            infoString += ";";
        }
    }

    private void info(String s, Throwable t) {
        LOG.info(s, t);
        if (infoString != null) {
            infoString += s;
            infoString += ", " + t;
            infoString += ";";
        }
    }

    private void applyModifications(Resource configToMonitor) {
        DtoBroker changed = loadConfiguration(configToMonitor);
        if (changed != null && !currentConfiguration.equals(changed)) {
            LOG.info("change in " + configToMonitor + " at: " + new Date(lastModified));
            LOG.debug("current:" + currentConfiguration);
            LOG.debug("new    :" + changed);
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
                DtoBroker.Plugins.class}) {
            processChanges(currentConfiguration, modifiedConfiguration, upDatable);
        }
    }

    private void processChanges(DtoBroker currentConfiguration, DtoBroker modifiedConfiguration, Class upDatable) {

        List current = filter(currentConfiguration, upDatable);
        List modified = filter(modifiedConfiguration, upDatable);

        if (current.equals(modified)) {
            LOG.debug("no changes to " + upDatable.getSimpleName());
            return;
        } else {
            info("changes to " + upDatable.getSimpleName());
        }

        int modIndex = 0, currentIndex = 0;
        for (; modIndex < modified.size() && currentIndex < current.size(); modIndex++, currentIndex++) {
            // walk the list for mods
            applyModifications(getContents(current.get(currentIndex)),
                    getContents(modified.get(modIndex)));
        }

        for (; modIndex < modified.size(); modIndex++) {
            // new element; add all
            for (Object nc : getContents(modified.get(modIndex))) {
                addNew(nc);
            }
        }

        for (; currentIndex < current.size(); currentIndex++) {
            // removal of element; remove all
            for (Object nc : getContents(current.get(currentIndex))) {
                remove(nc);
            }
        }
    }

    // mapping all supported updatable elements to support getContents
    private List<Object> getContents(Object o) {
        List<Object> answer = new ArrayList<Object>();
        try {
            Object val = o.getClass().getMethod("getContents", new Class[]{}).invoke(o, new Object[]{});
            if (val instanceof List) {
                answer = (List<Object>) val;
            } else {
                answer.add(val);
            }
        } catch (Exception e) {
            info("Failed to access getContents for " + o + ", runtime modifications not supported", e);
        }
        return answer;
    }

    private void applyModifications(List<Object> current, List<Object> modification) {
        int modIndex = 0, currentIndex = 0;
        for (; modIndex < modification.size() && currentIndex < current.size(); modIndex++, currentIndex++) {
            Object existing = current.get(currentIndex);
            Object candidate = modification.get(modIndex);
            if (!existing.equals(candidate)) {
                info("modification to:" + existing + " , with: " + candidate);
                modify(existing, candidate);
            }
        }

        for (; modIndex < modification.size(); modIndex++) {
            addNew(modification.get(modIndex));
        }

        for (; currentIndex < current.size(); currentIndex++) {
            remove(current.get(currentIndex));
        }
    }

    private void modify(Object existing, Object candidate) {
        if (candidate instanceof DtoAuthorizationPlugin) {

            try {
                // replace authorization map - need exclusive write lock to total broker
                AuthorizationBroker authorizationBroker =
                        (AuthorizationBroker) getBrokerService().getBroker().getAdaptor(AuthorizationBroker.class);

                authorizationBroker.setAuthorizationMap(fromDto(filter(candidate, DtoAuthorizationPlugin.Map.class)));
            } catch (Exception e) {
                info("failed to apply modified AuthorizationMap to AuthorizationBroker", e);
            }

        } else if (candidate instanceof DtoPolicyMap) {

            List<Object> existingEntries = filter(existing, DtoPolicyMap.PolicyEntries.class);
            List<Object> candidateEntries = filter(candidate, DtoPolicyMap.PolicyEntries.class);
            // walk the map for mods
            applyModifications(getContents(existingEntries.get(0)), getContents(candidateEntries.get(0)));

        } else if (candidate instanceof DtoPolicyEntry) {

            PolicyMap existingMap = getBrokerService().getDestinationPolicy();

            PolicyEntry updatedEntry = fromDto(candidate, new PolicyEntry());

            Set existingEntry = existingMap.get(updatedEntry.getDestination());
            if (existingEntry.size() == 1) {
                updatedEntry = fromDto(candidate, (PolicyEntry) existingEntry.iterator().next());
                applyRetrospectively(updatedEntry);
                info("updated policy for: " + updatedEntry.getDestination());
            } else {
                info("cannot modify policy matching multiple destinations: " + existingEntry + ", destination:" + updatedEntry.getDestination());
            }

        } else {
            remove(existing);
            addNew(candidate);
        }
    }

    private void applyRetrospectively(PolicyEntry updatedEntry) {
        RegionBroker regionBroker = (RegionBroker) getBrokerService().getRegionBroker();
        for (Destination destination : regionBroker.getDestinations(updatedEntry.getDestination())) {
            if (destination.getActiveMQDestination().isQueue()) {
                updatedEntry.update((Queue) destination);
            } else if (destination.getActiveMQDestination().isTopic()) {
                updatedEntry.update((Topic) destination);
            }
            LOG.debug("applied update to:" + destination);
        }
    }

    private AuthorizationMap fromDto(List<Object> map) {
        XBeanAuthorizationMap xBeanAuthorizationMap = new XBeanAuthorizationMap();
        for (Object o : map) {
            if (o instanceof DtoAuthorizationPlugin.Map) {
                DtoAuthorizationPlugin.Map dtoMap = (DtoAuthorizationPlugin.Map) o;
                List<DestinationMapEntry> entries = new LinkedList<DestinationMapEntry>();
                // revisit - would like to map getAuthorizationMap to generic getContents
                for (Object authMap : filter(dtoMap.getAuthorizationMap(), DtoAuthorizationMap.AuthorizationEntries.class)) {
                    for (Object entry : filter(getContents(authMap), DtoAuthorizationEntry.class)) {
                        entries.add(fromDto(entry, new XBeanAuthorizationEntry()));
                    }
                }
                xBeanAuthorizationMap.setAuthorizationEntries(entries);
                try {
                    xBeanAuthorizationMap.afterPropertiesSet();
                } catch (Exception e) {
                    info("failed to update xBeanAuthorizationMap auth entries:", e);
                }

                for (Object entry : filter(dtoMap.getAuthorizationMap(), DtoAuthorizationMap.TempDestinationAuthorizationEntry.class)) {
                    // another restriction - would like to be getContents
                    DtoAuthorizationMap.TempDestinationAuthorizationEntry dtoEntry = (DtoAuthorizationMap.TempDestinationAuthorizationEntry) entry;
                    xBeanAuthorizationMap.setTempDestinationAuthorizationEntry(fromDto(dtoEntry.getTempDestinationAuthorizationEntry(), new TempDestinationAuthorizationEntry()));
                }

            } else {
                info("No support for updates to: " + o);
            }
        }
        return xBeanAuthorizationMap;
    }

    private void remove(Object o) {
        if (o instanceof DtoNetworkConnector) {
            DtoNetworkConnector toRemove = (DtoNetworkConnector) o;
            for (NetworkConnector existingCandidate :
                    getBrokerService().getNetworkConnectors()) {
                if (configMatch(toRemove, existingCandidate)) {
                    if (getBrokerService().removeNetworkConnector(existingCandidate)) {
                        try {
                            existingCandidate.stop();
                            info("stopped and removed networkConnector: " + existingCandidate);
                        } catch (Exception e) {
                            info("Failed to stop removed network connector: " + existingCandidate);
                        }
                    }
                }
            }
        } else if (o instanceof DtoVirtualDestinationInterceptor) {
            // whack it
            destinationInterceptorUpdateWork.add(new Runnable() {
                public void run() {
                    List<DestinationInterceptor> interceptorsList = new ArrayList<DestinationInterceptor>();
                    for (DestinationInterceptor candidate : getBrokerService().getDestinationInterceptors()) {
                        if (!(candidate instanceof VirtualDestinationInterceptor)) {
                            interceptorsList.add(candidate);
                        }
                    }
                    DestinationInterceptor[] destinationInterceptors = interceptorsList.toArray(new DestinationInterceptor[]{});
                    getBrokerService().setDestinationInterceptors(destinationInterceptors);
                    ((CompositeDestinationInterceptor) ((RegionBroker) getBrokerService().getRegionBroker()).getDestinationInterceptor()).setInterceptors(destinationInterceptors);
                    info("removed VirtualDestinationInterceptor from: " + interceptorsList);
                }
            });
        } else {
            info("No runtime support for removal of: " + o);
        }
    }

    private boolean configMatch(DtoNetworkConnector dto, NetworkConnector candidate) {
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

    private void addNew(Object o) {
        if (o instanceof DtoNetworkConnector) {
            DtoNetworkConnector networkConnector = (DtoNetworkConnector) o;
            if (networkConnector.getUri() != null) {
                try {
                    NetworkConnector nc =
                            getBrokerService().addNetworkConnector(networkConnector.getUri());
                    Properties properties = new Properties();
                    IntrospectionSupport.getProperties(networkConnector, properties, null);
                    properties.remove("uri");
                    LOG.trace("applying networkConnector props: " + properties);
                    IntrospectionSupport.setProperties(nc, properties);
                    nc.start();
                    info("started new network connector: " + nc);
                } catch (Exception e) {
                    info("Failed to add new networkConnector " + networkConnector, e);
                }
            }
        } else if (o instanceof DtoVirtualDestinationInterceptor) {
            final DtoVirtualDestinationInterceptor dto = (DtoVirtualDestinationInterceptor) o;
            destinationInterceptorUpdateWork.add(new Runnable() {
                public void run() {

                    boolean updatedExistingInterceptor = false;

                    for (DestinationInterceptor destinationInterceptor : getBrokerService().getDestinationInterceptors()) {
                        if (destinationInterceptor instanceof VirtualDestinationInterceptor) {
                            // update existing interceptor
                            final VirtualDestinationInterceptor virtualDestinationInterceptor =
                                    (VirtualDestinationInterceptor) destinationInterceptor;

                            virtualDestinationInterceptor.setVirtualDestinations(fromDto(dto));
                            info("applied updates to: " + virtualDestinationInterceptor);
                            updatedExistingInterceptor = true;
                        }
                    }

                    if (!updatedExistingInterceptor) {
                        // add
                        VirtualDestinationInterceptor virtualDestinationInterceptor =
                                new VirtualDestinationInterceptor();
                        virtualDestinationInterceptor.setVirtualDestinations(fromDto(dto));

                        List<DestinationInterceptor> interceptorsList = new ArrayList<DestinationInterceptor>();
                        interceptorsList.addAll(Arrays.asList(getBrokerService().getDestinationInterceptors()));
                        interceptorsList.add(virtualDestinationInterceptor);

                        DestinationInterceptor[] destinationInterceptors = interceptorsList.toArray(new DestinationInterceptor[]{});
                        getBrokerService().setDestinationInterceptors(destinationInterceptors);
                        RegionBroker regionBroker = (RegionBroker) getBrokerService().getRegionBroker();
                        ((CompositeDestinationInterceptor) regionBroker.getDestinationInterceptor()).setInterceptors(destinationInterceptors);
                        info("applied new: " + interceptorsList);
                    }
                }
            });
        } else if (o instanceof DtoPolicyEntry) {

            PolicyEntry addition = fromDto(o, new PolicyEntry());
            PolicyMap existingMap = getBrokerService().getDestinationPolicy();
            existingMap.put(addition.getDestination(), addition);
            applyRetrospectively(addition);
            info("added policy for: " + addition.getDestination());

        } else {
            info("No runtime support for additions of " + o);
        }
    }

    private VirtualDestination[] fromDto(DtoVirtualDestinationInterceptor virtualDestinationInterceptor) {
        List<VirtualDestination> answer = new ArrayList<VirtualDestination>();
        for (Object vd : filter(virtualDestinationInterceptor, DtoVirtualDestinationInterceptor.VirtualDestinations.class)) {
            for (Object vt : filter(vd, DtoVirtualTopic.class)) {
                answer.add(fromDto(vt, new VirtualTopic()));
            }
            for (Object vt : filter(vd, DtoCompositeTopic.class)) {
                answer.add(fromDto(vt, new CompositeTopic()));
            }
            for (Object vt : filter(vd, DtoCompositeQueue.class)) {
                answer.add(fromDto(vt, new CompositeQueue()));
            }
        }
        VirtualDestination[] array = new VirtualDestination[answer.size()];
        answer.toArray(array);
        return array;
    }

    private <T> T fromDto(Object dto, T instance) {
        Properties properties = new Properties();
        IntrospectionSupport.getProperties(dto, properties, null);
        LOG.trace("applying props: " + properties + ", to " + instance.getClass().getSimpleName());
        IntrospectionSupport.setProperties(instance, properties);
        return instance;
    }

    private <T> List<Object> filter(Object obj, Class<T> type) {
        return filter(getContents(obj), type);
    }

    private <T> List<Object> filter(List<Object> objectList, Class<T> type) {
        List<Object> result = new LinkedList<Object>();
        for (Object o : objectList) {
            if (o instanceof JAXBElement) {
                JAXBElement element = (JAXBElement) o;
                if (element.getDeclaredType() == type) {
                    result.add((T) element.getValue());
                }
            } else if (type.isAssignableFrom(o.getClass())) {
                result.add((T) o);
            }
        }
        return result;
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
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.parse(configToMonitor.getInputStream());
                Node brokerRootNode = doc.getElementsByTagName("broker").item(0);

                JAXBElement<DtoBroker> brokerJAXBElement =
                        unMarshaller.unmarshal(brokerRootNode, DtoBroker.class);
                jaxbConfig = brokerJAXBElement.getValue();

                // if we can parse we can track mods
                lastModified = configToMonitor.lastModified();

            } catch (IOException e) {
                info("Failed to access: " + configToMonitor, e);
            } catch (JAXBException e) {
                info("Failed to parse: " + configToMonitor, e);
            } catch (ParserConfigurationException e) {
                info("Failed to document parse: " + configToMonitor, e);
            } catch (SAXException e) {
                info("Failed to find broker element in: " + configToMonitor, e);
            }
        }
        return jaxbConfig;
    }

    private Schema getSchema() throws SAXException {
        if (schema == null) {
            // need to pull the spring schemas from the classpath and find reelvant
            // constants for the system id etc something like ...
            //PluggableSchemaResolver resolver =
            //        new PluggableSchemaResolver(getClass().getClassLoader());
            //InputSource springBeans = resolver.resolveEntity("http://www.springframework.org/schema/beans",
            //                                                "http://www.springframework.org/schema/beans/spring-beans-2.0.xsd");
            //LOG.trace("Beans schema:" + springBeans);
            SchemaFactory schemaFactory = SchemaFactory.newInstance(
                    XMLConstants.W3C_XML_SCHEMA_NS_URI);
            schema = schemaFactory.newSchema(
                    new Source[]{new StreamSource(getClass().getResource("/activemq.xsd").toExternalForm()),
                            new StreamSource("http://www.springframework.org/schema/beans/spring-beans-2.0.xsd")});
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
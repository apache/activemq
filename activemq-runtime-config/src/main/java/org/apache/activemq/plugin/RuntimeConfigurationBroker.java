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
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.region.*;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.CompositeQueue;
import org.apache.activemq.broker.region.virtual.CompositeTopic;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.plugin.jmx.RuntimeConfigurationView;
import org.apache.activemq.schema.core.DtoAuthenticationUser;
import org.apache.activemq.schema.core.DtoAuthorizationEntry;
import org.apache.activemq.schema.core.DtoAuthorizationMap;
import org.apache.activemq.schema.core.DtoAuthorizationPlugin;
import org.apache.activemq.schema.core.DtoBroker;
import org.apache.activemq.schema.core.DtoCompositeQueue;
import org.apache.activemq.schema.core.DtoCompositeTopic;
import org.apache.activemq.schema.core.DtoNetworkConnector;
import org.apache.activemq.schema.core.DtoPolicyEntry;
import org.apache.activemq.schema.core.DtoPolicyMap;
import org.apache.activemq.schema.core.DtoQueue;
import org.apache.activemq.schema.core.DtoSimpleAuthenticationPlugin;
import org.apache.activemq.schema.core.DtoTopic;
import org.apache.activemq.schema.core.DtoVirtualDestinationInterceptor;
import org.apache.activemq.schema.core.DtoVirtualTopic;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationBroker;
import org.apache.activemq.security.AuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationBroker;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.apache.activemq.security.XBeanAuthorizationEntry;
import org.apache.activemq.security.XBeanAuthorizationMap;
import org.apache.activemq.spring.Utils;
import org.apache.activemq.util.IntrospectionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.xml.PluggableSchemaResolver;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class RuntimeConfigurationBroker extends BrokerFilter {

    public static final Logger LOG = LoggerFactory.getLogger(RuntimeConfigurationBroker.class);
    public static final String objectNamePropsAppendage = ",service=RuntimeConfiguration,name=Plugin";
    private final ReentrantReadWriteLock addDestinationBarrier = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock addConnectionBarrier = new ReentrantReadWriteLock();
    PropertiesPlaceHolderUtil placeHolderUtil = null;
    private long checkPeriod;
    private long lastModified = -1;
    private Resource configToMonitor;
    private DtoBroker currentConfiguration;
    private Runnable monitorTask;
    private ConcurrentLinkedQueue<Runnable> addDestinationWork = new ConcurrentLinkedQueue<Runnable>();
    private ConcurrentLinkedQueue<Runnable> addConnectionWork = new ConcurrentLinkedQueue<Runnable>();
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
        Runnable work = addDestinationWork.poll();
        if (work != null) {
            try {
                addDestinationBarrier.writeLock().lockInterruptibly();
                do {
                    work.run();
                    work = addDestinationWork.poll();
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

    // modification to authentication plugin needs exclusive access to connection add
    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        Runnable work = addConnectionWork.poll();
        if (work != null) {
            try {
                addConnectionBarrier.writeLock().lockInterruptibly();
                do {
                    work.run();
                    work = addConnectionWork.poll();
                } while (work != null);
                super.addConnection(context, info);
            } finally {
                addConnectionBarrier.writeLock().unlock();
            }
        } else {
            try {
                addConnectionBarrier.readLock().lockInterruptibly();
                super.addConnection(context, info);
            } finally {
                addConnectionBarrier.readLock().unlock();
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

    private void info(String s) {
        LOG.info(filterPasswords(s));
        if (infoString != null) {
            infoString += s;
            infoString += ";";
        }
    }

    private void info(String s, Throwable t) {
        LOG.info(filterPasswords(s), t);
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
        } catch (NoSuchMethodException mappingIncomplete) {
            LOG.debug(filterPasswords(o) + " has no modifiable elements");
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

        } else if (candidate instanceof DtoSimpleAuthenticationPlugin) {
            try {
                final SimpleAuthenticationPlugin updatedPlugin = fromDto(candidate, new SimpleAuthenticationPlugin());
                final SimpleAuthenticationBroker authenticationBroker =
                    (SimpleAuthenticationBroker) getBrokerService().getBroker().getAdaptor(SimpleAuthenticationBroker.class);
                addConnectionWork.add(new Runnable() {
                    public void run() {
                        authenticationBroker.setUserGroups(updatedPlugin.getUserGroups());
                        authenticationBroker.setUserPasswords(updatedPlugin.getUserPasswords());
                        authenticationBroker.setAnonymousAccessAllowed(updatedPlugin.isAnonymousAccessAllowed());
                        authenticationBroker.setAnonymousUser(updatedPlugin.getAnonymousUser());
                        authenticationBroker.setAnonymousGroup(updatedPlugin.getAnonymousGroup());
                    }
                });
            } catch (Exception e) {
                info("failed to apply SimpleAuthenticationPlugin modifications to SimpleAuthenticationBroker", e);
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
            Destination target = destination;
            if (destination instanceof DestinationFilter) {
                target = ((DestinationFilter)destination).getNext();
            }
            if (target.getActiveMQDestination().isQueue()) {
                updatedEntry.update((Queue) target);
            } else if (target.getActiveMQDestination().isTopic()) {
                updatedEntry.update((Topic) target);
            }
            LOG.debug("applied update to:" + target);
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
            addDestinationWork.add(new Runnable() {
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
                    DiscoveryNetworkConnector nc = fromDto(networkConnector, new DiscoveryNetworkConnector());
                    getBrokerService().addNetworkConnector(nc);
                    nc.start();
                    info("started new network connector: " + nc);
                } catch (Exception e) {
                    info("Failed to add new networkConnector " + networkConnector, e);
                }
            }
        } else if (o instanceof DtoVirtualDestinationInterceptor) {
            final DtoVirtualDestinationInterceptor dto = (DtoVirtualDestinationInterceptor) o;
            addDestinationWork.add(new Runnable() {
                public void run() {

                    boolean updatedExistingInterceptor = false;
                    RegionBroker regionBroker = (RegionBroker) getBrokerService().getRegionBroker();

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

                        ((CompositeDestinationInterceptor) regionBroker.getDestinationInterceptor()).setInterceptors(destinationInterceptors);
                        info("applied new: " + interceptorsList);
                    }
                    regionBroker.reapplyInterceptor();
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
        replacePlaceHolders(properties);
        LOG.trace("applying props: " + filterPasswords(properties) + ", to " + instance.getClass().getSimpleName());
        IntrospectionSupport.setProperties(instance, properties);

        // deal with nested elements
        for (Object nested : filter(dto, Object.class)) {
            String elementName = nested.getClass().getSimpleName();
            Method setter = findSetter(instance, elementName);
            if (setter != null) {
                List<Object> argument = new LinkedList<Object>();
                for (Object elementContent : filter(nested, Object.class)) {
                    argument.add(fromDto(elementContent, inferTargetObject(elementContent)));
                }
                try {
                    setter.invoke(instance, matchType(argument, setter.getParameterTypes()[0]));
                } catch (Exception e) {
                    info("failed to invoke " + setter + " on " + instance, e);
                }
            } else {
                info("failed to find setter for " + elementName + " on :" + instance);
            }
        }
        return instance;
    }

    Pattern matchPassword = Pattern.compile("password=.*,");
    private String filterPasswords(Object toEscape) {
        return matchPassword.matcher(toEscape.toString()).replaceAll("password=???,");
    }

    private Object matchType(List<Object> parameterValues, Class<?> aClass) {
        Object result = parameterValues;
        if (Set.class.isAssignableFrom(aClass)) {
            result = new HashSet(parameterValues);
        }
        return result;
    }

    private Object inferTargetObject(Object elementContent) {
        if (DtoTopic.class.isAssignableFrom(elementContent.getClass())) {
            return new ActiveMQTopic();
        } else if (DtoQueue.class.isAssignableFrom(elementContent.getClass())) {
            return new ActiveMQQueue();
        } else if (DtoAuthenticationUser.class.isAssignableFrom(elementContent.getClass())) {
            return new AuthenticationUser();
        } else {
            info("update not supported for dto: " + elementContent);
            return new Object();
        }
    }

    private Method findSetter(Object instance, String elementName) {
        String setter = "set" + elementName;
        for (Method m : instance.getClass().getMethods()) {
            if (setter.equals(m.getName())) {
                return m;
            }
        }
        return null;
    }

    private <T> List<Object> filter(Object obj, Class<T> type) {
        return filter(getContents(obj), type);
    }

    private <T> List<Object> filter(List<Object> objectList, Class<T> type) {
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
            mergeProperties(doc, initialProperties, brokerContext);
        }
    }

    private void mergeProperties(Document doc, Properties initialProperties, BrokerContext brokerContext) {
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
                    propResources.add(Utils.resourceFromString(replacePlaceHolders(value)));
                }
            } catch (MalformedURLException e) {
                info("failed to resolve resource: " + value, e);
            }
        }
        for (Resource resource : propResources) {
            Properties properties = new Properties();
            try {
                properties.load(resource.getInputStream());
            } catch (IOException e) {
                info("failed to load properties resource: " + resource, e);
            }
            initialProperties.putAll(properties);
        }
    }

    private void replacePlaceHolders(Properties properties) {
        if (placeHolderUtil != null) {
            placeHolderUtil.filter(properties);
        }
    }

    private String replacePlaceHolders(String s) {
        if (placeHolderUtil != null) {
            s = placeHolderUtil.filter(s);
        }
        return s;
    }

    private Schema getSchema() throws SAXException, IOException {
        if (schema == null) {
            SchemaFactory schemaFactory = SchemaFactory.newInstance(
                    XMLConstants.W3C_XML_SCHEMA_NS_URI);

            ArrayList<StreamSource> schemas = new ArrayList<StreamSource>();
            schemas.add(new StreamSource(getClass().getResource("/activemq.xsd").toExternalForm()));
            schemas.add(new StreamSource(getClass().getResource("/org/springframework/beans/factory/xml/spring-beans-3.0.xsd").toExternalForm()));
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

    static public class PropertiesPlaceHolderUtil {

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

    }
}
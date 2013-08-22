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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.schema.core.Broker;
import org.apache.activemq.schema.core.CompositeQueue;
import org.apache.activemq.schema.core.CompositeTopic;
import org.apache.activemq.schema.core.NetworkConnector;
import org.apache.activemq.schema.core.VirtualDestinationInterceptor;
import org.apache.activemq.schema.core.VirtualTopic;
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
    private ConcurrentLinkedQueue<Runnable> destinationInterceptorUpdateWork = new ConcurrentLinkedQueue<Runnable>();
    private final ReentrantReadWriteLock addDestinationBarrier = new ReentrantReadWriteLock();

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
            processSelectiveChanges(currentConfiguration, changed);
            currentConfiguration = changed;
        } else {
            LOG.info("file modification but no material change to configuration in " + configToMonitor + " at: " + new Date(lastModified));
        }
    }

    private void processSelectiveChanges(Broker currentConfiguration, Broker modifiedConfiguration) {

        for (Class upDatable : new Class[]{Broker.NetworkConnectors.class, Broker.DestinationInterceptors.class}) {
             processChanges(currentConfiguration, modifiedConfiguration, upDatable);
        }
    }

    private void processChanges(Broker currentConfiguration, Broker modifiedConfiguration, Class upDatable) {

        List current = filter(currentConfiguration, upDatable);
        List modified = filter(modifiedConfiguration, upDatable);

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
        try {
            return (List<Object>) o.getClass().getMethod("getContents", new Class[]{}).invoke(o, new Object[]{});
        } catch (Exception e) {
            LOG.info("Failed to access getContents for " + o + ", runtime modifications not supported", e);
        }
        return new ArrayList<Object>();
    }

    private void applyModifications(List<Object> current, List<Object> modification) {
        int modIndex = 0, currentIndex = 0;
        for (; modIndex < modification.size() && currentIndex < current.size(); modIndex++, currentIndex++) {
            Object existing = current.get(currentIndex);
            Object candidate = modification.get(modIndex);
            if (! existing.equals(candidate)) {
                LOG.info("modification to:" + existing + " , with: " + candidate);
                remove(existing);
                addNew(candidate);
            }
        }

        for (; modIndex < modification.size(); modIndex++) {
            addNew(modification.get(modIndex));
        }

        for (; currentIndex < current.size(); currentIndex++) {
            remove(current.get(currentIndex));
        }
    }

    private void remove(Object o) {
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
        } else if (o instanceof VirtualDestinationInterceptor) {
            // whack it
            destinationInterceptorUpdateWork.add(new Runnable() {
                public void run() {
                    List<DestinationInterceptor> interceptorsList = new ArrayList<DestinationInterceptor>();
                    for (DestinationInterceptor candidate : getBrokerService().getDestinationInterceptors()) {
                        if (!(candidate instanceof org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor)) {
                            interceptorsList.add(candidate);
                        }
                    }
                    DestinationInterceptor[] destinationInterceptors = interceptorsList.toArray(new DestinationInterceptor[]{});
                    getBrokerService().setDestinationInterceptors(destinationInterceptors);
                    ((CompositeDestinationInterceptor) ((RegionBroker) getBrokerService().getRegionBroker()).getDestinationInterceptor()).setInterceptors(destinationInterceptors);
                    LOG.trace("removed VirtualDestinationInterceptor from: " + interceptorsList);
                }
            });
        } else {
            LOG.info("No runtime support for removal of: " + o);
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

    private void addNew(Object o) {
        if (o instanceof NetworkConnector) {
            NetworkConnector networkConnector = (NetworkConnector) o;
            if (networkConnector.getUri() != null) {
                try {
                    org.apache.activemq.network.NetworkConnector nc =
                            getBrokerService().addNetworkConnector(networkConnector.getUri());
                    Properties properties = new Properties();
                    IntrospectionSupport.getProperties(networkConnector, properties, null);
                    properties.remove("uri");
                    LOG.trace("applying networkConnector props: " + properties);
                    IntrospectionSupport.setProperties(nc, properties);
                    nc.start();
                    LOG.info("started new network connector: " + nc);
                } catch (Exception e) {
                    LOG.error("Failed to add new networkConnector " + networkConnector, e);
                }
            }
        } else if (o instanceof VirtualDestinationInterceptor) {
            final VirtualDestinationInterceptor dto = (VirtualDestinationInterceptor) o;
            destinationInterceptorUpdateWork.add(new Runnable() {
                public void run() {

                    boolean updatedExistingInterceptor = false;

                    for (DestinationInterceptor destinationInterceptor : getBrokerService().getDestinationInterceptors()) {
                        if (destinationInterceptor instanceof org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor) {
                            // update existing interceptor
                            final org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor virtualDestinationInterceptor =
                                    (org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor) destinationInterceptor;

                            virtualDestinationInterceptor.setVirtualDestinations(fromDto(dto));
                            LOG.trace("applied updates to: " + virtualDestinationInterceptor);
                            updatedExistingInterceptor = true;
                        }
                    }

                    if (!updatedExistingInterceptor) {
                        // add
                        org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor virtualDestinationInterceptor =
                                new org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor();
                        virtualDestinationInterceptor.setVirtualDestinations(fromDto(dto));

                        List<DestinationInterceptor> interceptorsList = new ArrayList<DestinationInterceptor>();
                        interceptorsList.addAll(Arrays.asList(getBrokerService().getDestinationInterceptors()));
                        interceptorsList.add(virtualDestinationInterceptor);

                        DestinationInterceptor[] destinationInterceptors = interceptorsList.toArray(new DestinationInterceptor[]{});
                        getBrokerService().setDestinationInterceptors(destinationInterceptors);
                        ((CompositeDestinationInterceptor) ((RegionBroker) getBrokerService().getRegionBroker()).getDestinationInterceptor()).setInterceptors(destinationInterceptors);
                        LOG.trace("applied new: " + interceptorsList);
                    }
                }
            });
        } else {
            LOG.info("No runtime support for modifications to " + o);
        }
    }

    private VirtualDestination[] fromDto(VirtualDestinationInterceptor virtualDestinationInterceptor) {
        List<VirtualDestination> answer = new ArrayList<VirtualDestination>();
        for (Object vd : filter(virtualDestinationInterceptor, VirtualDestinationInterceptor.VirtualDestinations.class)) {
            for (Object vt : filter(vd, VirtualTopic.class)) {
                answer.add(fromDto(vt, new org.apache.activemq.broker.region.virtual.VirtualTopic()));
            }
            for (Object vt : filter(vd, CompositeTopic.class)) {
                answer.add(fromDto(vt, new org.apache.activemq.broker.region.virtual.CompositeTopic()));
            }
            for (Object vt : filter(vd, CompositeQueue.class)) {
                answer.add(fromDto(vt, new org.apache.activemq.broker.region.virtual.CompositeQueue()));
            }
        }
        VirtualDestination[] array = new VirtualDestination[answer.size()];
        answer.toArray(array);
        return array;
    }

    private VirtualDestination fromDto(Object dto, VirtualDestination instance) {
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
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
package org.apache.activemq.transport.discovery.http;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.Service;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.Suspendable;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPDiscoveryAgent implements DiscoveryAgent, Suspendable {

    static enum UpdateState {
        SUSPENDED,
        RESUMING,
        RESUMED
    }

    private static final Logger LOG = LoggerFactory.getLogger(HTTPDiscoveryAgent.class);

    private String registryURL = "http://localhost:8080/discovery-registry/default";
    private HttpClient httpClient = HttpClientBuilder.create().build();
    private AtomicBoolean running = new AtomicBoolean();
    private final AtomicReference<DiscoveryListener> discoveryListener = new AtomicReference<DiscoveryListener>();
    private final HashSet<String> registeredServices = new HashSet<String>();
    private final HashMap<String, SimpleDiscoveryEvent> discoveredServices = new HashMap<String, SimpleDiscoveryEvent>();
    private Thread thread;
    private long updateInterval = 1000 * 10;
    @SuppressWarnings("unused")
    private String brokerName;
    private boolean startEmbeddRegistry = false;
    private Service jetty;
    private AtomicInteger startCounter = new AtomicInteger(0);

    private long initialReconnectDelay = 1000;
    private long maxReconnectDelay = 1000 * 30;
    private long backOffMultiplier = 2;
    private boolean useExponentialBackOff = true;
    private int maxReconnectAttempts;
    private final Object sleepMutex = new Object();
    private final Object updateMutex = new Object();
    private UpdateState updateState = UpdateState.RESUMED;
    private long minConnectTime = 5000;

    class SimpleDiscoveryEvent extends DiscoveryEvent {

        private int connectFailures;
        private long reconnectDelay = initialReconnectDelay;
        private long connectTime = System.currentTimeMillis();
        private AtomicBoolean failed = new AtomicBoolean(false);
        private AtomicBoolean removed = new AtomicBoolean(false);

        public SimpleDiscoveryEvent(String service) {
            super(service);
        }
    }

    public String getGroup() {
        return null;
    }

    public void registerService(String service) throws IOException {
        synchronized (registeredServices) {
            registeredServices.add(service);
        }
        doRegister(service);
    }

    synchronized private void doRegister(String service) {
        String url = registryURL;
        try {
            HttpPut method = new HttpPut(url);
            method.addHeader("service", service);
            ResponseHandler<String> handler = new BasicResponseHandler();
            String responseBody = httpClient.execute(method, handler);
            LOG.debug("PUT to " + url + " got a " + responseBody);
        } catch (Exception e) {
            LOG.debug("PUT to " + url + " failed with: " + e);
        }
    }

    @SuppressWarnings("unused")
    synchronized private void doUnRegister(String service) {
        String url = registryURL;
        try {
            HttpDelete method = new HttpDelete(url);
            method.addHeader("service", service);
            ResponseHandler<String> handler = new BasicResponseHandler();
            String responseBody = httpClient.execute(method, handler);
            LOG.debug("DELETE to " + url + " got a " + responseBody);
        } catch (Exception e) {
            LOG.debug("DELETE to " + url + " failed with: " + e);
        }
    }

    synchronized private Set<String> doLookup(long freshness) {
        String url = registryURL + "?freshness=" + freshness;
        try {
            HttpGet method = new HttpGet(url);
            ResponseHandler<String> handler = new BasicResponseHandler();
            String response = httpClient.execute(method, handler);
            LOG.debug("GET to " + url + " got a " + response);
            Set<String> rc = new HashSet<String>();
            Scanner scanner = new Scanner(response);
            while (scanner.hasNextLine()) {
                String service = scanner.nextLine();
                if (service.trim().length() != 0) {
                    rc.add(service);
                }
            }
            scanner.close();
            return rc;
        } catch (Exception e) {
            LOG.debug("GET to " + url + " failed with: " + e);
            return null;
        }
    }

    public void serviceFailed(DiscoveryEvent devent) throws IOException {

        final SimpleDiscoveryEvent event = (SimpleDiscoveryEvent) devent;
        if (event.failed.compareAndSet(false, true)) {
            discoveryListener.get().onServiceRemove(event);
            if (!event.removed.get()) {
                // Setup a thread to re-raise the event...
                Thread thread = new Thread() {
                    public void run() {

                        // We detect a failed connection attempt because the
                        // service
                        // fails right away.
                        if (event.connectTime + minConnectTime > System.currentTimeMillis()) {
                            LOG.debug("Failure occured soon after the discovery event was generated.  " +
                                      "It will be clasified as a connection failure: " + event);

                            event.connectFailures++;

                            if (maxReconnectAttempts > 0 && event.connectFailures >= maxReconnectAttempts) {
                                LOG.debug("Reconnect attempts exceeded " + maxReconnectAttempts +
                                          " tries.  Reconnecting has been disabled.");
                                return;
                            }

                            synchronized (sleepMutex) {
                                try {
                                    if (!running.get() || event.removed.get()) {
                                        return;
                                    }
                                    LOG.debug("Waiting " + event.reconnectDelay +
                                              " ms before attepting to reconnect.");
                                    sleepMutex.wait(event.reconnectDelay);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }

                            if (!useExponentialBackOff) {
                                event.reconnectDelay = initialReconnectDelay;
                            } else {
                                // Exponential increment of reconnect delay.
                                event.reconnectDelay *= backOffMultiplier;
                                if (event.reconnectDelay > maxReconnectDelay) {
                                    event.reconnectDelay = maxReconnectDelay;
                                }
                            }

                        } else {
                            event.connectFailures = 0;
                            event.reconnectDelay = initialReconnectDelay;
                        }

                        if (!running.get() || event.removed.get()) {
                            return;
                        }

                        event.connectTime = System.currentTimeMillis();
                        event.failed.set(false);
                        discoveryListener.get().onServiceAdd(event);
                    }
                };
                thread.setDaemon(true);
                thread.start();
            }
        }
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public void setDiscoveryListener(DiscoveryListener discoveryListener) {
        this.discoveryListener.set(discoveryListener);
    }

    public void setGroup(String group) {
    }

    public void start() throws Exception {
        if (startCounter.addAndGet(1) == 1) {
            if (startEmbeddRegistry) {
                jetty = createEmbeddedJettyServer();
                Map<String, Object> props = new HashMap<String, Object>();
                props.put("agent", this);
                IntrospectionSupport.setProperties(jetty, props);
                jetty.start();
            }

            running.set(true);
            thread = new Thread("HTTPDiscovery Agent") {
                @Override
                public void run() {
                    while (running.get()) {
                        try {
                            update();
                            synchronized (updateMutex) {
                                do {
                                    if( updateState == UpdateState.RESUMING ) {
                                        updateState = UpdateState.RESUMED;
                                    } else {
                                        updateMutex.wait(updateInterval);
                                    }
                                } while( updateState==UpdateState.SUSPENDED && running.get());
                            }
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }
            };
            thread.setDaemon(true);
            thread.start();
        }
    }

    /**
     * Create the EmbeddedJettyServer instance via reflection so that we can
     * avoid a hard runtime dependency on jetty.
     *
     * @return
     * @throws Exception
     */
    private Service createEmbeddedJettyServer() throws Exception {
        Class<?> clazz = HTTPDiscoveryAgent.class.getClassLoader().loadClass("org.apache.activemq.transport.discovery.http.EmbeddedJettyServer");
        return (Service) clazz.newInstance();
    }

    private void update() {
        // Register all our services...
        synchronized (registeredServices) {
            for (String service : registeredServices) {
                doRegister(service);
            }
        }

        // Find new registered services...
        DiscoveryListener discoveryListener = this.discoveryListener.get();
        if (discoveryListener != null) {
            Set<String> activeServices = doLookup(updateInterval * 3);
            // If there is error talking the the central server, then
            // activeServices == null
            if (activeServices != null) {
                synchronized (discoveredServices) {

                    HashSet<String> removedServices = new HashSet<String>(discoveredServices.keySet());
                    removedServices.removeAll(activeServices);

                    HashSet<String> addedServices = new HashSet<String>(activeServices);
                    addedServices.removeAll(discoveredServices.keySet());
                    addedServices.removeAll(removedServices);

                    for (String service : addedServices) {
                        SimpleDiscoveryEvent e = new SimpleDiscoveryEvent(service);
                        discoveredServices.put(service, e);
                        discoveryListener.onServiceAdd(e);
                    }

                    for (String service : removedServices) {
                        SimpleDiscoveryEvent e = discoveredServices.remove(service);
                        if (e != null) {
                            e.removed.set(true);
                        }
                        discoveryListener.onServiceRemove(e);
                    }
                }
            }
        }
    }

    public void stop() throws Exception {
        if (startCounter.decrementAndGet() == 0) {
            resume();
            running.set(false);
            if (thread != null) {
                thread.join(updateInterval * 3);
                thread = null;
            }
            if (jetty != null) {
                jetty.stop();
                jetty = null;
            }
        }
    }

    public String getRegistryURL() {
        return registryURL;
    }

    public void setRegistryURL(String discoveryRegistryURL) {
        this.registryURL = discoveryRegistryURL;
    }

    public long getUpdateInterval() {
        return updateInterval;
    }

    public void setUpdateInterval(long updateInterval) {
        this.updateInterval = updateInterval;
    }

    public boolean isStartEmbeddRegistry() {
        return startEmbeddRegistry;
    }

    public void setStartEmbeddRegistry(boolean startEmbeddRegistry) {
        this.startEmbeddRegistry = startEmbeddRegistry;
    }


    @Override
    public void suspend() throws Exception {
        synchronized (updateMutex) {
            updateState = UpdateState.SUSPENDED;
        }
    }

    @Override
    public void resume() throws Exception {
        synchronized (updateMutex) {
            updateState = UpdateState.RESUMING;
            updateMutex.notify();
        }
    }

}

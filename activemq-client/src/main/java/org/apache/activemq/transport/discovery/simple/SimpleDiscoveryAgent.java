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
package org.apache.activemq.transport.discovery.simple;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple DiscoveryAgent that allows static configuration of the discovered
 * services.
 *
 *
 */
public class SimpleDiscoveryAgent implements DiscoveryAgent {

    private final static Logger LOG = LoggerFactory.getLogger(SimpleDiscoveryAgent.class);
    private long initialReconnectDelay = 1000;
    private long maxReconnectDelay = 1000 * 30;
    private long backOffMultiplier = 2;
    private boolean useExponentialBackOff=true;
    private int maxReconnectAttempts;
    private final Object sleepMutex = new Object();
    private long minConnectTime = 5000;
    private DiscoveryListener listener;
    private String services[] = new String[] {};
    private final AtomicBoolean running = new AtomicBoolean(false);
    private TaskRunnerFactory taskRunner;

    class SimpleDiscoveryEvent extends DiscoveryEvent {

        private int connectFailures;
        private long reconnectDelay = -1;
        private long connectTime = System.currentTimeMillis();
        private final AtomicBoolean failed = new AtomicBoolean(false);

        public SimpleDiscoveryEvent(String service) {
            super(service);
        }

        public SimpleDiscoveryEvent(SimpleDiscoveryEvent copy) {
            super(copy);
            connectFailures = copy.connectFailures;
            reconnectDelay = copy.reconnectDelay;
            connectTime = copy.connectTime;
            failed.set(copy.failed.get());
        }

        @Override
        public String toString() {
            return "[" + serviceName + ", failed:" + failed + ", connectionFailures:" + connectFailures + "]";
        }
    }

    @Override
    public void setDiscoveryListener(DiscoveryListener listener) {
        this.listener = listener;
    }

    @Override
    public void registerService(String name) throws IOException {
    }

    @Override
    public void start() throws Exception {
        taskRunner = new TaskRunnerFactory();
        taskRunner.init();

        running.set(true);
        for (int i = 0; i < services.length; i++) {
            listener.onServiceAdd(new SimpleDiscoveryEvent(services[i]));
        }
    }

    @Override
    public void stop() throws Exception {
        running.set(false);

        if (taskRunner != null) {
            taskRunner.shutdown();
        }

        // TODO: Should we not remove the services on the listener?

        synchronized (sleepMutex) {
            sleepMutex.notifyAll();
        }
    }

    public String[] getServices() {
        return services;
    }

    public void setServices(String services) {
        this.services = services.split(",");
    }

    public void setServices(String services[]) {
        this.services = services;
    }

    public void setServices(URI services[]) {
        this.services = new String[services.length];
        for (int i = 0; i < services.length; i++) {
            this.services[i] = services[i].toString();
        }
    }

    @Override
    public void serviceFailed(DiscoveryEvent devent) throws IOException {

        final SimpleDiscoveryEvent sevent = (SimpleDiscoveryEvent)devent;
        if (running.get() && sevent.failed.compareAndSet(false, true)) {

            listener.onServiceRemove(sevent);
            taskRunner.execute(new Runnable() {
                @Override
                public void run() {
                    SimpleDiscoveryEvent event = new SimpleDiscoveryEvent(sevent);

                    // We detect a failed connection attempt because the service
                    // fails right away.
                    if (event.connectTime + minConnectTime > System.currentTimeMillis()) {
                        LOG.debug("Failure occurred soon after the discovery event was generated.  It will be classified as a connection failure: {}", event);

                        event.connectFailures++;

                        if (maxReconnectAttempts > 0 && event.connectFailures >= maxReconnectAttempts) {
                            LOG.warn("Reconnect attempts exceeded {} tries.  Reconnecting has been disabled for: {}", maxReconnectAttempts, event);
                            return;
                        }

                        if (!useExponentialBackOff || event.reconnectDelay == -1) {
                            event.reconnectDelay = initialReconnectDelay;
                        } else {
                            // Exponential increment of reconnect delay.
                            event.reconnectDelay *= backOffMultiplier;
                            if (event.reconnectDelay > maxReconnectDelay) {
                                event.reconnectDelay = maxReconnectDelay;
                            }
                        }

                        doReconnectDelay(event);

                    } else {
                        LOG.trace("Failure occurred to long after the discovery event was generated.  " +
                                  "It will not be classified as a connection failure: {}", event);
                        event.connectFailures = 0;
                        event.reconnectDelay = initialReconnectDelay;

                        doReconnectDelay(event);
                    }

                    if (!running.get()) {
                        LOG.debug("Reconnecting disabled: stopped");
                        return;
                    }

                    event.connectTime = System.currentTimeMillis();
                    event.failed.set(false);
                    listener.onServiceAdd(event);
                }
            }, "Simple Discovery Agent");
        }
    }

    protected void doReconnectDelay(SimpleDiscoveryEvent event) {
        synchronized (sleepMutex) {
            try {
                if (!running.get()) {
                    LOG.debug("Reconnecting disabled: stopped");
                    return;
                }

                LOG.debug("Waiting {}ms before attempting to reconnect.", event.reconnectDelay);
                sleepMutex.wait(event.reconnectDelay);
            } catch (InterruptedException ie) {
                LOG.debug("Reconnecting disabled: ", ie);
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public long getBackOffMultiplier() {
        return backOffMultiplier;
    }

    public void setBackOffMultiplier(long backOffMultiplier) {
        this.backOffMultiplier = backOffMultiplier;
    }

    public long getInitialReconnectDelay() {
        return initialReconnectDelay;
    }

    public void setInitialReconnectDelay(long initialReconnectDelay) {
        this.initialReconnectDelay = initialReconnectDelay;
    }

    public int getMaxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    public long getMaxReconnectDelay() {
        return maxReconnectDelay;
    }

    public void setMaxReconnectDelay(long maxReconnectDelay) {
        this.maxReconnectDelay = maxReconnectDelay;
    }

    public long getMinConnectTime() {
        return minConnectTime;
    }

    public void setMinConnectTime(long minConnectTime) {
        this.minConnectTime = minConnectTime;
    }

    public boolean isUseExponentialBackOff() {
        return useExponentialBackOff;
    }

    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }
}

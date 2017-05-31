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
package org.apache.activemq.transport.discovery;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.CompositeTransport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.Suspendable;
import org.apache.activemq.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransportFilter} which uses a {@link DiscoveryAgent} to
 * discover remote broker instances and dynamically connect to them.
 */
public class DiscoveryTransport extends TransportFilter implements DiscoveryListener {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryTransport.class);

    private final CompositeTransport next;
    private DiscoveryAgent discoveryAgent;
    private final ConcurrentMap<String, URI> serviceURIs = new ConcurrentHashMap<String, URI>();

    private Map<String, String> parameters;

    public DiscoveryTransport(CompositeTransport next) {
        super(next);
        this.next = next;
    }

    @Override
    public void start() throws Exception {
        if (discoveryAgent == null) {
            throw new IllegalStateException("discoveryAgent not configured");
        }

        // lets pass into the agent the broker name and connection details
        discoveryAgent.setDiscoveryListener(this);
        discoveryAgent.start();
        next.start();
    }

    @Override
    public void stop() throws Exception {
        ServiceStopper ss = new ServiceStopper();
        ss.stop(discoveryAgent);
        ss.stop(next);
        ss.throwFirstException();
    }

    @Override
    public void onServiceAdd(DiscoveryEvent event) {
        String url = event.getServiceName();
        if (url != null) {
            try {
                URI uri = new URI(url);
                LOG.info("Adding new broker connection URL: " + uri);
                uri = URISupport.applyParameters(uri, parameters, DISCOVERED_OPTION_PREFIX);
                serviceURIs.put(event.getServiceName(), uri);
                next.add(false,new URI[] {uri});
            } catch (URISyntaxException e) {
                LOG.warn("Could not connect to remote URI: " + url + " due to bad URI syntax: " + e, e);
            }
        }
    }

    @Override
    public void onServiceRemove(DiscoveryEvent event) {
        URI uri = serviceURIs.get(event.getServiceName());
        if (uri != null) {
            next.remove(false,new URI[] {uri});
        }
    }

    public DiscoveryAgent getDiscoveryAgent() {
        return discoveryAgent;
    }

    public void setDiscoveryAgent(DiscoveryAgent discoveryAgent) {
        this.discoveryAgent = discoveryAgent;
    }

    public void setParameters(Map<String, String> parameters) {
       this.parameters = parameters;
    }

    @Override
    public void transportResumed() {
        if( discoveryAgent instanceof Suspendable ) {
            try {
                ((Suspendable)discoveryAgent).suspend();
            } catch (Exception e) {
                LOG.warn("Exception suspending discoverAgent: ", discoveryAgent);
            }
        }
        super.transportResumed();
    }

    @Override
    public void transportInterupted() {
        if( discoveryAgent instanceof Suspendable ) {
            try {
                ((Suspendable)discoveryAgent).resume();
            } catch (Exception e) {
                LOG.warn("Exception resuming discoverAgent: ", discoveryAgent);
            }
        }
        super.transportInterupted();
    }
}

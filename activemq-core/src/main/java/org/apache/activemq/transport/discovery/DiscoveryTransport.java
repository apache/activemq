/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.discovery;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.CompositeTransport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link ReliableTransportChannel} which uses a {@link DiscoveryAgent} to
 * discover remote broker instances and dynamically connect to them.
 * 
 * @version $Revision$
 */
public class DiscoveryTransport extends TransportFilter implements DiscoveryListener {

    private static final Log log = LogFactory.getLog(DiscoveryTransport.class);

    private final CompositeTransport next;
    private DiscoveryAgent discoveryAgent;
    private final ConcurrentHashMap serviceURIs = new ConcurrentHashMap();

    public DiscoveryTransport(CompositeTransport next) {
        super(next);
        this.next = next;
    }

    public void start() throws Exception {
        if (discoveryAgent == null) {
            throw new IllegalStateException("discoveryAgent not configured");
        }

        // lets pass into the agent the broker name and connection details
        discoveryAgent.setDiscoveryListener(this);
        discoveryAgent.start();
        next.start();
    }

    public void stop() throws Exception {
        IOException firstError = null;
        try {
            discoveryAgent.stop();
        } catch (IOException e) {
            firstError = e;
        }
        try {
            next.stop();
        } catch (IOException e) {
            if (firstError != null)
                firstError = e;
        }
        if (firstError != null) {
            throw firstError;
        }
    }

    public void onServiceAdd(DiscoveryEvent event) {
        String url = event.getServiceName();
        if (url != null) {
            try {
                URI uri = new URI(url);
                serviceURIs.put(event.getServiceName(), uri);
                log.info("Adding new broker connection URL: " + uri );
                next.add(new URI[]{uri});
            } catch (URISyntaxException e) {
                log.warn("Could not connect to remote URI: " + url + " due to bad URI syntax: " + e, e);
            }
        }
    }

    public void onServiceRemove(DiscoveryEvent event) {
        URI uri = (URI) serviceURIs.get(event.getServiceName());
        if (uri != null) {
            next.remove(new URI[]{uri});
        }
    }

    public DiscoveryAgent getDiscoveryAgent() {
        return discoveryAgent;
    }

    public void setDiscoveryAgent(DiscoveryAgent discoveryAgent) {
        this.discoveryAgent = discoveryAgent;
    }

}

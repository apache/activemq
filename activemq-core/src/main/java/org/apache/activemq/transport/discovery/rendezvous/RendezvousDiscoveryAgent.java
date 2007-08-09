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
package org.apache.activemq.transport.discovery.rendezvous;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceEvent;
import javax.jmdns.ServiceInfo;
import javax.jmdns.ServiceListener;

import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.MapHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link DiscoveryAgent} using <a href="http://www.zeroconf.org/">Zeroconf</a>
 * via the <a href="http://jmdns.sf.net/">jmDNS</a> library
 * 
 * @version $Revision$
 */
public class RendezvousDiscoveryAgent implements DiscoveryAgent, ServiceListener {
    private static final Log LOG = LogFactory.getLog(RendezvousDiscoveryAgent.class);

    private static final String TYPE_SUFFIX = "ActiveMQ-4.";

    private JmDNS jmdns;
    private InetAddress localAddress;
    private String localhost;
    private int weight = 0;
    private int priority = 0;

    private DiscoveryListener listener;
    private String group = "default";
    private final CopyOnWriteArrayList serviceInfos = new CopyOnWriteArrayList();

    // DiscoveryAgent interface
    // -------------------------------------------------------------------------
    public void start() throws Exception {
        if (group == null) {
            throw new IOException("You must specify a group to discover");
        }
        String type = getType();
        if (!type.endsWith(".")) {
            LOG.warn("The type '" + type + "' should end with '.' to be a valid Rendezvous type");
            type += ".";
        }
        try {
            // force lazy construction
            getJmdns();
            if (listener != null) {
                LOG.info("Discovering service of type: " + type);
                jmdns.addServiceListener(type, this);
            }
        } catch (IOException e) {
            JMSExceptionSupport.create("Failed to start JmDNS service: " + e, e);
        }
    }

    public void stop() {
        if (jmdns != null) {
            for (Iterator iter = serviceInfos.iterator(); iter.hasNext();) {
                ServiceInfo si = (ServiceInfo)iter.next();
                jmdns.unregisterService(si);
            }

            // Close it down async since this could block for a while.
            final JmDNS closeTarget = jmdns;
            Thread thread = new Thread() {
                public void run() {
                    closeTarget.close();
                }
            };

            thread.setDaemon(true);
            thread.start();

            jmdns = null;
        }
    }

    public void registerService(String name) throws IOException {
        ServiceInfo si = createServiceInfo(name, new HashMap());
        serviceInfos.add(si);
        getJmdns().registerService(si);
    }

    // ServiceListener interface
    // -------------------------------------------------------------------------
    public void addService(JmDNS jmDNS, String type, String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addService with type: " + type + " name: " + name);
        }
        if (listener != null) {
            listener.onServiceAdd(new DiscoveryEvent(name));
        }
        jmDNS.requestServiceInfo(type, name);
    }

    public void removeService(JmDNS jmDNS, String type, String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeService with type: " + type + " name: " + name);
        }
        if (listener != null) {
            listener.onServiceRemove(new DiscoveryEvent(name));
        }
    }

    public void serviceAdded(ServiceEvent event) {
        addService(event.getDNS(), event.getType(), event.getName());
    }

    public void serviceRemoved(ServiceEvent event) {
        removeService(event.getDNS(), event.getType(), event.getName());
    }

    public void serviceResolved(ServiceEvent event) {
    }

    public void resolveService(JmDNS jmDNS, String type, String name, ServiceInfo serviceInfo) {
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public JmDNS getJmdns() throws IOException {
        if (jmdns == null) {
            jmdns = createJmDNS();
        }
        return jmdns;
    }

    public void setJmdns(JmDNS jmdns) {
        this.jmdns = jmdns;
    }

    public InetAddress getLocalAddress() throws UnknownHostException {
        if (localAddress == null) {
            localAddress = createLocalAddress();
        }
        return localAddress;
    }

    public void setLocalAddress(InetAddress localAddress) {
        this.localAddress = localAddress;
    }

    public String getLocalhost() {
        return localhost;
    }

    public void setLocalhost(String localhost) {
        this.localhost = localhost;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected ServiceInfo createServiceInfo(String name, Map map) {
        int port = MapHelper.getInt(map, "port", 0);

        String type = getType();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Registering service type: " + type + " name: " + name + " details: " + map);
        }
        return new ServiceInfo(type, name + "." + type, port, weight, priority, "");
    }

    protected JmDNS createJmDNS() throws IOException {
        return JmDNSFactory.create(getLocalAddress());
    }

    protected InetAddress createLocalAddress() throws UnknownHostException {
        if (localhost != null) {
            return InetAddress.getByName(localhost);
        }
        return InetAddress.getLocalHost();
    }

    public void setDiscoveryListener(DiscoveryListener listener) {
        this.listener = listener;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getType() {
        return "_" + group + "." + TYPE_SUFFIX;
    }

    public void serviceFailed(DiscoveryEvent event) throws IOException {
        // TODO: is there a way to notify the JmDNS that the service failed?
    }

    /**
     * @param brokerName
     * @see org.apache.activemq.transport.discovery.DiscoveryAgent#setBrokerName(java.lang.String)
     */
    public void setBrokerName(String brokerName) {
        // implementation of interface

    }
}

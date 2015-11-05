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
package org.apache.activemq.plugin.java;

import java.util.Arrays;
import java.util.Set;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.plugin.AbstractRuntimeConfigurationBroker;
import org.apache.activemq.plugin.UpdateVirtualDestinationsTask;
import org.apache.activemq.security.AuthorizationBroker;
import org.apache.activemq.security.AuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationBroker;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaRuntimeConfigurationBroker extends AbstractRuntimeConfigurationBroker {

    /**
     * @param next
     */
    public JavaRuntimeConfigurationBroker(Broker next) {
        super(next);
    }

    public static final Logger LOG = LoggerFactory.getLogger(JavaRuntimeConfigurationBroker.class);


    //Virtual Destinations
    public void setVirtualDestinations(final VirtualDestination[] virtualDestinations) {
        this.addDestinationWork.add(new UpdateVirtualDestinationsTask(this) {
            @Override
            protected VirtualDestination[] getVirtualDestinations() {
                return virtualDestinations;
            }
        });
    }

    /**
     * Set the virtual destinations and apply immediately, instead of waiting for a new
     * destination or connection to trigger the work.
     *
     * @param virtualDestinations
     * @param applyImmediately
     * @throws Exception
     */
    public void setVirtualDestinations(final VirtualDestination[] virtualDestinations, boolean applyImmediately) throws Exception {
        setVirtualDestinations(virtualDestinations);
        if (applyImmediately) {
            this.applyDestinationWork();
        }
    }

    //New Destinations
    public void setDestinations(final ActiveMQDestination[] destinations) {
        for (ActiveMQDestination destination : destinations) {
            try {
                if (!containsDestination(destination)) {
                    this.addDestination(this.getBrokerService().getAdminConnectionContext(), destination, true);
                    this.info("Added destination " + destination);
                }
            } catch (Exception e) {
                this.info("Failed to add a new destination for: " + destination, e);
            }
        }
    }

    protected boolean containsDestination(ActiveMQDestination destination) throws Exception {
        return Arrays.asList(this.getBrokerService().getRegionBroker().getDestinations()).contains(destination);
    }

    public void addNewDestination(ActiveMQDestination destination) {
        try {
            this.addDestination(this.getBrokerService().getAdminConnectionContext(), destination, true);
            this.info("Added destination " + destination);
        } catch (Exception e) {
            this.info("Failed to add a new destination for: " + destination, e);
        }
    }

    //Network Connectors
    public void addNetworkConnector(final DiscoveryNetworkConnector nc) {
        try {
            if (!getBrokerService().getNetworkConnectors().contains(nc)) {
                getBrokerService().addNetworkConnector(nc);
                nc.start();
                info("started new network connector: " + nc);
            } else {
                info("skipping network connector add, already exists: " + nc);
            }
        } catch (Exception e) {
            info("Failed to add new networkConnector " + nc, e);
        }
    }

    public void updateNetworkConnector(final DiscoveryNetworkConnector nc) {
        removeNetworkConnector(nc);
        addNetworkConnector(nc);
    }

    public void removeNetworkConnector(final DiscoveryNetworkConnector existingCandidate) {
        if (getBrokerService().removeNetworkConnector(existingCandidate)) {
            try {
                existingCandidate.stop();
                info("stopped and removed networkConnector: " + existingCandidate);
            } catch (Exception e) {
                info("Failed to stop removed network connector: " + existingCandidate);
            }
        }
    }

    //Policy entries
    public void addNewPolicyEntry(PolicyEntry addition) {
        PolicyMap existingMap = getBrokerService().getDestinationPolicy();
        existingMap.put(addition.getDestination(), addition);
        applyRetrospectively(addition);
        info("added policy for: " + addition.getDestination());
    }

    public void modifyPolicyEntry(PolicyEntry existing) {
        PolicyMap existingMap = this.getBrokerService().getDestinationPolicy();

        Set<?> existingEntry = existingMap.get(existing.getDestination());
        if (existingEntry.size() == 1) {
            applyRetrospectively(existing);
            this.info("updated policy for: " + existing.getDestination());
        } else {
            this.info("cannot modify policy matching multiple destinations: " + existingEntry + ", destination:" + existing.getDestination());
        }
    }

    protected void applyRetrospectively(PolicyEntry updatedEntry) {
        RegionBroker regionBroker = (RegionBroker) this.getBrokerService().getRegionBroker();
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
            this.debug("applied update to:" + target);
        }
    }

    //authentication plugin
    public void updateSimpleAuthenticationPlugin(final SimpleAuthenticationPlugin updatedPlugin) {
        try {
            final SimpleAuthenticationBroker authenticationBroker =
                (SimpleAuthenticationBroker) getBrokerService().getBroker().getAdaptor(SimpleAuthenticationBroker.class);
            addConnectionWork.add(new Runnable() {
                @Override
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
    }

    //authorization map
    public void updateAuthorizationMap(final AuthorizationMap authorizationMap) {
        try {
            // replace authorization map - need exclusive write lock to total broker
            AuthorizationBroker authorizationBroker =
                    (AuthorizationBroker) getBrokerService().getBroker().getAdaptor(AuthorizationBroker.class);

            authorizationBroker.setAuthorizationMap(authorizationMap);
        } catch (Exception e) {
            info("failed to apply modified AuthorizationMap to AuthorizationBroker", e);
        }
    }
}
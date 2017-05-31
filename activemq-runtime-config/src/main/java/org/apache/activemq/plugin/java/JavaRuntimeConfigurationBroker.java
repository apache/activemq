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
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.plugin.AbstractRuntimeConfigurationBroker;
import org.apache.activemq.plugin.UpdateVirtualDestinationsTask;
import org.apache.activemq.plugin.util.PolicyEntryUtil;
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
                getBrokerService().startNetworkConnector(nc, null);
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
        PolicyEntryUtil.applyRetrospectively(this, addition, null);
        info("added policy for: " + addition.getDestination());
    }


    /**
     * This method will modify an existing policy entry that matches the destination
     * set on the PolicyEntry passed in.
     *
     * The PolicyEntry reference must already be in the PolicyMap or it won't be updated.
     * To modify the entry the best way is to look up the existing PolicyEntry from the
     * PolicyMap, make changes to it, and pass it to this method to apply.
     *
     * To create or replace an existing entry (if the destination matches), see
     * {@link #modifyPolicyEntry(PolicyEntry, boolean)
     *
     *
     * @param existing
     */
    public void modifyPolicyEntry(PolicyEntry existing) {
        modifyPolicyEntry(existing, false);
    }

    public void modifyPolicyEntry(PolicyEntry existing, boolean createOrReplace) {
        modifyPolicyEntry(existing, createOrReplace, null);
    }

    /**
     * This method will modify an existing policy entry that matches the destination
     * set on the PolicyEntry passed in.  If createOrReplace is true, a new policy
     * will be created if it doesn't exist and a policy will be replaced in the PolicyMap,
     * versus modified, if it is a different reference but the destinations for the Policy match.
     *
     * If createOrReplace is false, the policy update will only be applied if
     * the PolicyEntry reference already exists in the PolicyMap.
     *
     * includedProperties is a list of properties that will be applied retrospectively. If
     * the list is null, then all properties on the policy will be reapplied to the destination.
     * This allows the ability to limit which properties are applied to existing destinations.
     *
     * @param existing
     * @param createIfAbsent
     * @param includedProperties - optional list of properties to apply retrospectively
     */
    public void modifyPolicyEntry(PolicyEntry existing, boolean createOrReplace,
            Set<String> includedProperties) {
        PolicyMap existingMap = this.getBrokerService().getDestinationPolicy();

        //First just look up by the destination type to see if anything matches
        PolicyEntry existingEntry = PolicyEntryUtil.findEntryByDestination(this, existing);

        //handle createOrReplace
        if (createOrReplace) {
            //if not found at all, go ahead and insert the policy entry
            if (existingEntry == null) {
                existingMap.put(existing.getDestination(), existing);
                existingEntry = existing;
            //If found but the objects are different, remove the old policy entry
            //and replace it with the new one
            } else if (!existing.equals(existingEntry)) {
                synchronized(existingMap) {
                    existingMap.remove(existingEntry.getDestination(), existingEntry);
                    existingMap.put(existing.getDestination(), existing);
                }
                existingEntry = existing;
            }
        }

        //Make sure that at this point the passed in object and the entry in
        //the map are the same
        if (existingEntry != null && existingEntry.equals(existing)) {
            PolicyEntryUtil.applyRetrospectively(this, existingEntry, includedProperties);
            this.info("updated policy for: " + existingEntry.getDestination());
        } else {
            throw new IllegalArgumentException("The policy can not be updated because it either does not exist or the PolicyEntry"
                    + " reference does not match an existing PolicyEntry in the PolicyMap.  To replace an"
                    + " entry (versus modifying) or add, set createOrReplace to true. "
                    + existing + ", destination:" + existing.getDestination());
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
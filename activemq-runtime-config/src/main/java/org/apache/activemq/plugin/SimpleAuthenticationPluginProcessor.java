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

import org.apache.activemq.security.SimpleAuthenticationBroker;
import org.apache.activemq.security.SimpleAuthenticationPlugin;

public class SimpleAuthenticationPluginProcessor extends DefaultConfigurationProcessor {

    public SimpleAuthenticationPluginProcessor(RuntimeConfigurationBroker plugin, Class configurationClass) {
        super(plugin, configurationClass);
    }

    @Override
    public void modify(Object existing, Object candidate) {
        try {
            final SimpleAuthenticationPlugin updatedPlugin = fromDto(candidate, new SimpleAuthenticationPlugin());
            final SimpleAuthenticationBroker authenticationBroker =
                (SimpleAuthenticationBroker) plugin.getBrokerService().getBroker().getAdaptor(SimpleAuthenticationBroker.class);
            plugin.addConnectionWork.add(new Runnable() {
                public void run() {
                    authenticationBroker.setUserGroups(updatedPlugin.getUserGroups());
                    authenticationBroker.setUserPasswords(updatedPlugin.getUserPasswords());
                    authenticationBroker.setAnonymousAccessAllowed(updatedPlugin.isAnonymousAccessAllowed());
                    authenticationBroker.setAnonymousUser(updatedPlugin.getAnonymousUser());
                    authenticationBroker.setAnonymousGroup(updatedPlugin.getAnonymousGroup());
                }
            });
        } catch (Exception e) {
            plugin.info("failed to apply SimpleAuthenticationPlugin modifications to SimpleAuthenticationBroker", e);
        }
    }
}

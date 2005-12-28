/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.apache.activemq.transport.discovery.simple;

import java.io.IOException;
import java.net.URI;

import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryListener;

/**
 * A simple DiscoveryAgent that allows static configuration of the discovered services.
 * 
 * @version $Revision$
 */
public class SimpleDiscoveryAgent implements DiscoveryAgent {
    
    private DiscoveryListener listener;
    String services[] = new String[] {};
    String group = "DEFAULT";
    
    public void setDiscoveryListener(DiscoveryListener listener) {
        this.listener = listener;
    }
    
    public void registerService(String name) throws IOException {
    }
    
    public void start() throws Exception {
        for (int i = 0; i < services.length; i++) {
            listener.onServiceAdd(new DiscoveryEvent(services[i]));
        }
    }
    
    public void stop() throws Exception {
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

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public void setBrokerName(String brokerName) {
    }

    public void serviceFailed(DiscoveryEvent event) throws IOException {
    }
    
}

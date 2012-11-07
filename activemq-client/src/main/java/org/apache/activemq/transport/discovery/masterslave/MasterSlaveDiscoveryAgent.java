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
package org.apache.activemq.transport.discovery.masterslave;

import java.net.URI;
import org.apache.activemq.transport.discovery.simple.SimpleDiscoveryAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A static DiscoveryAgent that supports connecting to a Master / Slave tuple
 * of brokers.
 */
public class MasterSlaveDiscoveryAgent extends SimpleDiscoveryAgent {

    private final static Logger LOG = LoggerFactory.getLogger(MasterSlaveDiscoveryAgent.class);

    private String[] msServices = new String[]{};

    @Override
    public String[] getServices() {
        return msServices;
    }

    @Override
    public void setServices(String services) {
        this.msServices = services.split(",");
        configureServices();
    }

    @Override
    public void setServices(String services[]) {
        this.msServices = services;
        configureServices();
    }

    @Override
    public void setServices(URI services[]) {
        this.msServices = new String[services.length];
        for (int i = 0; i < services.length; i++) {
            this.msServices[i] = services[i].toString();
        }
        configureServices();
    }

    protected void configureServices() {
        if ((msServices == null) || (msServices.length < 2)) {
            LOG.error("masterSlave requires at least 2 URIs");
            msServices = new String[]{};
            throw new IllegalArgumentException("Expecting at least 2 arguments");
        }

        StringBuffer buf = new StringBuffer();

        buf.append("failover:(");

        for (int i = 0; i < (msServices.length - 1); i++) {
            buf.append(msServices[i]);
            buf.append(',');
        }
        buf.append(msServices[msServices.length - 1]);

        buf.append(")?randomize=false&maxReconnectAttempts=0");

        super.setServices(new String[]{buf.toString()});
    }

}

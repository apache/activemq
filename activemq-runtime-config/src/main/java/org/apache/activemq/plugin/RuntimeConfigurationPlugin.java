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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker plugin that will monitor the broker xml configuration for
 * changes and selectively apply those changes to the running broker.
 *
 * @org.apache.xbean.XBean
 */
public class RuntimeConfigurationPlugin implements BrokerPlugin {
    public static final Logger LOG = LoggerFactory.getLogger(RuntimeConfigurationPlugin.class);

    private long checkPeriod;

    @Override
    public Broker installPlugin(Broker broker) throws Exception {
        LOG.info("installing runtimeConfiguration plugin");
        RuntimeConfigurationBroker runtimeConfigurationBroker = new RuntimeConfigurationBroker(broker);
        runtimeConfigurationBroker.setCheckPeriod(getCheckPeriod());

        return runtimeConfigurationBroker;
    }

    public long getCheckPeriod() {
        return checkPeriod;
    }

    public void setCheckPeriod(long checkPeriod) {
        this.checkPeriod = checkPeriod;
    }
}

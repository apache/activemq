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
package org.apache.activemq.plugin.jmx;

import java.util.Date;

import org.apache.activemq.plugin.RuntimeConfigurationBroker;
import org.springframework.core.io.Resource;

public class RuntimeConfigurationView implements RuntimeConfigurationViewMBean {
    private final RuntimeConfigurationBroker runtimeConfigurationBroker;

    public RuntimeConfigurationView(RuntimeConfigurationBroker runtimeConfigurationBroker) {
        this.runtimeConfigurationBroker = runtimeConfigurationBroker;
    }

    @Override
    public String getUrl() {
        Resource value = runtimeConfigurationBroker.getConfigToMonitor();
        return value != null ? value.toString() : "null" ;
    }

    @Override
    public String getModified() {
        long lastModified =  runtimeConfigurationBroker.getLastModified();
        if (lastModified > 0) {
            return new Date(lastModified).toString();
        }
        return "unknown";
    }

    @Override
    public String getCheckPeriod() {
        return String.valueOf(runtimeConfigurationBroker.getCheckPeriod());
    }

    @Override
    public String updateNow() {
        return runtimeConfigurationBroker.updateNow();
    }
}

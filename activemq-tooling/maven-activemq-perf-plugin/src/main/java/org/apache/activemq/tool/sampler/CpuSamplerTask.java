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
package org.apache.activemq.tool.sampler;

import java.io.IOException;

import org.apache.activemq.tool.reports.AbstractPerfReportWriter;
import org.apache.activemq.tool.sampler.plugins.CpuSamplerPlugin;
import org.apache.activemq.tool.sampler.plugins.LinuxCpuSamplerPlugin;

public class CpuSamplerTask extends AbstractPerformanceSampler {

    private CpuSamplerPlugin plugin;

    public void createPlugin() throws IOException {
        createPlugin(System.getProperty("os.name"));
    }

    public void createPlugin(String osName) throws IOException {
        if (osName == null) {
            throw new IOException("No defined OS name found. Found: " + osName);
        }

        if (osName.equalsIgnoreCase(CpuSamplerPlugin.LINUX)) {
            plugin = new LinuxCpuSamplerPlugin(getInterval());
        } else {
            throw new IOException("No CPU Sampler Plugin found for OS: " + osName + ". CPU Sampler will not be started.");
        }
    }

    public void sampleData() {
        if (plugin != null && perfReportWriter != null) {
            perfReportWriter.writeCsvData(AbstractPerfReportWriter.REPORT_PLUGIN_CPU, "index=" + sampleIndex + "," + plugin.getCpuUtilizationStats());
        }
    }

    protected void onRampUpStart() {
        super.onRampUpStart();
        if (plugin != null) {
            plugin.start();
        }
    }

    protected void onRampDownEnd() {
        super.onRampDownEnd();
        if (plugin != null) {
            plugin.stop();
        }
    }
}

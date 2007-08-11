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
package org.apache.activemq.tool.reports;

import org.apache.activemq.tool.reports.plugins.ReportPlugin;
import org.apache.activemq.tool.reports.plugins.ThroughputReportPlugin;
import org.apache.activemq.tool.reports.plugins.CpuReportPlugin;

import java.util.Map;

public abstract class AbstractPerfReportWriter implements PerformanceReportWriter {

    public static final int REPORT_PLUGIN_THROUGHPUT = 0;
    public static final int REPORT_PLUGIN_CPU        = 1;
    
    protected ReportPlugin[] plugins = new ReportPlugin[] {
                                                new ThroughputReportPlugin(),
                                                new CpuReportPlugin()
                                       };

    protected void handleCsvData(int pluginType, String csvData) {
        plugins[pluginType].handleCsvData(csvData);
    }

    protected Map getSummary(int pluginType) {
        return plugins[pluginType].getSummary();
    }
}
